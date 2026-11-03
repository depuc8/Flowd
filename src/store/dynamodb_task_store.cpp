#include "flowd/store/dynamodb_task_store.hpp"

#include "flowd/core/error.hpp"
#include "flowd/core/log.hpp"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/DynamoDBErrors.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/AttributeValue.h>
#include <aws/dynamodb/model/BatchWriteItemRequest.h>
#include <aws/dynamodb/model/CreateTableRequest.h>
#include <aws/dynamodb/model/DeleteTableRequest.h>
#include <aws/dynamodb/model/DescribeTableRequest.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/GlobalSecondaryIndex.h>
#include <aws/dynamodb/model/KeySchemaElement.h>
#include <aws/dynamodb/model/Projection.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/Put.h>
#include <aws/dynamodb/model/PutRequest.h>
#include <aws/dynamodb/model/QueryRequest.h>
#include <aws/dynamodb/model/TransactWriteItem.h>
#include <aws/dynamodb/model/TransactWriteItemsRequest.h>
#include <aws/dynamodb/model/UpdateItemRequest.h>
#include <aws/dynamodb/model/WriteRequest.h>

#include <algorithm>
#include <charconv>
#include <format>
#include <thread>

namespace flowd {

namespace ddb = Aws::DynamoDB;
namespace model = Aws::DynamoDB::Model;

using Item = Aws::Map<Aws::String, model::AttributeValue>;
using Values = Aws::Map<Aws::String, model::AttributeValue>;

namespace {

constexpr const char* kIndexName = "sched";
constexpr std::size_t kTransactLimit = 100;
constexpr std::size_t kBatchLimit = 25;

// ---- attribute helpers ----------------------------------------------------

model::AttributeValue S(std::string_view v) {
    return model::AttributeValue(Aws::String(v));
}

model::AttributeValue N(std::int64_t v) {
    model::AttributeValue a;
    a.SetN(Aws::String(std::to_string(v)));
    return a;
}

model::AttributeValue D(double v) {
    model::AttributeValue a;
    a.SetN(Aws::String(std::format("{}", v)));
    return a;
}

model::AttributeValue B(bool v) {
    model::AttributeValue a;
    a.SetBool(v);
    return a;
}

model::AttributeValue SS(const std::vector<std::string>& v) {
    model::AttributeValue a;
    Aws::Vector<Aws::String> values(v.begin(), v.end());
    a.SetSS(values);
    return a;
}

std::string pk(std::string_view workflow_id) {
    return std::string("WF#").append(workflow_id);
}

std::string sk(std::string_view task_id) {
    return std::string("TASK#").append(task_id);
}

std::string sched_pk(std::string_view kind, std::uint32_t shard) {
    return std::format("{}#{}", kind, shard);
}

Item key_of(const TaskKey& key) {
    return Item{{"pk", S(pk(key.workflow_id))}, {"sk", S(sk(key.task_id))}};
}

std::string truncate(std::string_view text) {
    if (text.size() <= DynamoDbTaskStore::kMaxTextBytes) {
        return std::string(text);
    }
    return std::string(text.substr(0, DynamoDbTaskStore::kMaxTextBytes)) + "\n[truncated]";
}

const model::AttributeValue* find(const Item& item, const char* name) {
    auto it = item.find(name);
    return it == item.end() ? nullptr : &it->second;
}

std::string get_s(const Item& item, const char* name) {
    const auto* a = find(item, name);
    return a == nullptr ? std::string{} : std::string(a->GetS());
}

std::int64_t get_n(const Item& item, const char* name, std::int64_t fallback = 0) {
    const auto* a = find(item, name);
    if (a == nullptr || a->GetN().empty()) {
        return fallback;
    }
    const Aws::String& s = a->GetN();
    std::int64_t v = fallback;
    std::from_chars(s.data(), s.data() + s.size(), v);
    return v;
}

double get_d(const Item& item, const char* name, double fallback) {
    const auto* a = find(item, name);
    if (a == nullptr || a->GetN().empty()) {
        return fallback;
    }
    return std::stod(std::string(a->GetN()));
}

bool get_b(const Item& item, const char* name) {
    const auto* a = find(item, name);
    return a != nullptr && a->GetBool();
}

std::vector<std::string> get_ss(const Item& item, const char* name) {
    std::vector<std::string> out;
    if (const auto* a = find(item, name)) {
        for (const auto& s : a->GetSS()) {
            out.emplace_back(s);
        }
    }
    std::ranges::sort(out);
    return out;
}

Item to_item(const TaskRecord& r) {
    Item item = key_of(r.key());
    item["wf"] = S(r.workflow_id);
    item["task"] = S(r.task_id);
    item["handler"] = S(r.handler);
    if (!r.payload.empty()) {
        item["payload"] = S(r.payload);
    }
    item["state"] = S(to_string(r.state));
    item["attempt"] = N(r.attempt);
    item["max_attempts"] = N(r.retry.max_attempts);
    item["backoff_initial_ms"] = N(r.retry.initial_backoff.count());
    item["backoff_multiplier"] = D(r.retry.multiplier);
    item["backoff_max_ms"] = N(r.retry.max_backoff.count());
    item["backoff_jitter"] = D(r.retry.jitter);
    item["timeout_ms"] = N(r.timeout.count());
    if (!r.depends_on.empty()) {
        item["deps"] = SS(r.depends_on);
    }
    if (!r.children.empty()) {
        item["children"] = SS(r.children);
    }
    // String sets cannot be empty: an absent "pending" means "no pending
    // dependencies", which is also what DynamoDB leaves behind when DELETE
    // removes the last element.
    if (!r.pending_deps.empty()) {
        item["pending"] = SS(r.pending_deps);
    }
    item["available_at"] = N(r.available_at);
    item["needs_prop"] = B(r.needs_propagation);
    item["shard"] = N(r.shard);
    item["created_at"] = N(r.created_at);
    item["updated_at"] = N(r.updated_at);
    if (r.state == TaskState::Pending && r.pending_deps.empty()) {
        item["sched_pk"] = S(sched_pk("READY", r.shard));
        item["sched_at"] = N(r.available_at);
    }
    return item;
}

TaskRecord from_item(const Item& item) {
    TaskRecord r;
    r.workflow_id = get_s(item, "wf");
    r.task_id = get_s(item, "task");
    r.handler = get_s(item, "handler");
    r.payload = get_s(item, "payload");
    const std::string state = get_s(item, "state");
    auto parsed = parse_task_state(state);
    if (!parsed) {
        throw StoreError("task " + r.workflow_id + "/" + r.task_id + " has invalid state '" +
                         state + "'");
    }
    r.state = *parsed;
    r.attempt = static_cast<std::uint32_t>(get_n(item, "attempt"));
    r.retry.max_attempts = static_cast<std::uint32_t>(get_n(item, "max_attempts", 1));
    r.retry.initial_backoff = std::chrono::milliseconds{get_n(item, "backoff_initial_ms")};
    r.retry.multiplier = get_d(item, "backoff_multiplier", 2.0);
    r.retry.max_backoff = std::chrono::milliseconds{get_n(item, "backoff_max_ms")};
    r.retry.jitter = get_d(item, "backoff_jitter", 0.0);
    r.timeout = std::chrono::milliseconds{get_n(item, "timeout_ms")};
    r.depends_on = get_ss(item, "deps");
    r.children = get_ss(item, "children");
    r.pending_deps = get_ss(item, "pending");
    r.lease_owner = get_s(item, "lease_owner");
    r.lease_expires_at = get_n(item, "lease_expires_at");
    r.available_at = get_n(item, "available_at");
    r.output = get_s(item, "output");
    r.last_error = get_s(item, "last_error");
    r.needs_propagation = get_b(item, "needs_prop");
    r.shard = static_cast<std::uint32_t>(get_n(item, "shard"));
    r.created_at = get_n(item, "created_at");
    r.updated_at = get_n(item, "updated_at");
    r.finished_at = get_n(item, "finished_at");
    return r;
}

TaskKey key_from_item(const Item& item) {
    std::string wf = get_s(item, "pk");
    std::string task = get_s(item, "sk");
    if (wf.starts_with("WF#")) {
        wf.erase(0, 3);
    }
    if (task.starts_with("TASK#")) {
        task.erase(0, 5);
    }
    return TaskKey{std::move(wf), std::move(task)};
}

template <typename Outcome>
[[noreturn]] void throw_error(std::string_view op, const Outcome& outcome) {
    const auto& err = outcome.GetError();
    throw StoreError(std::format("DynamoDB {} failed: {} ({})", op, std::string(err.GetMessage()),
                                 std::string(err.GetExceptionName())));
}

template <typename Outcome>
bool is_condition_failure(const Outcome& outcome) {
    return outcome.GetError().GetErrorType() == ddb::DynamoDBErrors::CONDITIONAL_CHECK_FAILED;
}

} // namespace

// ---- SDK lifetime -------------------------------------------------------------

namespace {
Aws::SDKOptions& sdk_options() {
    static Aws::SDKOptions options;
    return options;
}
} // namespace

AwsSdkSession::AwsSdkSession() {
    Aws::InitAPI(sdk_options());
}

AwsSdkSession::~AwsSdkSession() {
    Aws::ShutdownAPI(sdk_options());
}

// ---- implementation -----------------------------------------------------------

class DynamoDbTaskStore::Impl {
public:
    explicit Impl(const DynamoDbOptions& o) : table(o.table_name) {
        // The default configuration probes the EC2 instance metadata service
        // to discover the region, which costs several seconds per process
        // off EC2. Skip it whenever the region is already known. (Instance
        // profile credentials are resolved separately and are unaffected.)
        Aws::Client::ClientConfigurationInitValues init;
        init.shouldDisableIMDS = !o.region.empty() || !o.endpoint.empty();
        Aws::Client::ClientConfiguration cfg(init);
        if (!o.region.empty()) {
            cfg.region = o.region;
        } else if (!o.endpoint.empty() && cfg.region.empty()) {
            cfg.region = "us-east-1";
        }
        if (!o.endpoint.empty()) {
            cfg.endpointOverride = o.endpoint;
            if (o.endpoint.starts_with("http://")) {
                cfg.scheme = Aws::Http::Scheme::HTTP;
            }
        }
        cfg.requestTimeoutMs = static_cast<long>(o.request_timeout.count());
        cfg.connectTimeoutMs = static_cast<long>(o.connect_timeout.count());
        cfg.maxConnections = static_cast<unsigned>(o.max_connections);
        // Throttling and transient 5xx errors are retried with exponential
        // backoff by the SDK; conditional check failures are not retried.
        cfg.retryStrategy = Aws::MakeShared<Aws::Client::DefaultRetryStrategy>(
            "flowd", static_cast<long>(o.max_retries), 25L);

        if (o.use_dummy_credentials) {
            if (o.endpoint.empty()) {
                throw StoreError("dummy credentials are only allowed with an endpoint override");
            }
            // DynamoDB Local accepts any alphanumeric key pair.
            client = std::make_shared<ddb::DynamoDBClient>(
                Aws::Auth::AWSCredentials("flowdlocal", "flowdlocal"), cfg);
        } else {
            client = std::make_shared<ddb::DynamoDBClient>(cfg);
        }
    }

    // Runs a conditional UpdateItem. Returns the new item, nullopt if the
    // condition failed; throws on any other error.
    std::optional<Item> update(const TaskKey& key, std::string expression, std::string condition,
                               Values values, bool return_new,
                               Aws::Map<Aws::String, Aws::String> names = {}) {
        model::UpdateItemRequest req;
        req.SetTableName(table);
        req.SetKey(key_of(key));
        req.SetUpdateExpression(Aws::String(expression));
        if (!condition.empty()) {
            req.SetConditionExpression(Aws::String(condition));
        }
        // Aliases for attribute names that are (or may become) DynamoDB
        // reserved words. Only aliases that occur are sent: DynamoDB rejects
        // unused placeholders.
        for (const auto& [alias, attribute] :
             {std::pair{"#st", "state"}, std::pair{"#att", "attempt"},
              std::pair{"#pend", "pending"}, std::pair{"#out", "output"}}) {
            if (expression.find(alias) != std::string::npos ||
                condition.find(alias) != std::string::npos) {
                names.emplace(alias, attribute);
            }
        }
        if (!names.empty()) {
            req.SetExpressionAttributeNames(names);
        }
        if (!values.empty()) {
            req.SetExpressionAttributeValues(values);
        }
        req.SetReturnValues(return_new ? model::ReturnValue::ALL_NEW : model::ReturnValue::NONE);
        auto outcome = client->UpdateItem(req);
        if (!outcome.IsSuccess()) {
            if (is_condition_failure(outcome)) {
                return std::nullopt;
            }
            throw_error("UpdateItem", outcome);
        }
        return outcome.GetResult().GetAttributes();
    }

    std::vector<Item> query_index(const std::string& partition, const char* op, TimestampMs bound,
                                  std::size_t limit) {
        std::vector<Item> out;
        Item start_key;
        while (out.size() < limit) {
            model::QueryRequest req;
            req.SetTableName(table);
            req.SetIndexName(kIndexName);
            req.SetKeyConditionExpression(Aws::String(std::format("sched_pk = :p AND sched_at {} :t", op)));
            req.SetExpressionAttributeValues(Values{{":p", S(partition)}, {":t", N(bound)}});
            req.SetScanIndexForward(true);
            req.SetLimit(static_cast<int>(std::min<std::size_t>(limit - out.size(), 1000)));
            if (!start_key.empty()) {
                req.SetExclusiveStartKey(start_key);
            }
            auto outcome = client->Query(req);
            if (!outcome.IsSuccess()) {
                throw_error("Query", outcome);
            }
            for (const auto& item : outcome.GetResult().GetItems()) {
                out.push_back(item);
            }
            start_key = outcome.GetResult().GetLastEvaluatedKey();
            if (start_key.empty()) {
                break;
            }
        }
        return out;
    }

    void batch_put(std::vector<Item> items) {
        for (std::size_t offset = 0; offset < items.size(); offset += kBatchLimit) {
            Aws::Vector<model::WriteRequest> writes;
            for (std::size_t i = offset; i < std::min(items.size(), offset + kBatchLimit); ++i) {
                model::PutRequest put;
                put.SetItem(items[i]);
                model::WriteRequest w;
                w.SetPutRequest(put);
                writes.push_back(std::move(w));
            }
            auto delay = std::chrono::milliseconds{25};
            for (int round = 0; !writes.empty(); ++round) {
                model::BatchWriteItemRequest req;
                req.AddRequestItems(table, writes);
                auto outcome = client->BatchWriteItem(req);
                if (!outcome.IsSuccess()) {
                    throw_error("BatchWriteItem", outcome);
                }
                const auto& unprocessed = outcome.GetResult().GetUnprocessedItems();
                auto it = unprocessed.find(table);
                writes = it == unprocessed.end() ? Aws::Vector<model::WriteRequest>{} : it->second;
                if (!writes.empty()) {
                    if (round >= 20) {
                        throw StoreError("BatchWriteItem: items still unprocessed after retries");
                    }
                    std::this_thread::sleep_for(delay);
                    delay = std::min(delay * 2, std::chrono::milliseconds{2000});
                }
            }
        }
    }

    Aws::String table;
    std::shared_ptr<ddb::DynamoDBClient> client;
};

DynamoDbTaskStore::DynamoDbTaskStore(DynamoDbOptions options)
    : options_(std::move(options)), impl_(std::make_unique<Impl>(options_)) {
    if (options_.shard_count == 0) {
        options_.shard_count = 1;
    }
}

DynamoDbTaskStore::~DynamoDbTaskStore() = default;

bool DynamoDbTaskStore::table_exists() {
    model::DescribeTableRequest req;
    req.SetTableName(impl_->table);
    auto outcome = impl_->client->DescribeTable(req);
    if (outcome.IsSuccess()) {
        return true;
    }
    if (outcome.GetError().GetErrorType() == ddb::DynamoDBErrors::RESOURCE_NOT_FOUND) {
        return false;
    }
    throw_error("DescribeTable", outcome);
}

bool DynamoDbTaskStore::create_table(std::chrono::seconds timeout) {
    bool created = false;
    if (!table_exists()) {
        model::CreateTableRequest req;
        req.SetTableName(impl_->table);
        req.SetBillingMode(model::BillingMode::PAY_PER_REQUEST);
        for (const auto& [name, type] :
             {std::pair{"pk", model::ScalarAttributeType::S}, std::pair{"sk", model::ScalarAttributeType::S},
              std::pair{"sched_pk", model::ScalarAttributeType::S},
              std::pair{"sched_at", model::ScalarAttributeType::N}}) {
            model::AttributeDefinition def;
            def.SetAttributeName(name);
            def.SetAttributeType(type);
            req.AddAttributeDefinitions(def);
        }
        auto key = [](const char* name, model::KeyType type) {
            model::KeySchemaElement k;
            k.SetAttributeName(name);
            k.SetKeyType(type);
            return k;
        };
        req.AddKeySchema(key("pk", model::KeyType::HASH));
        req.AddKeySchema(key("sk", model::KeyType::RANGE));

        model::GlobalSecondaryIndex gsi;
        gsi.SetIndexName(kIndexName);
        gsi.AddKeySchema(key("sched_pk", model::KeyType::HASH));
        gsi.AddKeySchema(key("sched_at", model::KeyType::RANGE));
        model::Projection projection;
        projection.SetProjectionType(model::ProjectionType::ALL);
        gsi.SetProjection(projection);
        req.AddGlobalSecondaryIndexes(gsi);

        auto outcome = impl_->client->CreateTable(req);
        if (!outcome.IsSuccess() &&
            outcome.GetError().GetErrorType() != ddb::DynamoDBErrors::RESOURCE_IN_USE) {
            throw_error("CreateTable", outcome);
        }
        created = outcome.IsSuccess();
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        model::DescribeTableRequest req;
        req.SetTableName(impl_->table);
        auto outcome = impl_->client->DescribeTable(req);
        if (outcome.IsSuccess()) {
            const auto& t = outcome.GetResult().GetTable();
            bool ready = t.GetTableStatus() == model::TableStatus::ACTIVE;
            for (const auto& idx : t.GetGlobalSecondaryIndexes()) {
                ready = ready && idx.GetIndexStatus() == model::IndexStatus::ACTIVE;
            }
            if (ready) {
                return created;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
    }
    throw StoreError("table '" + std::string(impl_->table) + "' did not become ACTIVE in time");
}

void DynamoDbTaskStore::delete_table() {
    model::DeleteTableRequest req;
    req.SetTableName(impl_->table);
    auto outcome = impl_->client->DeleteTable(req);
    if (!outcome.IsSuccess() &&
        outcome.GetError().GetErrorType() != ddb::DynamoDBErrors::RESOURCE_NOT_FOUND) {
        throw_error("DeleteTable", outcome);
    }
}

// ---- workflows ----------------------------------------------------------------

void DynamoDbTaskStore::create_workflow(const WorkflowRecord& workflow,
                                        const std::vector<TaskRecord>& tasks) {
    Item meta{{"pk", S(pk(workflow.workflow_id))},
              {"sk", S("META")},
              {"wf", S(workflow.workflow_id)},
              {"name", S(workflow.name)},
              {"task_count", N(static_cast<std::int64_t>(workflow.task_count))},
              {"created_at", N(workflow.created_at)}};
    // Keep the item well below DynamoDB's 400 KB limit.
    if (!workflow.definition.empty() && workflow.definition.size() < 300 * 1024) {
        meta["definition"] = S(workflow.definition);
    }

    std::vector<Item> items;
    items.reserve(tasks.size());
    for (const TaskRecord& t : tasks) {
        TaskRecord copy = t;
        // Transitions derive index partitions from the key, so the stored
        // shard must be the canonical one for this table's shard count.
        copy.shard = shard_for(t.workflow_id, t.task_id, options_.shard_count);
        items.push_back(to_item(copy));
    }

    if (items.size() + 1 <= kTransactLimit) {
        // Small workflows are created atomically.
        model::TransactWriteItemsRequest req;
        model::Put put_meta;
        put_meta.SetTableName(impl_->table);
        put_meta.SetItem(meta);
        put_meta.SetConditionExpression("attribute_not_exists(pk)");
        model::TransactWriteItem first;
        first.SetPut(put_meta);
        req.AddTransactItems(first);
        for (const Item& item : items) {
            model::Put put;
            put.SetTableName(impl_->table);
            put.SetItem(item);
            put.SetConditionExpression("attribute_not_exists(pk)");
            model::TransactWriteItem w;
            w.SetPut(put);
            req.AddTransactItems(w);
        }
        auto outcome = impl_->client->TransactWriteItems(req);
        if (!outcome.IsSuccess()) {
            if (outcome.GetError().GetErrorType() == ddb::DynamoDBErrors::TRANSACTION_CANCELED &&
                get_workflow(workflow.workflow_id)) {
                throw AlreadyExists("workflow '" + workflow.workflow_id + "' already exists");
            }
            throw_error("TransactWriteItems", outcome);
        }
        return;
    }

    // Large workflows: claim the id with a conditional put, then write tasks
    // in batches. Non-root tasks are written before roots so that whenever a
    // task can start, everything downstream of it already exists.
    model::PutItemRequest put;
    put.SetTableName(impl_->table);
    put.SetItem(meta);
    put.SetConditionExpression("attribute_not_exists(pk)");
    auto outcome = impl_->client->PutItem(put);
    if (!outcome.IsSuccess()) {
        if (is_condition_failure(outcome)) {
            throw AlreadyExists("workflow '" + workflow.workflow_id + "' already exists");
        }
        throw_error("PutItem", outcome);
    }
    std::stable_partition(items.begin(), items.end(),
                          [](const Item& item) { return item.find("sched_pk") == item.end(); });
    impl_->batch_put(std::move(items));
}

std::optional<WorkflowRecord> DynamoDbTaskStore::get_workflow(std::string_view id) {
    model::GetItemRequest req;
    req.SetTableName(impl_->table);
    req.SetKey(Item{{"pk", S(pk(id))}, {"sk", S("META")}});
    req.SetConsistentRead(true);
    auto outcome = impl_->client->GetItem(req);
    if (!outcome.IsSuccess()) {
        throw_error("GetItem", outcome);
    }
    const Item& item = outcome.GetResult().GetItem();
    if (item.empty()) {
        return std::nullopt;
    }
    return WorkflowRecord{.workflow_id = get_s(item, "wf"),
                          .name = get_s(item, "name"),
                          .task_count = static_cast<std::size_t>(get_n(item, "task_count")),
                          .created_at = get_n(item, "created_at"),
                          .definition = get_s(item, "definition")};
}

std::vector<TaskRecord> DynamoDbTaskStore::list_tasks(std::string_view workflow_id) {
    std::vector<TaskRecord> out;
    Item start_key;
    do {
        model::QueryRequest req;
        req.SetTableName(impl_->table);
        req.SetKeyConditionExpression("pk = :p AND begins_with(sk, :t)");
        req.SetExpressionAttributeValues(Values{{":p", S(pk(workflow_id))}, {":t", S("TASK#")}});
        req.SetConsistentRead(true);
        if (!start_key.empty()) {
            req.SetExclusiveStartKey(start_key);
        }
        auto outcome = impl_->client->Query(req);
        if (!outcome.IsSuccess()) {
            throw_error("Query", outcome);
        }
        for (const auto& item : outcome.GetResult().GetItems()) {
            out.push_back(from_item(item));
        }
        start_key = outcome.GetResult().GetLastEvaluatedKey();
    } while (!start_key.empty());
    return out;
}

std::optional<TaskRecord> DynamoDbTaskStore::get_task(const TaskKey& key) {
    model::GetItemRequest req;
    req.SetTableName(impl_->table);
    req.SetKey(key_of(key));
    req.SetConsistentRead(true);
    auto outcome = impl_->client->GetItem(req);
    if (!outcome.IsSuccess()) {
        throw_error("GetItem", outcome);
    }
    const Item& item = outcome.GetResult().GetItem();
    if (item.empty()) {
        return std::nullopt;
    }
    return from_item(item);
}

// ---- discovery ----------------------------------------------------------------

std::vector<TaskKey> DynamoDbTaskStore::find_ready(std::uint32_t shard, TimestampMs now,
                                                   std::size_t limit) {
    std::vector<TaskKey> out;
    for (const Item& item : impl_->query_index(sched_pk("READY", shard), "<=", now, limit)) {
        out.push_back(key_from_item(item));
    }
    return out;
}

std::vector<TaskRecord> DynamoDbTaskStore::find_expired_leases(std::uint32_t shard,
                                                               TimestampMs now,
                                                               std::size_t limit) {
    std::vector<TaskRecord> out;
    for (const Item& item : impl_->query_index(sched_pk("LEASE", shard), "<", now, limit)) {
        out.push_back(from_item(item));
    }
    return out;
}

std::vector<TaskRecord> DynamoDbTaskStore::find_unpropagated(std::uint32_t shard,
                                                             TimestampMs finished_before,
                                                             std::size_t limit) {
    std::vector<TaskRecord> out;
    for (const Item& item :
         impl_->query_index(sched_pk("PROP", shard), "<", finished_before, limit)) {
        out.push_back(from_item(item));
    }
    return out;
}

// ---- conditional transitions -------------------------------------------------

std::optional<TaskRecord> DynamoDbTaskStore::try_claim(const TaskKey& key,
                                                       std::string_view worker_id, TimestampMs now,
                                                       TimestampMs lease_expires_at) {
    // The READY marker exists only while the task is claimable, so checking
    // it (plus the state, defensively) is the whole precondition.
    auto item = impl_->update(
        key,
        "SET #st = :running, #att = #att + :one, lease_owner = :w, "
        "lease_expires_at = :exp, sched_pk = :lease_pk, sched_at = :exp, updated_at = :now",
        "begins_with(sched_pk, :ready) AND available_at <= :now AND #st IN (:pending, :retrying)",
        Values{{":running", S("RUNNING")},
               {":pending", S("PENDING")},
               {":retrying", S("RETRYING")},
               {":one", N(1)},
               {":w", S(worker_id)},
               {":exp", N(lease_expires_at)},
               {":now", N(now)},
               {":ready", S("READY#")},
               {":lease_pk", S(sched_pk("LEASE", shard_for(key.workflow_id, key.task_id,
                                                             options_.shard_count)))}},
        true);
    if (!item) {
        return std::nullopt;
    }
    return from_item(*item);
}

bool DynamoDbTaskStore::renew_lease(const TaskKey& key, std::string_view worker_id,
                                    std::uint32_t attempt, TimestampMs lease_expires_at) {
    return impl_
        ->update(key, "SET lease_expires_at = :exp, sched_at = :exp",
                 "#st = :running AND #att = :a AND lease_owner = :w",
                 Values{{":running", S("RUNNING")},
                        {":a", N(attempt)},
                        {":w", S(worker_id)},
                        {":exp", N(lease_expires_at)}},
                 false)
        .has_value();
}

bool DynamoDbTaskStore::complete(const TaskKey& key, std::string_view worker_id,
                                 std::uint32_t attempt, std::string_view output, TimestampMs now) {
    const std::uint32_t shard = shard_for(key.workflow_id, key.task_id, options_.shard_count);
    return impl_
        ->update(key,
                 "SET #st = :succeeded, #out = :out, finished_at = :now, updated_at = :now, "
                 "needs_prop = :true, sched_pk = :prop, sched_at = :now "
                 "REMOVE lease_owner, lease_expires_at, last_error",
                 "#st = :running AND #att = :a AND lease_owner = :w",
                 Values{{":running", S("RUNNING")},
                        {":succeeded", S("SUCCEEDED")},
                        {":a", N(attempt)},
                        {":w", S(worker_id)},
                        {":out", S(truncate(output))},
                        {":now", N(now)},
                        {":true", B(true)},
                        {":prop", S(sched_pk("PROP", shard))}},
                 false)
        .has_value();
}

namespace {

// Update expression + values shared by fail() and expire_lease().
std::pair<std::string, Values> failure_update(const FailureTransition& t, std::uint32_t shard) {
    Values v{{":err", S(truncate(t.error))}, {":now", N(t.now)}};
    if (t.next == TaskState::Retrying) {
        v[":next"] = S("RETRYING");
        v[":avail"] = N(t.available_at);
        v[":ready"] = S(sched_pk("READY", shard));
        return {"SET #st = :next, last_error = :err, available_at = :avail, updated_at = :now, "
                "sched_pk = :ready, sched_at = :avail REMOVE lease_owner, lease_expires_at",
                v};
    }
    if (t.next != TaskState::Failed) {
        throw InvalidTransition("a failed attempt must move to RETRYING or FAILED");
    }
    v[":next"] = S("FAILED");
    v[":true"] = B(true);
    v[":prop"] = S(sched_pk("PROP", shard));
    return {"SET #st = :next, last_error = :err, finished_at = :now, updated_at = :now, "
            "needs_prop = :true, sched_pk = :prop, sched_at = :now "
            "REMOVE lease_owner, lease_expires_at",
            v};
}

} // namespace

bool DynamoDbTaskStore::fail(const TaskKey& key, std::string_view worker_id, std::uint32_t attempt,
                             const FailureTransition& transition) {
    auto [expression, values] = failure_update(
        transition, shard_for(key.workflow_id, key.task_id, options_.shard_count));
    values[":running"] = S("RUNNING");
    values[":a"] = N(attempt);
    values[":w"] = S(worker_id);
    return impl_
        ->update(key, expression, "#st = :running AND #att = :a AND lease_owner = :w", values,
                 false)
        .has_value();
}

bool DynamoDbTaskStore::expire_lease(const TaskKey& key, std::uint32_t attempt,
                                     const FailureTransition& transition) {
    auto [expression, values] = failure_update(
        transition, shard_for(key.workflow_id, key.task_id, options_.shard_count));
    values[":running"] = S("RUNNING");
    values[":a"] = N(attempt);
    return impl_
        ->update(key, expression, "#st = :running AND #att = :a AND lease_expires_at < :now",
                 values, false)
        .has_value();
}

// ---- propagation -----------------------------------------------------------------

bool DynamoDbTaskStore::resolve_dependency(const TaskKey& child, std::string_view parent_id,
                                           TimestampMs now) {
    // Step 1: DELETE from a string set is idempotent. The condition stops the
    // update from materialising an item for a task that does not exist.
    auto item = impl_->update(child, "DELETE #pend :parent", "attribute_exists(pk)",
                              Values{{":parent", SS({std::string(parent_id)})}}, true);
    if (!item) {
        return false;
    }
    if (item->contains("pending") || item->contains("sched_pk") ||
        get_s(*item, "state") != "PENDING") {
        return false;
    }
    // Step 2: publish the READY marker. If we crash between the steps the
    // parent's needs_prop flag is still set, so the repairer re-runs both;
    // the condition makes concurrent or repeated publication harmless.
    const auto shard = static_cast<std::uint32_t>(get_n(*item, "shard"));
    return impl_
        ->update(child,
                 "SET sched_pk = :ready, sched_at = :now, available_at = :now, updated_at = :now",
                 "attribute_not_exists(#pend) AND attribute_not_exists(sched_pk) AND #st = :pending",
                 Values{{":ready", S(sched_pk("READY", shard))},
                        {":now", N(now)},
                        {":pending", S("PENDING")}},
                 false)
        .has_value();
}

std::optional<TaskRecord> DynamoDbTaskStore::fail_upstream(const TaskKey& child,
                                                           std::string_view error,
                                                           TimestampMs now) {
    const std::uint32_t shard = shard_for(child.workflow_id, child.task_id, options_.shard_count);
    auto item = impl_->update(
        child,
        "SET #st = :failed, last_error = :err, finished_at = :now, updated_at = :now, "
        "needs_prop = :true, sched_pk = :prop, sched_at = :now",
        "#st = :pending",
        Values{{":failed", S("FAILED")},
               {":pending", S("PENDING")},
               {":err", S(truncate(error))},
               {":now", N(now)},
               {":true", B(true)},
               {":prop", S(sched_pk("PROP", shard))}},
        true);
    if (!item) {
        return std::nullopt;
    }
    return from_item(*item);
}

void DynamoDbTaskStore::mark_propagated(const TaskKey& key) {
    (void)impl_->update(key, "SET needs_prop = :false REMOVE sched_pk, sched_at",
                        "needs_prop = :true", Values{{":true", B(true)}, {":false", B(false)}},
                        false);
}

} // namespace flowd
