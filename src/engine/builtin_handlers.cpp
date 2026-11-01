#include "flowd/core/sleep.hpp"
#include "flowd/engine/handler.hpp"

#include <nlohmann/json.hpp>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <system_error>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ; // NOLINT(readability-redundant-declaration)

namespace flowd {
namespace {

using nlohmann::json;
using Clock = std::chrono::steady_clock;

constexpr std::size_t kMaxCapturedOutput = std::size_t{64} * 1024;

json parse_payload(const TaskContext& ctx) {
    if (ctx.payload.empty()) {
        return json::object();
    }
    json doc = json::parse(ctx.payload, nullptr, /*allow_exceptions=*/false);
    if (doc.is_discarded()) {
        throw std::invalid_argument("payload is not valid JSON");
    }
    return doc;
}

TaskResult sleep_handler(const TaskContext& ctx) {
    const json payload = parse_payload(ctx);
    const auto ms = std::chrono::milliseconds{payload.value("ms", std::int64_t{0})};
    if (!interruptible_sleep(ms, ctx.stop)) {
        return TaskResult::failure("sleep interrupted");
    }
    return TaskResult::success();
}

TaskResult fail_handler(const TaskContext& ctx) {
    const json payload = parse_payload(ctx);
    if (payload.value("fatal", false)) {
        return TaskResult::fatal("configured to fail permanently");
    }
    const auto times = payload.value("times", std::uint32_t{UINT32_MAX});
    if (ctx.attempt <= times) {
        return TaskResult::failure("configured failure on attempt " + std::to_string(ctx.attempt));
    }
    return TaskResult::success("succeeded on attempt " + std::to_string(ctx.attempt));
}

TaskResult echo_handler(const TaskContext& ctx) {
    const json payload = parse_payload(ctx);
    if (payload.is_object() && payload.contains("message") && payload["message"].is_string()) {
        return TaskResult::success(payload["message"].get<std::string>());
    }
    return TaskResult::success(std::string(ctx.payload));
}

class Pipe {
public:
    Pipe() {
        if (::pipe2(fds_, O_CLOEXEC) != 0) {
            throw std::system_error(errno, std::system_category(), "pipe2");
        }
    }
    Pipe(const Pipe&) = delete;
    Pipe& operator=(const Pipe&) = delete;
    ~Pipe() {
        close_read();
        close_write();
    }
    [[nodiscard]] int read_end() const { return fds_[0]; }
    [[nodiscard]] int write_end() const { return fds_[1]; }
    void close_read() { close_fd(fds_[0]); }
    void close_write() { close_fd(fds_[1]); }

private:
    static void close_fd(int& fd) {
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }
    int fds_[2] = {-1, -1};
};

std::string describe_status(int status) {
    if (WIFEXITED(status)) {
        return "exit status " + std::to_string(WEXITSTATUS(status));
    }
    if (WIFSIGNALED(status)) {
        return "killed by signal " + std::to_string(WTERMSIG(status));
    }
    return "status " + std::to_string(status);
}

TaskResult shell_handler(const TaskContext& ctx) {
    const json payload = parse_payload(ctx);
    if (!payload.is_object() || !payload.contains("command") || !payload["command"].is_string()) {
        return TaskResult::fatal("shell handler requires a string 'command' in the payload");
    }
    const std::string command = payload["command"].get<std::string>();

    // Expose identity and the idempotency key to the script.
    std::vector<std::string> env_storage;
    for (char** e = environ; e != nullptr && *e != nullptr; ++e) {
        env_storage.emplace_back(*e);
    }
    env_storage.push_back("FLOWD_WORKFLOW_ID=" + std::string(ctx.workflow_id));
    env_storage.push_back("FLOWD_TASK_ID=" + std::string(ctx.task_id));
    env_storage.push_back("FLOWD_ATTEMPT=" + std::to_string(ctx.attempt));
    env_storage.push_back("FLOWD_IDEMPOTENCY_KEY=" + ctx.idempotency_key());
    std::vector<char*> envp;
    envp.reserve(env_storage.size() + 1);
    for (auto& s : env_storage) {
        envp.push_back(s.data());
    }
    envp.push_back(nullptr);

    Pipe out;
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_adddup2(&actions, out.write_end(), STDOUT_FILENO);
    posix_spawn_file_actions_adddup2(&actions, out.write_end(), STDERR_FILENO);
    posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null", O_RDONLY, 0);
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);
    // Own process group so the whole tree can be killed on timeout.
    posix_spawnattr_setflags(&attr, POSIX_SPAWN_SETPGROUP);
    posix_spawnattr_setpgroup(&attr, 0);

    std::string sh = "/bin/sh";
    std::string dash_c = "-c";
    std::string cmd = command;
    char* argv[] = {sh.data(), dash_c.data(), cmd.data(), nullptr};
    pid_t pid = -1;
    const int rc = ::posix_spawn(&pid, "/bin/sh", &actions, &attr, argv, envp.data());
    posix_spawn_file_actions_destroy(&actions);
    posix_spawnattr_destroy(&attr);
    if (rc != 0) {
        return TaskResult::failure("posix_spawn failed: " + std::system_category().message(rc));
    }
    out.close_write();

    const auto start = Clock::now();
    std::string captured;
    bool truncated = false;
    bool killed = false;
    std::string kill_reason;
    char buf[4096];
    for (;;) {
        if (!killed) {
            const bool timed_out = ctx.timeout.count() > 0 && Clock::now() - start >= ctx.timeout;
            if (timed_out || ctx.stop.stop_requested()) {
                kill_reason = timed_out ? "timed out after " + std::to_string(ctx.timeout.count()) + " ms"
                                        : "cancelled";
                ::kill(-pid, SIGKILL);
                killed = true;
            }
        }
        pollfd pfd{out.read_end(), POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 50);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (ready == 0) {
            continue;
        }
        const ssize_t n = ::read(out.read_end(), buf, sizeof(buf));
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            break; // EOF: every writer (the process tree) closed the pipe
        }
        const auto len = static_cast<std::size_t>(n);
        if (captured.size() < kMaxCapturedOutput) {
            captured.append(buf, std::min(len, kMaxCapturedOutput - captured.size()));
        } else {
            truncated = true;
        }
    }
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
    if (truncated) {
        captured += "\n[output truncated]";
    }
    if (killed) {
        return TaskResult::failure("command " + kill_reason);
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return TaskResult::success(std::move(captured));
    }
    std::string tail = captured.size() > 512 ? captured.substr(captured.size() - 512) : captured;
    return TaskResult::failure("command failed with " + describe_status(status) +
                               (tail.empty() ? "" : ": " + tail));
}

} // namespace

void register_builtin_handlers(HandlerRegistry& registry) {
    registry.add("noop", [](const TaskContext&) { return TaskResult::success(); });
    registry.add("sleep", sleep_handler);
    registry.add("fail", fail_handler);
    registry.add("echo", echo_handler);
    registry.add("shell", shell_handler);
}

} // namespace flowd
