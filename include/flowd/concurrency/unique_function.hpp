#pragma once

#include <cstddef>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace flowd::concurrency {

template <typename Signature>
class UniqueFunction;

// Move-only type-erased callable (a C++20 stand-in for std::move_only_function).
// Callables up to kInlineSize bytes that are nothrow-movable are stored inline,
// so submitting a typical lambda to the thread pool does not allocate.
template <typename R, typename... Args>
class UniqueFunction<R(Args...)> {
public:
    static constexpr std::size_t kInlineSize = 48;

    UniqueFunction() noexcept = default;
    UniqueFunction(std::nullptr_t) noexcept {}

    template <typename F>
        requires(!std::is_same_v<std::remove_cvref_t<F>, UniqueFunction> &&
                 std::is_invocable_r_v<R, std::decay_t<F>&, Args...>)
    UniqueFunction(F&& f) { // NOLINT(google-explicit-constructor)
        using Fn = std::decay_t<F>;
        if constexpr (fits_inline<Fn>()) {
            ::new (static_cast<void*>(storage_)) Fn(std::forward<F>(f));
            ops_ = &inline_ops<Fn>;
        } else {
            *reinterpret_cast<Fn**>(storage_) = new Fn(std::forward<F>(f));
            ops_ = &heap_ops<Fn>;
        }
    }

    UniqueFunction(UniqueFunction&& other) noexcept : ops_(other.ops_) {
        if (ops_ != nullptr) {
            ops_->move(other.storage_, storage_);
            other.ops_ = nullptr;
        }
    }

    UniqueFunction& operator=(UniqueFunction&& other) noexcept {
        if (this != &other) {
            reset();
            ops_ = other.ops_;
            if (ops_ != nullptr) {
                ops_->move(other.storage_, storage_);
                other.ops_ = nullptr;
            }
        }
        return *this;
    }

    UniqueFunction(const UniqueFunction&) = delete;
    UniqueFunction& operator=(const UniqueFunction&) = delete;

    ~UniqueFunction() { reset(); }

    explicit operator bool() const noexcept { return ops_ != nullptr; }

    R operator()(Args... args) { return ops_->invoke(storage_, std::forward<Args>(args)...); }

private:
    struct Ops {
        R (*invoke)(void*, Args&&...);
        void (*move)(void* from, void* to) noexcept;
        void (*destroy)(void*) noexcept;
    };

    template <typename Fn>
    static constexpr bool fits_inline() {
        return sizeof(Fn) <= kInlineSize && alignof(Fn) <= alignof(std::max_align_t) &&
               std::is_nothrow_move_constructible_v<Fn>;
    }

    template <typename Fn>
    static constexpr Ops inline_ops{
        [](void* s, Args&&... a) -> R { return (*std::launder(static_cast<Fn*>(s)))(std::forward<Args>(a)...); },
        [](void* from, void* to) noexcept {
            Fn* src = std::launder(static_cast<Fn*>(from));
            ::new (to) Fn(std::move(*src));
            src->~Fn();
        },
        [](void* s) noexcept { std::launder(static_cast<Fn*>(s))->~Fn(); },
    };

    template <typename Fn>
    static constexpr Ops heap_ops{
        [](void* s, Args&&... a) -> R { return (**static_cast<Fn**>(s))(std::forward<Args>(a)...); },
        [](void* from, void* to) noexcept { *static_cast<Fn**>(to) = *static_cast<Fn**>(from); },
        [](void* s) noexcept { delete *static_cast<Fn**>(s); },
    };

    void reset() noexcept {
        if (ops_ != nullptr) {
            ops_->destroy(storage_);
            ops_ = nullptr;
        }
    }

    alignas(std::max_align_t) std::byte storage_[kInlineSize]{};
    const Ops* ops_ = nullptr;
};

} // namespace flowd::concurrency
