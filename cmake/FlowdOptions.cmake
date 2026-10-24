# Compiler warnings and sanitizer flags shared by every Flowd target.

add_library(flowd_options INTERFACE)
add_library(flowd::options ALIAS flowd_options)

target_compile_features(flowd_options INTERFACE cxx_std_20)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(flowd_options INTERFACE
        -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion
        -Wnon-virtual-dtor -Wold-style-cast -Wcast-align -Woverloaded-virtual
        -Wnull-dereference -Wimplicit-fallthrough
        # Designated initializers that rely on default member initializers are
        # intentional throughout the code base.
        -Wno-missing-field-initializers)
    if(FLOWD_WARNINGS_AS_ERRORS)
        target_compile_options(flowd_options INTERFACE -Werror)
    endif()
endif()

if(FLOWD_SANITIZER)
    if(FLOWD_SANITIZER STREQUAL "thread")
        set(_flowd_san_flags -fsanitize=thread)
    elseif(FLOWD_SANITIZER STREQUAL "address")
        set(_flowd_san_flags -fsanitize=address)
    elseif(FLOWD_SANITIZER STREQUAL "undefined")
        set(_flowd_san_flags -fsanitize=undefined -fno-sanitize-recover=undefined)
    elseif(FLOWD_SANITIZER STREQUAL "address+undefined")
        set(_flowd_san_flags -fsanitize=address,undefined -fno-sanitize-recover=undefined)
    else()
        message(FATAL_ERROR "Unknown FLOWD_SANITIZER '${FLOWD_SANITIZER}'")
    endif()
    # Applied globally so that GoogleTest and other fetched dependencies are
    # instrumented as well; mixing instrumented and plain code produces false
    # positives under ThreadSanitizer.
    add_compile_options(${_flowd_san_flags} -fno-omit-frame-pointer -g)
    add_link_options(${_flowd_san_flags})
endif()
