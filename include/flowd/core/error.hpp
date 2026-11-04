#pragma once

#include <stdexcept>
#include <string>

namespace flowd {

// Base class for all errors raised by Flowd.
class Error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The workflow definition is malformed (duplicate ids, unknown dependencies,
// cycles, invalid retry settings, ...).
class ValidationError : public Error {
public:
    using Error::Error;
};

// An attempted state transition is not an edge of the task state machine.
class InvalidTransition : public Error {
public:
    using Error::Error;
};

// The persistence layer failed in a way the caller cannot recover from by
// retrying the same operation immediately.
class StoreError : public Error {
public:
    using Error::Error;
};

// A workflow with the given id already exists.
class AlreadyExists : public StoreError {
public:
    using StoreError::StoreError;
};

} // namespace flowd
