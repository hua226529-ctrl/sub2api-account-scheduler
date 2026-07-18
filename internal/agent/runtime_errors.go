package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
)

type runtimeErrorClass string

const (
	runtimeErrorModelContractInvalid      runtimeErrorClass = "model_contract_invalid"
	runtimeErrorModelToolArgumentsInvalid runtimeErrorClass = "model_tool_arguments_invalid"
	runtimeErrorModelNoProgress           runtimeErrorClass = "model_no_progress"
	runtimeErrorProviderRateLimited       runtimeErrorClass = "provider_rate_limited"
	runtimeErrorProviderTimeout           runtimeErrorClass = "provider_timeout"
	runtimeErrorProviderAuthFailed        runtimeErrorClass = "provider_auth_failed"
	runtimeErrorProviderServer            runtimeErrorClass = "provider_server_error"
	runtimeErrorRuntimeInternal           runtimeErrorClass = "runtime_internal_error"
	runtimeErrorExternalMutationUncertain runtimeErrorClass = "external_mutation_uncertain"
)

type classifiedRuntimeError struct {
	class runtimeErrorClass
	err   error
}

type providerConfigurationError struct{ err error }

func (e *providerConfigurationError) Error() string { return e.err.Error() }
func (e *providerConfigurationError) Unwrap() error { return e.err }

func (e *classifiedRuntimeError) Error() string {
	if e == nil || e.err == nil {
		return string(e.class)
	}
	return fmt.Sprintf("%s: %v", e.class, e.err)
}

func (e *classifiedRuntimeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func newRuntimeError(class runtimeErrorClass, err error) error {
	if err == nil {
		err = errors.New(string(class))
	}
	return &classifiedRuntimeError{class: class, err: err}
}

func runtimeErrorClassOf(err error) runtimeErrorClass {
	var classified *classifiedRuntimeError
	if errors.As(err, &classified) {
		return classified.class
	}
	return runtimeErrorRuntimeInternal
}

func runtimeErrorRetryable(class runtimeErrorClass) bool {
	switch class {
	case runtimeErrorProviderRateLimited, runtimeErrorProviderTimeout, runtimeErrorProviderServer:
		return true
	default:
		return false
	}
}

func classifyProviderError(err error) error {
	if err == nil {
		return nil
	}
	var configuration *providerConfigurationError
	if errors.As(err, &configuration) {
		return newRuntimeError(runtimeErrorProviderAuthFailed, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return newRuntimeError(runtimeErrorProviderTimeout, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return newRuntimeError(runtimeErrorProviderTimeout, err)
	}
	return newRuntimeError(runtimeErrorRuntimeInternal, err)
}

func preferredRuntimeFailure(current, candidate error) error {
	if current == nil {
		return candidate
	}
	currentClass, candidateClass := runtimeErrorClassOf(current), runtimeErrorClassOf(candidate)
	if runtimeErrorRetryable(candidateClass) && !runtimeErrorRetryable(currentClass) {
		return candidate
	}
	if currentClass == runtimeErrorRuntimeInternal && candidateClass != runtimeErrorRuntimeInternal {
		return candidate
	}
	return current
}

func classifyProviderStatus(status int, message string) error {
	err := fmt.Errorf("模型接口返回 %d: %s", status, message)
	switch {
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return newRuntimeError(runtimeErrorProviderAuthFailed, err)
	case status == http.StatusTooManyRequests:
		return newRuntimeError(runtimeErrorProviderRateLimited, err)
	case status >= 500:
		return newRuntimeError(runtimeErrorProviderServer, err)
	default:
		return newRuntimeError(runtimeErrorRuntimeInternal, err)
	}
}
