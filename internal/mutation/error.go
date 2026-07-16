package mutation

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var definiteHTTP4xx = regexp.MustCompile(`(?i)(?:HTTP\s*|returned(?:\s+code)?\s+)(4\d\d)\b`)

// Error means an external write may have reached its target but no confirmed
// final state was obtained. Callers must reconcile through readback instead of
// blindly replaying the mutation.
type Error struct {
	Operation string
	Err       error
}

func (e *Error) Error() string {
	if e == nil {
		return "外部写入结果不明确"
	}
	operation := strings.TrimSpace(e.Operation)
	if operation == "" {
		operation = "外部写入"
	}
	return fmt.Sprintf("%s结果不明确: %v", operation, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *Error) MutationOutcomeUncertain() bool { return true }

func IsUncertain(err error) bool {
	var target interface{ MutationOutcomeUncertain() bool }
	return errors.As(err, &target) && target.MutationOutcomeUncertain()
}

func Wrap(operation string, err error) error {
	if err == nil {
		err = errors.New("未获得确认结果")
	}
	if DefinitelyRejected(err) {
		operation = strings.TrimSpace(operation)
		if operation == "" {
			operation = "外部写入"
		}
		return fmt.Errorf("%s被上游明确拒绝: %w", operation, err)
	}
	return &Error{Operation: operation, Err: err}
}

func DefinitelyRejected(err error) bool {
	if err == nil {
		return false
	}
	var rejected interface{ MutationDefinitelyRejected() bool }
	if errors.As(err, &rejected) && rejected.MutationDefinitelyRejected() {
		return true
	}
	return definiteHTTP4xx.MatchString(err.Error())
}
