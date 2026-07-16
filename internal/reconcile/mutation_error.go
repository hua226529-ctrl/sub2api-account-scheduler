package reconcile

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/mutation"
)

// ExternalMutationError means a write request may have reached Sub2API but a
// confirmed final state was not obtained. Callers must read back; retrying the
// write blindly is unsafe.
type ExternalMutationError = mutation.Error

func IsExternalMutationUncertain(err error) bool {
	return mutation.IsUncertain(err)
}

func uncertainExternalMutation(operation string, err error) error {
	return mutation.Wrap(operation, err)
}

// mutationDefinitelyRejected distinguishes a response which proves the write
// was rejected from transport and response-processing failures where the
// server may already have applied it. Implementations may expose the optional
// interface below; the Sub2API client currently reports HTTP status errors as
// text, so 4xx responses are also recognized here.
func mutationDefinitelyRejected(err error) bool {
	return mutation.DefinitelyRejected(err)
}

func mutationRollbackFailure(commitErr, rollbackErr error, unconfirmed string) error {
	if rollbackErr == nil {
		rollbackErr = errors.New(strings.TrimSpace(unconfirmed))
	}
	return fmt.Errorf("本地提交失败且外部回滚未确认: %w", errors.Join(commitErr, rollbackErr))
}
