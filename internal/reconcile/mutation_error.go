package reconcile

import (
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
