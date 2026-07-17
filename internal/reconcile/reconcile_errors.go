package reconcile

import (
	"errors"
	"fmt"
	"strings"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
)

type AccountReconcileIssue struct {
	AccountID int64
	Status    accountcontrol.MutationStatus
	Code      string
	Err       error
}

func (i AccountReconcileIssue) Error() string {
	return fmt.Sprintf("account %d reconcile %s (%s): %v", i.AccountID, i.Status, i.Code, i.Err)
}

func (i AccountReconcileIssue) Unwrap() error { return i.Err }

type AccountReconcileErrors struct {
	Issues []AccountReconcileIssue
}

func (e *AccountReconcileErrors) Error() string {
	parts := make([]string, 0, len(e.Issues))
	for _, issue := range e.Issues {
		parts = append(parts, issue.Error())
	}
	return strings.Join(parts, "; ")
}

func (e *AccountReconcileErrors) Unwrap() []error {
	result := make([]error, 0, len(e.Issues))
	for _, issue := range e.Issues {
		result = append(result, issue)
	}
	return result
}

func accountReconcileIssue(accountID int64, err error) AccountReconcileIssue {
	issue := AccountReconcileIssue{AccountID: accountID, Status: accountcontrol.StatusFailed, Code: "account_error", Err: err}
	var blocked *accountcontrol.BlockedError
	if errors.As(err, &blocked) {
		issue.Status = accountcontrol.StatusBlocked
		issue.Code = string(blocked.Result.BlockedReason)
		return issue
	}
	var state *accountcontrol.MutationStateError
	if errors.As(err, &state) {
		issue.Status = state.Result.Status
		issue.Code = string(state.Result.BlockedReason)
		if issue.Code == "" {
			issue.Code = "mutation_" + string(state.Result.Status)
		}
	}
	return issue
}

func expectedPolicyBlock(err error) bool {
	var blocked *accountcontrol.BlockedError
	if !errors.As(err, &blocked) {
		return false
	}
	switch blocked.Result.BlockedReason {
	case accountcontrol.BlockWritesFrozen, accountcontrol.BlockStaleTelemetry, accountcontrol.BlockCooldown:
		return true
	default:
		return false
	}
}
