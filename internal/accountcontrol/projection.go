package accountcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func accountMatchesIntent(account model.Account, intent controlplane.Intent) bool {
	if value, ok := intent.DesiredState.Schedulable(); ok {
		return account.Schedulable == value
	}
	value, configured, ok := intent.DesiredState.LoadFactor()
	if !ok {
		return false
	}
	if !configured {
		return account.LoadFactor == nil
	}
	return account.LoadFactor != nil && *account.LoadFactor == value
}

func sameAccountState(left, right model.Account) bool {
	if left.Schedulable != right.Schedulable {
		return false
	}
	if left.LoadFactor == nil || right.LoadFactor == nil {
		return left.LoadFactor == nil && right.LoadFactor == nil
	}
	return *left.LoadFactor == *right.LoadFactor
}

func stateFromAccount(account model.Account) AccountState {
	return AccountState{Schedulable: account.Schedulable, LoadFactor: cloneInt(account.LoadFactor)}
}

func resultFromMutation(mutation Mutation, replay bool) Result {
	result := Result{
		CommandID: mutation.CommandID, MutationID: mutation.ID, IntentID: mutation.IntentID,
		AccountID: mutation.AccountID, Operation: mutation.Operation, WinningAuthority: mutation.WinningAuthority,
		Status: mutation.Status, Before: mutation.Before, VerifiedAfter: mutation.After, ExpiresAt: cloneTime(mutation.ExpiresAt),
		IdempotentReplay: replay, Uncertain: mutation.Status == StatusUncertain || mutation.Status == StatusVerifying,
		BlockedReason: BlockReason(mutation.LastErrorCode), RequestedLoadSet: mutation.RequestedLoadSet,
	}
	result.ExpiresAt = cloneTime(mutation.WinningExpiresAt)
	if mutation.RequestedSchedulable != nil {
		result.Requested.Schedulable = *mutation.RequestedSchedulable
	}
	if mutation.RequestedLoadSet {
		result.Requested.LoadFactor = cloneInt(mutation.RequestedLoadFactor)
	}
	return result
}

func replayMutation(mutation Mutation) (Result, error) {
	result := resultFromMutation(mutation, true)
	switch mutation.Status {
	case StatusApplied, StatusAppliedNoop, StatusSuperseded, StatusExpired:
		return result, nil
	case StatusBlocked:
		return result, &BlockedError{Result: result}
	default:
		return result, &MutationStateError{Result: result}
	}
}

func updateControl(control model.AccountControl, mutation Mutation, verified model.Account, submission Submission, now time.Time) model.AccountControl {
	control.AccountID = mutation.AccountID
	control.LastActionAt = &now
	control.LastDecision = string(mutation.Status)
	if mutation.Operation == controlplane.OperationSetAccountSchedulable {
		value := verified.Schedulable
		control.ExpectedSchedulable = &value
		control.LastObserved = &value
		control.OwnsPause = !value
		if value {
			control.Owner = ""
			control.ManualLocked = false
			if mutation.WinningAuthority == controlplane.AuthorityActivePolicy {
				control.FlapActive = false
				control.FlapTriggeredAt = nil
				control.FlapRecoveryRequired = 0
			}
		} else {
			switch mutation.WinningAuthority {
			case controlplane.AuthorityManualHold, controlplane.AuthorityAdministratorCommand:
				control.Owner = "operator"
			case controlplane.AuthorityAutonomousAgent:
				control.Owner = "agent"
			default:
				control.Owner = policyPauseOwner(control)
			}
			control.ManualLocked = mutation.WinningAuthority == controlplane.AuthorityManualHold
		}
		control.ManualOverrideUntil = cloneTime(mutation.WinningExpiresAt)
	} else {
		if control.OriginalLoadFactor == nil && mutation.Before != nil {
			control.OriginalLoadFactor = cloneInt(mutation.Before.LoadFactor)
		}
		control.ExpectedLoadFactor = cloneInt(verified.LoadFactor)
		control.OwnsLoadFactor = mutation.WinningAuthority == controlplane.AuthorityActivePolicy && mutation.WinningLoadSet
		if submission.Event.Type == "load_factor_restored" {
			control.OwnsLoadFactor = false
			control.OriginalLoadFactor = nil
			control.LoadStage = model.HealthStageHealthy
			control.RecoveryStep = 0
			control.RecoveryStartedAt = nil
			control.LoadOverrideUntil = nil
			control.LoadPinValue = nil
			control.LoadPinUntil = nil
			control.LoadPinOwner = ""
			control.LoadPinReason = ""
		}
		if mutation.WinningOverrideKind == OverrideKindLoadPin {
			control.OwnsLoadFactor = mutation.WinningLoadSet
			control.LoadPinValue = cloneInt(verified.LoadFactor)
			control.LoadPinUntil = cloneTime(mutation.WinningExpiresAt)
			control.LoadPinOwner = mutation.WinningActor
			control.LoadPinReason = mutation.WinningReason
			control.LoadOverrideUntil = nil
		} else if mutation.WinningAuthority == controlplane.AuthorityAdministratorCommand || mutation.WinningAuthority == controlplane.AuthorityAutonomousAgent {
			control.LoadOverrideUntil = cloneTime(mutation.WinningExpiresAt)
		} else if submission.RevokeOverrideID != "" {
			control.LoadPinValue = nil
			control.LoadPinUntil = nil
			control.LoadPinOwner = ""
			control.LoadPinReason = ""
			control.LoadOverrideUntil = nil
		} else if mutation.WinningAuthority == controlplane.AuthorityActivePolicy {
			control.LoadOverrideUntil = nil
			if control.LoadPinUntil != nil && !control.LoadPinUntil.After(now) {
				control.LoadPinValue = nil
				control.LoadPinUntil = nil
				control.LoadPinOwner = ""
				control.LoadPinReason = ""
			}
		}
	}
	control.UpdatedAt = now
	return control
}

func policyPauseOwner(control model.AccountControl) string {
	count := 0
	if control.HealthLocked {
		count++
	}
	if control.BalanceLocked {
		count++
	}
	if control.CostLocked {
		count++
	}
	if count > 1 {
		return "combined"
	}
	if control.BalanceLocked {
		return "balance"
	}
	if control.CostLocked {
		return "cost"
	}
	return "automatic"
}

func sameInt(left, right *int) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func defaultEvent(mutation Mutation, status MutationStatus, reason BlockReason, now time.Time) model.Event {
	message := "account mutation " + string(status)
	if reason != BlockNone {
		message += ": " + string(reason)
	}
	return model.Event{Type: "account_mutation_" + string(status), Severity: "info", AccountID: &mutation.AccountID,
		GoalID: mutation.GoalID, StepID: mutation.StepID, Message: message, Actor: mutation.Actor, CreatedAt: now}
}

func formatState(state *AccountState) string {
	if state == nil {
		return ""
	}
	payload, _ := json.Marshal(state)
	return string(payload)
}

func ambiguousWriteError(err error) bool {
	if err == nil {
		return false
	}
	var networkError net.Error
	if errors.As(err, &networkError) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "returned 4") {
		return false
	}
	return strings.Contains(message, "timeout") || strings.Contains(message, "connection") || strings.Contains(message, "returned 5")
}
