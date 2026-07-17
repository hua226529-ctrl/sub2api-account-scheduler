package accountcontrol

import (
	"context"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func (s *Service) checkSafety(ctx context.Context, intent controlplane.Intent, account model.Account, control model.AccountControl, safety SafetyContext) (BlockReason, error) {
	freeze, err := s.repository.GetAgentFreezeState(ctx, "global", "")
	if err != nil {
		return BlockWritesFrozen, err
	}
	mode := freeze.Mode
	if freeze.ExpiresAt != nil && !freeze.ExpiresAt.After(s.now()) {
		mode = model.AgentFreezeModeActive
	}
	if mode == model.AgentFreezeModeWritesFrozen {
		return BlockWritesFrozen, nil
	}
	if intent.Producer == controlplane.ProducerAgentOperator &&
		(mode == model.AgentFreezeModeAgentPaused || mode == model.AgentFreezeModeReadOnly) {
		return BlockAgentWritesFrozen, nil
	}
	if intent.Authority == controlplane.AuthorityActivePolicy && mode == model.AgentFreezeModeReadOnly {
		return BlockWritesFrozen, nil
	}

	unsafeIncrease := unsafeTarget(intent, account)
	if unsafeIncrease && !accountCredentialValid(account, s.now()) {
		return BlockCredentialInvalid, nil
	}
	if unsafeIncrease {
		if accountRateLimited(account, s.now()) {
			return BlockRateLimited, nil
		}
		if control.HealthLocked {
			return BlockHealthLocked, nil
		}
		balanceLock, lockErr := s.repository.GetActiveBalanceLock(ctx, account.ID)
		if lockErr != nil {
			return BlockBalanceLocked, lockErr
		}
		if balanceLock != nil || control.BalanceLocked {
			return BlockBalanceLocked, nil
		}
		costLock, lockErr := s.repository.GetActiveCostLock(ctx, account.ID)
		if lockErr != nil {
			return BlockCostLocked, lockErr
		}
		if costLock != nil || control.CostLocked {
			return BlockCostLocked, nil
		}
	}
	if intent.Authority == controlplane.AuthorityActivePolicy || intent.Authority == controlplane.AuthorityAutonomousAgent {
		if !safety.TelemetryFresh {
			return BlockStaleTelemetry, nil
		}
		if safety.CooldownActive {
			return BlockCooldown, nil
		}
	}
	return BlockNone, nil
}

func unsafeTarget(intent controlplane.Intent, account model.Account) bool {
	switch intent.Operation {
	case controlplane.OperationSetAccountSchedulable:
		value, ok := intent.DesiredState.Schedulable()
		return ok && value
	case controlplane.OperationSetAccountLoadFactor:
		value, configured, ok := intent.DesiredState.LoadFactor()
		if !ok {
			return true
		}
		current := 100
		if account.LoadFactor != nil {
			current = *account.LoadFactor
		}
		if !configured {
			return account.LoadFactor != nil
		}
		return value > current
	default:
		return true
	}
}

func accountCredentialValid(account model.Account, now time.Time) bool {
	if account.Status != "active" || strings.TrimSpace(account.ErrorMessage) != "" {
		return false
	}
	credentialStatus := strings.ToLower(strings.TrimSpace(account.CredentialStatus))
	if credentialStatus == "invalid" || credentialStatus == "expired" || credentialStatus == "error" {
		return false
	}
	if account.ExpiresAt != nil && *account.ExpiresAt > 0 && time.Unix(*account.ExpiresAt, 0).Before(now) {
		return false
	}
	return true
}

func accountRateLimited(account model.Account, now time.Time) bool {
	for _, until := range []*time.Time{account.RateLimitResetAt, account.OverloadUntil, account.TempUnschedulableUntil} {
		if until != nil && until.After(now) {
			return true
		}
	}
	return false
}
