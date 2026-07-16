package reconcile

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplaneshadow"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type EngineOption func(*Engine)

func WithControlplaneShadow(observer controlplaneshadow.Observer) EngineOption {
	return func(engine *Engine) {
		engine.shadow = controlplaneshadow.NewRuntime(observer)
	}
}

func (e *Engine) observeManualPause(ctx context.Context, accountID int64, actor string) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	createdAt := shadowCreatedAt(actionContext)
	observedAt := shadowObservedAt(time.Time{})
	legacy := shadowLegacyContext(actionContext, actor, "账号已由管理端暂停调度", createdAt, nil)
	producer := controlplane.ProducerAdminUI
	adapter := func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptPermanentManualPause(controlplanebridge.AccountActionInput{Context: legacy, AccountID: accountID})
	}
	if actionContext.StableSourceNamespace == controlplanebridge.SourceAdministratorGrantConsumption {
		producer = controlplane.ProducerAgentOperator
		adapter = func() controlplanebridge.ConversionResult {
			return controlplanebridge.AdaptAgentAdministratorAccountSchedulable(controlplanebridge.AccountSchedulableInput{
				Context: legacy, AccountID: accountID, Schedulable: false,
			}, actionContext.AdministratorAuthorization)
		}
	}
	e.shadow.Observe(controlplaneshadow.NewAccountSchedulableAction(controlplaneshadow.PathManualPause, producer, accountID, false, observedAt), adapter)
}

func (e *Engine) observeManualResume(ctx context.Context, accountID int64, actor string) {
	if !e.shadow.Enabled() {
		return
	}
	observedAt := shadowObservedAt(time.Time{})
	e.shadow.Observe(controlplaneshadow.NewAccountSchedulableAction(controlplaneshadow.PathManualResume,
		controlplane.ProducerAdminUI, accountID, true, observedAt), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptManualResume(controlplanebridge.AccountActionInput{
			Context: controlplanebridge.LegacyContext{Actor: strings.TrimSpace(actor)}, AccountID: accountID,
		})
	})
}

func (e *Engine) observeAdministratorSchedulable(ctx context.Context, path controlplaneshadow.Path, accountID int64,
	value bool, actor, reason string, observedAt time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	createdAt := shadowCreatedAt(actionContext)
	legacy := shadowLegacyContext(actionContext, actor, reason, createdAt, nil)
	producer := controlplane.ProducerAdminUI
	adapter := func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptTemporaryAdministratorAccountSchedulable(controlplanebridge.AccountSchedulableInput{
			Context: legacy, AccountID: accountID, Schedulable: value,
		})
	}
	if actionContext.StableSourceNamespace == controlplanebridge.SourceAdministratorGrantConsumption {
		producer = controlplane.ProducerAgentOperator
		adapter = func() controlplanebridge.ConversionResult {
			return controlplanebridge.AdaptAgentAdministratorAccountSchedulable(controlplanebridge.AccountSchedulableInput{
				Context: legacy, AccountID: accountID, Schedulable: value,
			}, actionContext.AdministratorAuthorization)
		}
	}
	e.shadow.Observe(controlplaneshadow.NewAccountSchedulableAction(path, producer, accountID, value, shadowObservedAt(observedAt)), adapter)
}

func (e *Engine) observeAutonomousSchedulable(ctx context.Context, path controlplaneshadow.Path, accountID int64,
	value bool, actor, reason string, observedAt time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	createdAt := shadowCreatedAt(actionContext)
	legacy := shadowLegacyContext(actionContext, actor, reason, createdAt, nil)
	e.shadow.Observe(controlplaneshadow.NewAccountSchedulableAction(path, controlplane.ProducerAgentOperator,
		accountID, value, shadowObservedAt(observedAt)), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptAutonomousAgentAccountSchedulable(controlplanebridge.AccountSchedulableInput{
			Context: legacy, AccountID: accountID, Schedulable: value,
		})
	})
}

func (e *Engine) observeAdministratorLoadFactor(ctx context.Context, path controlplaneshadow.Path, accountID int64,
	value *int, actor, reason string, observedAt time.Time, defaultExpiration *time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	createdAt := shadowCreatedAt(actionContext)
	legacy := shadowLegacyContext(actionContext, actor, reason, createdAt, defaultExpiration)
	producer := controlplane.ProducerAdminUI
	adapter := func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptTemporaryAdministratorLoadFactor(controlplanebridge.AccountLoadFactorInput{
			Context: legacy, AccountID: accountID, LoadFactor: value,
		})
	}
	if actionContext.StableSourceNamespace == controlplanebridge.SourceAdministratorGrantConsumption {
		producer = controlplane.ProducerAgentOperator
		adapter = func() controlplanebridge.ConversionResult {
			return controlplanebridge.AdaptAgentAdministratorLoadFactor(controlplanebridge.AccountLoadFactorInput{
				Context: legacy, AccountID: accountID, LoadFactor: value,
			}, actionContext.AdministratorAuthorization)
		}
	}
	e.shadow.Observe(controlplaneshadow.NewAccountLoadFactorAction(path, producer, accountID, value, shadowObservedAt(observedAt)), adapter)
}

func (e *Engine) observeAutonomousLoadFactor(ctx context.Context, path controlplaneshadow.Path, accountID int64,
	value *int, actor, reason string, observedAt time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	createdAt := shadowCreatedAt(actionContext)
	legacy := shadowLegacyContext(actionContext, actor, reason, createdAt, nil)
	e.shadow.Observe(controlplaneshadow.NewAccountLoadFactorAction(path, controlplane.ProducerAgentOperator,
		accountID, value, shadowObservedAt(observedAt)), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptAutonomousAgentLoadFactor(controlplanebridge.AccountLoadFactorInput{
			Context: legacy, AccountID: accountID, LoadFactor: value,
		})
	})
}

func (e *Engine) observePinnedLoadFactor(ctx context.Context, accountID int64, value *int, actor, reason string,
	observedAt, until time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	if actionContext.StableSourceNamespace == controlplanebridge.SourceAgentAction {
		createdAt := shadowCreatedAt(actionContext)
		legacy := shadowLegacyContext(actionContext, actor, reason, createdAt, &until)
		e.shadow.Observe(controlplaneshadow.NewAccountLoadFactorAction(controlplaneshadow.PathPinLoad,
			controlplane.ProducerAgentOperator, accountID, value, shadowObservedAt(observedAt)), func() controlplanebridge.ConversionResult {
			return controlplanebridge.AdaptAutonomousAgentLoadFactor(controlplanebridge.AccountLoadFactorInput{
				Context: legacy, AccountID: accountID, LoadFactor: value,
			})
		})
		return
	}
	e.observeAdministratorLoadFactor(ctx, controlplaneshadow.PathPinLoad, accountID, value, actor, reason, observedAt, &until)
}

func (e *Engine) observePolicySchedulable(ctx context.Context, path controlplaneshadow.Path, binding *model.ResolvedBinding,
	value bool, reason string, observedAt time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	legacy := policyLegacyContext(actionContext, binding, reason, observedAt)
	e.shadow.Observe(controlplaneshadow.NewAccountSchedulableAction(path, controlplane.ProducerPolicyScheduler,
		binding.Account.ID, value, shadowObservedAt(observedAt)), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptPolicyAccountSchedulable(controlplanebridge.AccountSchedulableInput{
			Context: legacy, AccountID: binding.Account.ID, Schedulable: value,
		})
	})
}

func (e *Engine) observePolicyLoadFactor(ctx context.Context, binding *model.ResolvedBinding, value *int,
	reason string, observedAt time.Time) {
	if !e.shadow.Enabled() {
		return
	}
	actionContext := controlplaneshadow.ActionContextFrom(ctx)
	legacy := policyLegacyContext(actionContext, binding, reason, observedAt)
	e.shadow.Observe(controlplaneshadow.NewAccountLoadFactorAction(controlplaneshadow.PathReconcilePolicyLoad,
		controlplane.ProducerPolicyScheduler, binding.Account.ID, value, shadowObservedAt(observedAt)), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptPolicyAccountLoadFactor(controlplanebridge.AccountLoadFactorInput{
			Context: legacy, AccountID: binding.Account.ID, LoadFactor: value,
		})
	})
}

func policyLegacyContext(actionContext controlplaneshadow.ActionContext, binding *model.ResolvedBinding,
	reason string, observedAt time.Time) controlplanebridge.LegacyContext {
	createdAt := actionContext.CreatedAt
	if createdAt.IsZero() {
		createdAt = observedAt
	}
	legacy := shadowLegacyContext(actionContext, "scheduler", reason, createdAt, nil)
	if legacy.PolicyVersion == "" && binding != nil && binding.Policy.ScorePolicyVersionID != nil {
		legacy.PolicyVersion = fmt.Sprintf("score_policy_version:%d", *binding.Policy.ScorePolicyVersionID)
	}
	return legacy
}

func shadowLegacyContext(actionContext controlplaneshadow.ActionContext, actor, reason string,
	createdAt time.Time, defaultExpiration *time.Time) controlplanebridge.LegacyContext {
	if strings.TrimSpace(actionContext.Reason) != "" {
		reason = actionContext.Reason
	}
	expiresAt := actionContext.ExpiresAt
	if expiresAt == nil && defaultExpiration != nil {
		copy := defaultExpiration.UTC()
		expiresAt = &copy
	}
	return controlplanebridge.LegacyContext{
		StableSourceNamespace: actionContext.StableSourceNamespace,
		StableSourceID:        actionContext.StableSourceID,
		Actor:                 strings.TrimSpace(actor),
		Reason:                strings.TrimSpace(reason),
		EvidenceRefs:          append([]string(nil), actionContext.EvidenceRefs...),
		PolicyVersion:         actionContext.PolicyVersion,
		SnapshotVersion:       actionContext.SnapshotVersion,
		CreatedAt:             createdAt,
		ExpiresAt:             expiresAt,
	}
}

func shadowCreatedAt(actionContext controlplaneshadow.ActionContext) time.Time {
	if !actionContext.CreatedAt.IsZero() {
		return actionContext.CreatedAt.UTC()
	}
	return time.Time{}
}

func shadowObservedAt(fallback time.Time) time.Time {
	if !fallback.IsZero() {
		return fallback.UTC()
	}
	return time.Now().UTC()
}

func reasonForAdaptiveLoad(restore bool) string {
	if restore {
		return "账号健康恢复后还原原始负载"
	}
	return "账号负载已按渠道健康状态调整"
}
