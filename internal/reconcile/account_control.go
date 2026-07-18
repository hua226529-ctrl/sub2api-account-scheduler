package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/accountcontrol"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func (e *Engine) ManualPauseCommand(ctx context.Context, accountID int64, actor, commandID string) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	created := time.Now().UTC()
	conversion := controlplanebridge.AdaptPermanentManualPause(controlplanebridge.AccountActionInput{
		AccountID: accountID, Context: administratorContext(commandID, actor, "manual pause", created, nil),
	})
	return e.submitConverted(ctx, conversion, accountcontrol.Submission{CommandID: commandID, PersistOverride: true,
		RequestIdempotencyKey: administratorCommandRequestKey(commandID),
		Safety:                accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: "manual_pause", Severity: "warning", Message: "账号已由管理端暂停调度"}})
}

func (e *Engine) ManualResumeCommand(ctx context.Context, accountID int64, actor, commandID string, ttl time.Duration) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	if ttl <= 0 {
		ttl = accountcontrol.DefaultAdministratorTTL
	}
	created := time.Now().UTC()
	expires := created.Add(ttl)
	conversion := controlplanebridge.AdaptTemporaryAdministratorAccountSchedulable(controlplanebridge.AccountSchedulableInput{
		AccountID: accountID, Schedulable: true, Context: administratorContext(commandID, actor, "manual resume", created, &expires),
	})
	hold, err := e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountSchedulable, controlplane.AuthorityManualHold)
	if err != nil {
		return accountcontrol.Result{}, err
	}
	submission := accountcontrol.Submission{CommandID: commandID, PersistOverride: true,
		RequestIdempotencyKey: administratorCommandRequestKey(commandID),
		Safety:                accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: "manual_resume", Severity: "warning", Message: "管理员临时恢复账号"}}
	if hold != nil {
		submission.RevokeOverrideID = hold.ID
	}
	return e.submitConverted(ctx, conversion, submission)
}

func (e *Engine) ReleaseManualHoldCommand(ctx context.Context, accountID int64, actor, commandID string) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	requestKey := administratorCommandRequestKey(commandID)
	requestSignature := stableDigest("account-command-sem-v1-", "release-manual-hold", strconv.FormatInt(accountID, 10), actor)
	if replay, err := e.accountControl.LookupResult(ctx, requestKey, requestSignature); replay != nil || err != nil {
		if replay == nil {
			return accountcontrol.Result{}, err
		}
		return *replay, err
	}
	hold, err := e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountSchedulable, controlplane.AuthorityManualHold)
	if err != nil {
		return accountcontrol.Result{}, err
	}
	if hold == nil {
		return accountcontrol.Result{}, errors.New("account has no active schedulable ManualHold")
	}
	binding, ok := findBinding(e.Snapshot().Bindings, accountID)
	if !ok {
		return accountcontrol.Result{}, errors.New("account is not present in the current policy snapshot")
	}
	desired := !(binding.Control.HealthLocked || binding.Control.BalanceLocked || binding.Control.CostLocked)
	intent, safety, err := e.policySchedulableIntent(ctx, &binding, desired, "manual hold released")
	if err != nil {
		return accountcontrol.Result{}, err
	}
	return e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: commandID, Intent: intent,
		RequestIdempotencyKey: requestKey, RequestSemanticSignature: requestSignature, RevokeOverrideID: hold.ID,
		Safety: safety, Event: model.Event{Type: "manual_hold_released", Severity: "info", Message: "管理员已解除永久人工保持"}})
}

func (e *Engine) ForceResumeCommand(ctx context.Context, accountID int64, actor, reason, commandID string, ttl time.Duration) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	if ttl <= 0 {
		ttl = accountcontrol.DefaultAdministratorTTL
	}
	created := time.Now().UTC()
	expires := created.Add(ttl)
	conversion := controlplanebridge.AdaptTemporaryAdministratorAccountSchedulable(controlplanebridge.AccountSchedulableInput{
		AccountID: accountID, Schedulable: true, Context: administratorContext(commandID, actor, fallbackReason(reason, "administrator force resume"), created, &expires),
	})
	return e.submitConverted(ctx, conversion, accountcontrol.Submission{CommandID: commandID, PersistOverride: true,
		RequestIdempotencyKey: administratorCommandRequestKey(commandID),
		Safety:                accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: "admin_force_resume", Severity: "warning", Message: "管理员已提交临时恢复命令"}})
}

func (e *Engine) ForceSetLoadFactorCommand(ctx context.Context, accountID int64, value *int, actor, reason, commandID string,
	ttl time.Duration) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	if ttl <= 0 {
		ttl = accountcontrol.DefaultAdministratorTTL
	}
	created := time.Now().UTC()
	expires := created.Add(ttl)
	conversion := controlplanebridge.AdaptTemporaryAdministratorLoadFactor(controlplanebridge.AccountLoadFactorInput{
		AccountID: accountID, LoadFactor: cloneIntPointer(value), Context: administratorContext(commandID, actor,
			fallbackReason(reason, "administrator load adjustment"), created, &expires),
	})
	return e.submitConverted(ctx, conversion, accountcontrol.Submission{CommandID: commandID, PersistOverride: true,
		RequestIdempotencyKey: administratorCommandRequestKey(commandID),
		Safety:                accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: "admin_force_load_factor", Severity: "warning", Message: "管理员已提交临时负载命令"}})
}

func (e *Engine) PinLoadCommand(ctx context.Context, accountID int64, value int, until *time.Time, permanent bool,
	actor, reason, commandID string) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	created := time.Now().UTC()
	legacy := administratorContext(commandID, actor, fallbackReason(reason, "administrator load pin"), created, until)
	var conversion controlplanebridge.ConversionResult
	if permanent {
		metadata := controlplane.IntentMetadata{ID: "pending", IdempotencyKey: stableDigest("pin-idem-v1-", commandID, strconv.FormatInt(accountID, 10)),
			Producer: controlplane.ProducerAdminUI, Authority: controlplane.AuthorityManualHold, Actor: actor,
			Reason: legacy.Reason, CreatedAt: created}
		intent, err := controlplane.NewAccountLoadFactorIntent(metadata, accountID, &value)
		if err == nil {
			signature, _ := controlplane.SemanticSignature(intent)
			intent.ID = stableDigest("pin-intent-v1-", intent.IdempotencyKey, signature)
		}
		if err != nil {
			return accountcontrol.Result{}, err
		}
		conversion = controlplanebridge.ConversionResult{Intent: &intent, Status: controlplanebridge.ConversionMapped, GapCode: controlplanebridge.GapNone}
	} else {
		if until == nil || !until.After(created) {
			return accountcontrol.Result{}, errors.New("temporary load pin requires a future expiration")
		}
		conversion = controlplanebridge.AdaptTemporaryAdministratorLoadFactor(controlplanebridge.AccountLoadFactorInput{AccountID: accountID, LoadFactor: &value, Context: legacy})
	}
	return e.submitConverted(ctx, conversion, accountcontrol.Submission{CommandID: commandID, RequestIdempotencyKey: administratorCommandRequestKey(commandID),
		PersistOverride: true, OverrideKind: accountcontrol.OverrideKindLoadPin,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: "load_pin_set", Severity: "warning", Message: "管理员已固定账号负载"}})
}

func (e *Engine) ClearLoadPinCommand(ctx context.Context, accountID int64, actor, reason, commandID string) (accountcontrol.Result, error) {
	if err := explicitAdministrator(actor); err != nil {
		return accountcontrol.Result{}, err
	}
	reason = fallbackReason(reason, "load pin released")
	requestKey := administratorCommandRequestKey(commandID)
	requestSignature := stableDigest("account-command-sem-v1-", "release-load-pin", strconv.FormatInt(accountID, 10), actor, reason)
	if replay, err := e.accountControl.LookupResult(ctx, requestKey, requestSignature); replay != nil || err != nil {
		if replay == nil {
			return accountcontrol.Result{}, err
		}
		return *replay, err
	}
	override, err := e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountLoadFactor, controlplane.AuthorityManualHold)
	if err != nil {
		return accountcontrol.Result{}, err
	}
	if override == nil {
		override, err = e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountLoadFactor, controlplane.AuthorityAdministratorCommand)
		if err != nil {
			return accountcontrol.Result{}, err
		}
	}
	if override == nil {
		return accountcontrol.Result{}, errors.New("account has no active load pin")
	}
	binding, ok := findBinding(e.Snapshot().Bindings, accountID)
	if !ok {
		return accountcontrol.Result{}, errors.New("account is not present in the current policy snapshot")
	}
	desired := cloneIntPointer(binding.Control.OriginalLoadFactor)
	intent, safety, err := e.policyLoadIntent(ctx, &binding, desired, reason)
	if err != nil {
		return accountcontrol.Result{}, err
	}
	return e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: commandID, Intent: intent,
		RequestIdempotencyKey: requestKey, RequestSemanticSignature: requestSignature,
		RevokeOverrideID: override.ID, Safety: safety, Event: model.Event{Type: "load_pin_cleared", Severity: "info", Message: "管理员已解除账号负载固定"}})
}

func (e *Engine) AgentReleaseManualHold(ctx context.Context, accountID int64, actor, reason string) error {
	command, err := consumedAdministratorCommand(ctx)
	if err != nil {
		return err
	}
	reason = fallbackReason(reason, "administrator agent released manual hold")
	requestKey := agentCommandRequestKey(command.CommandID)
	requestSignature := stableDigest("account-command-sem-v1-", "agent-release-manual-hold", strconv.FormatInt(accountID, 10), actor, reason)
	if replay, lookupErr := e.accountControl.LookupResult(ctx, requestKey, requestSignature); replay != nil || lookupErr != nil {
		if replay == nil {
			return lookupErr
		}
		return legacyMutationResult(*replay, lookupErr)
	}
	hold, err := e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountSchedulable, controlplane.AuthorityManualHold)
	if err != nil {
		return err
	}
	if hold == nil {
		return errors.New("account has no active schedulable ManualHold")
	}
	binding, ok := findBinding(e.Snapshot().Bindings, accountID)
	if !ok {
		return errors.New("account is not present in the current policy snapshot")
	}
	desired := !(binding.Control.HealthLocked || binding.Control.BalanceLocked || binding.Control.CostLocked)
	intent, safety, err := e.policySchedulableIntent(ctx, &binding, desired, reason)
	if err != nil {
		return err
	}
	result, err := e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: command.CommandID, Intent: intent,
		RequestIdempotencyKey: requestKey, RequestSemanticSignature: requestSignature, RevokeOverrideID: hold.ID,
		Safety: safety, Event: model.Event{Type: "manual_hold_released", Severity: "info", Message: "管理员通过 Agent 解除永久人工保持"}})
	return legacyMutationResult(result, err)
}

func (e *Engine) AgentReleaseLoadPin(ctx context.Context, accountID int64, actor, reason string) error {
	command, err := consumedAdministratorCommand(ctx)
	if err != nil {
		return err
	}
	reason = fallbackReason(reason, "administrator agent released load pin")
	requestKey := agentCommandRequestKey(command.CommandID)
	requestSignature := stableDigest("account-command-sem-v1-", "agent-release-load-pin", strconv.FormatInt(accountID, 10), actor, reason)
	if replay, lookupErr := e.accountControl.LookupResult(ctx, requestKey, requestSignature); replay != nil || lookupErr != nil {
		if replay == nil {
			return lookupErr
		}
		return legacyMutationResult(*replay, lookupErr)
	}
	override, err := e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountLoadFactor, controlplane.AuthorityManualHold)
	if err != nil {
		return err
	}
	if override == nil {
		override, err = e.accountControl.FindActiveOverride(ctx, accountID, controlplane.OperationSetAccountLoadFactor, controlplane.AuthorityAdministratorCommand)
		if err != nil {
			return err
		}
	}
	if override == nil {
		return errors.New("account has no active load pin")
	}
	binding, ok := findBinding(e.Snapshot().Bindings, accountID)
	if !ok {
		return errors.New("account is not present in the current policy snapshot")
	}
	intent, safety, err := e.policyLoadIntent(ctx, &binding, cloneIntPointer(binding.Control.OriginalLoadFactor), reason)
	if err != nil {
		return err
	}
	result, err := e.submitAccountMutation(ctx, accountcontrol.Submission{CommandID: command.CommandID, Intent: intent,
		RequestIdempotencyKey: requestKey, RequestSemanticSignature: requestSignature, RevokeOverrideID: override.ID,
		Safety: safety, Event: model.Event{Type: "load_pin_cleared", Severity: "info", Message: "管理员通过 Agent 解除负载固定"}})
	return legacyMutationResult(result, err)
}

func consumedAdministratorCommand(ctx context.Context) (accountcontrol.CommandContext, error) {
	command := accountcontrol.CommandContextFrom(ctx)
	if !command.Administrator || strings.TrimSpace(command.GrantConsumptionID) == "" || command.CommandID != command.GrantConsumptionID {
		return accountcontrol.CommandContext{}, errors.New("account release requires a consumed exact administrator grant")
	}
	return command, nil
}

func (e *Engine) agentSchedulable(ctx context.Context, accountID int64, value bool, actor, reason string) (accountcontrol.Result, error) {
	metadata, authorization, err := agentCommandMetadata(ctx, actor, fallbackReason(reason, "agent account action"))
	if err != nil {
		return accountcontrol.Result{}, err
	}
	input := controlplanebridge.AccountSchedulableInput{AccountID: accountID, Schedulable: value, Context: metadata}
	var conversion controlplanebridge.ConversionResult
	if authorization != nil {
		conversion = controlplanebridge.AdaptAgentAdministratorAccountSchedulable(input, *authorization)
	} else {
		conversion = controlplanebridge.AdaptAutonomousAgentAccountSchedulable(input)
	}
	return e.submitConverted(ctx, conversion, accountcontrol.Submission{CommandID: metadata.StableSourceID,
		RequestIdempotencyKey: agentCommandRequestKey(metadata.StableSourceID), PersistOverride: true,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: map[bool]string{false: "agent_pause", true: "agent_resume"}[value], Severity: "warning", Message: reason}})
}

func (e *Engine) agentLoad(ctx context.Context, accountID int64, value *int, actor, reason string) (accountcontrol.Result, error) {
	metadata, authorization, err := agentCommandMetadata(ctx, actor, fallbackReason(reason, "agent load action"))
	if err != nil {
		return accountcontrol.Result{}, err
	}
	input := controlplanebridge.AccountLoadFactorInput{AccountID: accountID, LoadFactor: cloneIntPointer(value), Context: metadata}
	var conversion controlplanebridge.ConversionResult
	if authorization != nil {
		conversion = controlplanebridge.AdaptAgentAdministratorLoadFactor(input, *authorization)
	} else {
		conversion = controlplanebridge.AdaptAutonomousAgentLoadFactor(input)
	}
	command := accountcontrol.CommandContextFrom(ctx)
	return e.submitConverted(ctx, conversion, accountcontrol.Submission{CommandID: metadata.StableSourceID,
		RequestIdempotencyKey: agentCommandRequestKey(metadata.StableSourceID), PersistOverride: true, OverrideKind: command.OverrideKind,
		Safety: accountcontrol.SafetyContext{TelemetryFresh: true}, Event: model.Event{Type: "agent_load_factor", Severity: "warning", Message: reason}})
}

func agentCommandMetadata(ctx context.Context, actor, reason string) (controlplanebridge.LegacyContext, *controlplanebridge.AdministratorAuthorization, error) {
	command := accountcontrol.CommandContextFrom(ctx)
	if command.CommandID == "" || command.CreatedAt.IsZero() {
		return controlplanebridge.LegacyContext{}, nil, errors.New("agent account action lacks a durable command identity")
	}
	expires := accountcontrol.DefaultAutonomousTTL
	namespace := controlplanebridge.SourceAgentAction
	var authorization *controlplanebridge.AdministratorAuthorization
	if command.Administrator {
		expires = accountcontrol.DefaultAdministratorTTL
		namespace = controlplanebridge.SourceAdministratorGrantConsumption
		value := controlplanebridge.AdministratorAuthorization{IdentityVerified: true, ExactGrant: true, GrantConsumed: true,
			GrantConsumptionID: command.GrantConsumptionID}
		if strings.TrimSpace(command.GrantConsumptionID) == "" {
			return controlplanebridge.LegacyContext{}, nil, errors.New("administrator agent action lacks consumed exact grant")
		}
		authorization = &value
	}
	expiresAt := command.ExpiresAt
	if expiresAt == nil {
		value := command.CreatedAt.Add(expires)
		expiresAt = &value
	}
	if !command.Administrator && expiresAt.Sub(command.CreatedAt) > accountcontrol.MaximumAutonomousTTL {
		return controlplanebridge.LegacyContext{}, nil, errors.New("autonomous agent override exceeds the two-hour maximum TTL")
	}
	metadata := controlplanebridge.LegacyContext{StableSourceNamespace: namespace, StableSourceID: command.CommandID,
		Actor: actor, Reason: reason, EvidenceRefs: append([]string(nil), command.EvidenceRefs...), SnapshotVersion: command.SnapshotVersion,
		CreatedAt: command.CreatedAt, ExpiresAt: expiresAt}
	return metadata, authorization, nil
}

func (e *Engine) submitConverted(ctx context.Context, conversion controlplanebridge.ConversionResult,
	submission accountcontrol.Submission) (accountcontrol.Result, error) {
	intent, err := conversion.MappedIntent()
	if err != nil {
		return accountcontrol.Result{}, err
	}
	submission.Intent = intent
	lookupKey := strings.TrimSpace(submission.RequestIdempotencyKey)
	if lookupKey == "" {
		lookupKey = submission.Intent.IdempotencyKey
	}
	if existing, err := e.accountControl.FindMutation(ctx, lookupKey); err != nil {
		return accountcontrol.Result{}, err
	} else if existing != nil {
		// A retry must reuse the lifecycle that was persisted by the original
		// command. Rebuilding with wall time would correctly look like a semantic
		// conflict because CreatedAt and ExpiresAt are part of the signature.
		copy := submission.Intent
		copy.CreatedAt = existing.CreatedAt
		copy.ExpiresAt = cloneTimePointer(existing.ExpiresAt)
		copy.ID = existing.IntentID
		if signature, signatureErr := controlplane.SemanticSignature(copy); signatureErr == nil && signature == existing.SemanticSignature {
			submission.Intent = copy
		}
	}
	return e.submitAccountMutation(ctx, submission)
}

func (e *Engine) submitAccountMutation(ctx context.Context, submission accountcontrol.Submission) (accountcontrol.Result, error) {
	defer e.notifyOverrideChanged()
	command := accountcontrol.CommandContextFrom(ctx)
	if command.AutomationLeaseHeld {
		return e.accountControl.Submit(ctx, submission)
	}
	release, err := e.barrier.EnterMutation(ctx)
	if err != nil {
		return accountcontrol.Result{}, fmt.Errorf("等待账号 mutation 冻结屏障: %w", err)
	}
	defer release()
	return e.accountControl.Submit(ctx, submission)
}

func (e *Engine) policySchedulableIntent(ctx context.Context, binding *model.ResolvedBinding, desired bool, reason string) (controlplane.Intent, accountcontrol.SafetyContext, error) {
	legacy, safety, err := e.policyContext(ctx, binding, controlplane.OperationSetAccountSchedulable, reason)
	if err != nil {
		return controlplane.Intent{}, safety, err
	}
	conversion := controlplanebridge.AdaptPolicyAccountSchedulable(controlplanebridge.AccountSchedulableInput{AccountID: binding.Account.ID, Schedulable: desired, Context: legacy})
	intent, err := conversion.MappedIntent()
	if err != nil {
		return controlplane.Intent{}, safety, err
	}
	return intent, safety, nil
}

func (e *Engine) policyLoadIntent(ctx context.Context, binding *model.ResolvedBinding, desired *int, reason string) (controlplane.Intent, accountcontrol.SafetyContext, error) {
	legacy, safety, err := e.policyContext(ctx, binding, controlplane.OperationSetAccountLoadFactor, reason)
	if err != nil {
		return controlplane.Intent{}, safety, err
	}
	conversion := controlplanebridge.AdaptPolicyAccountLoadFactor(controlplanebridge.AccountLoadFactorInput{AccountID: binding.Account.ID, LoadFactor: cloneIntPointer(desired), Context: legacy})
	intent, err := conversion.MappedIntent()
	if err != nil {
		return controlplane.Intent{}, safety, err
	}
	return intent, safety, nil
}

func (e *Engine) policyContext(ctx context.Context, binding *model.ResolvedBinding, operation controlplane.Operation, reason string) (controlplanebridge.LegacyContext, accountcontrol.SafetyContext, error) {
	policyVersion := stablePolicyVersion(binding.Policy)
	snapshotVersion, created, fresh := stableSnapshotVersion(binding)
	if snapshotVersion == "" || created.IsZero() {
		return controlplanebridge.LegacyContext{}, accountcontrol.SafetyContext{}, errors.New("policy action lacks an actual observation watermark")
	}
	arbitrationRevision, err := e.accountControl.ArbitrationRevision(ctx, binding.Account.ID, operation)
	if err != nil {
		return controlplanebridge.LegacyContext{}, accountcontrol.SafetyContext{}, fmt.Errorf("load account override revision: %w", err)
	}
	legacy := controlplanebridge.LegacyContext{StableSourceNamespace: controlplanebridge.SourcePolicyDecision,
		StableSourceID: policyVersion + ":" + snapshotVersion + ":" + stableDigest("override-revision-v1-", arbitrationRevision), Actor: "scheduler", Reason: reason,
		PolicyVersion: policyVersion, SnapshotVersion: snapshotVersion, CreatedAt: created}
	return legacy, accountcontrol.SafetyContext{TelemetryFresh: fresh, CooldownActive: binding.Control.FlapActive}, nil
}

func stablePolicyVersion(policy model.Policy) string {
	if policy.ScorePolicyVersionID != nil {
		return "score-policy:" + strconv.FormatInt(*policy.ScorePolicyVersionID, 10)
	}
	payload, _ := json.Marshal(policy)
	return stableDigest("policy-v1-", string(payload))
}

func stableSnapshotVersion(binding *model.ResolvedBinding) (string, time.Time, bool) {
	if binding == nil {
		return "", time.Time{}, false
	}
	created := binding.MonitorState.UpdatedAt
	if binding.Decision != nil && !binding.Decision.CheckedAt.IsZero() {
		created = binding.Decision.CheckedAt
	}
	if binding.Monitor != nil && binding.Monitor.LastCheckedAt != nil && binding.Monitor.LastCheckedAt.After(created) {
		created = binding.Monitor.LastCheckedAt.UTC()
	}
	if created.IsZero() {
		created = binding.Account.UpdatedAt
	}
	if created.IsZero() {
		return "", time.Time{}, false
	}
	payload, _ := json.Marshal(struct {
		MonitorState   model.MonitorState
		HealthState    model.MonitorHealthState
		Decision       *model.HealthDecision
		AccountUpdated time.Time
	}{
		binding.MonitorState, binding.HealthState, binding.Decision, binding.Account.UpdatedAt})
	fresh := true
	if binding.Monitor != nil && binding.Monitor.LastCheckedAt != nil {
		staleAfter := time.Duration(binding.Monitor.IntervalSeconds*3) * time.Second
		if staleAfter < 3*time.Minute {
			staleAfter = 3 * time.Minute
		}
		fresh = time.Since(binding.Monitor.LastCheckedAt.UTC()) <= staleAfter
	}
	return stableDigest("snapshot-v1-", string(payload)), created.UTC(), fresh
}

func administratorContext(commandID, actor, reason string, created time.Time, expires *time.Time) controlplanebridge.LegacyContext {
	return controlplanebridge.LegacyContext{StableSourceNamespace: controlplanebridge.SourceAdministratorRequest,
		StableSourceID: strings.TrimSpace(commandID), Actor: strings.TrimSpace(actor), Reason: strings.TrimSpace(reason), CreatedAt: created, ExpiresAt: expires}
}

func explicitAdministrator(actor string) error {
	actor = strings.ToLower(strings.TrimSpace(actor))
	if actor == "" || strings.HasPrefix(actor, "agent:") || actor == "administrator:agent" {
		return errors.New("account command requires an explicit direct administrator")
	}
	return nil
}

func stableDigest(prefix string, values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		hash.Write([]byte(strconv.Itoa(len(value))))
		hash.Write([]byte{':'})
		hash.Write([]byte(value))
		hash.Write([]byte{'|'})
	}
	return prefix + hex.EncodeToString(hash.Sum(nil))
}

func administratorCommandRequestKey(commandID string) string {
	return stableDigest("administrator-command-v1-", strings.TrimSpace(commandID))
}

func agentCommandRequestKey(commandID string) string {
	return stableDigest("agent-command-v1-", strings.TrimSpace(commandID))
}

func fallbackReason(value, fallback string) string {
	if value = strings.TrimSpace(value); value != "" {
		return value
	}
	return fallback
}

func findBinding(bindings []model.ResolvedBinding, accountID int64) (model.ResolvedBinding, bool) {
	for _, binding := range bindings {
		if binding.Account.ID == accountID {
			return binding, true
		}
	}
	return model.ResolvedBinding{}, false
}

func reasonForAdaptiveLoad(restore bool) string {
	if restore {
		return "账号健康恢复后还原原始负载"
	}
	return "账号负载已按渠道健康状态调整"
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

func legacyMutationResult(result accountcontrol.Result, err error) error {
	if err == nil {
		return nil
	}
	if result.Uncertain {
		return uncertainExternalMutation("account mutation", err)
	}
	return err
}
