package accountcontrol

import (
	"context"
	"time"
)

type CommandContext struct {
	CommandID           string
	CreatedAt           time.Time
	ExpiresAt           *time.Time
	SnapshotVersion     string
	EvidenceRefs        []string
	OverrideKind        OverrideKind
	Administrator       bool
	GrantConsumptionID  string
	RunID               int64
	GoalID              int64
	StepID              int64
	AutomationLeaseHeld bool
}

type commandContextKey struct{}

func WithCommandContext(ctx context.Context, value CommandContext) context.Context {
	value.EvidenceRefs = append([]string(nil), value.EvidenceRefs...)
	value.ExpiresAt = cloneTime(value.ExpiresAt)
	return context.WithValue(ctx, commandContextKey{}, value)
}

func CommandContextFrom(ctx context.Context) CommandContext {
	value, _ := ctx.Value(commandContextKey{}).(CommandContext)
	value.EvidenceRefs = append([]string(nil), value.EvidenceRefs...)
	value.ExpiresAt = cloneTime(value.ExpiresAt)
	return value
}
