package controlplaneshadow

import (
	"context"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
)

type ActionContext struct {
	StableSourceNamespace      controlplanebridge.StableSourceNamespace
	StableSourceID             string
	Reason                     string
	PolicyVersion              string
	SnapshotVersion            string
	EvidenceRefs               []string
	CreatedAt                  time.Time
	ExpiresAt                  *time.Time
	AdministratorAuthorization controlplanebridge.AdministratorAuthorization
}

type actionContextKey struct{}

func WithActionContext(ctx context.Context, value ActionContext) context.Context {
	return context.WithValue(ctx, actionContextKey{}, cloneActionContext(value))
}

func ActionContextFrom(ctx context.Context) ActionContext {
	if ctx == nil {
		return ActionContext{}
	}
	value, _ := ctx.Value(actionContextKey{}).(ActionContext)
	return cloneActionContext(value)
}

func cloneActionContext(value ActionContext) ActionContext {
	value.EvidenceRefs = append([]string(nil), value.EvidenceRefs...)
	if value.ExpiresAt != nil {
		expiresAt := *value.ExpiresAt
		value.ExpiresAt = &expiresAt
	}
	return value
}
