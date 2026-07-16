package controlplane

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrInvalidOverride = errors.New("invalid control-plane override")

type OverrideRevocation struct {
	Actor     string
	Reason    string
	RevokedAt time.Time
}

type OverrideLease struct {
	intent     Intent
	revocation *OverrideRevocation
}

func NewTemporaryOverride(intent Intent) (OverrideLease, error) {
	if err := intent.Validate(); err != nil {
		return OverrideLease{}, fmt.Errorf("%w: %w", ErrInvalidOverride, err)
	}
	if intent.ExpiresAt == nil {
		return OverrideLease{}, fmt.Errorf("%w: temporary override requires expiration", ErrInvalidOverride)
	}
	switch intent.Authority {
	case AuthorityAdministratorCommand, AuthorityEmergencyAutomation, AuthorityAutonomousAgent, AuthorityOptimization:
		return OverrideLease{intent: cloneIntent(intent)}, nil
	default:
		return OverrideLease{}, fmt.Errorf("%w: authority %s is not a temporary override", ErrInvalidOverride, intent.Authority.String())
	}
}

func NewManualHold(intent Intent) (OverrideLease, error) {
	if err := intent.Validate(); err != nil {
		return OverrideLease{}, fmt.Errorf("%w: %w", ErrInvalidOverride, err)
	}
	if intent.Authority != AuthorityManualHold {
		return OverrideLease{}, fmt.Errorf("%w: manual hold authority is required", ErrInvalidOverride)
	}
	return OverrideLease{intent: cloneIntent(intent)}, nil
}

func (l OverrideLease) Intent() Intent { return cloneIntent(l.intent) }

func (l OverrideLease) Revocation() *OverrideRevocation {
	if l.revocation == nil {
		return nil
	}
	copy := *l.revocation
	return &copy
}

func (l OverrideLease) Revoke(at time.Time, actor, reason string) (OverrideLease, error) {
	if err := l.intent.Validate(); err != nil {
		return OverrideLease{}, fmt.Errorf("%w: %w", ErrInvalidOverride, err)
	}
	actor = strings.TrimSpace(actor)
	reason = strings.TrimSpace(reason)
	if actor == "" || reason == "" || at.IsZero() || at.Before(l.intent.CreatedAt) {
		return OverrideLease{}, fmt.Errorf("%w: revocation requires actor, reason, and a time at or after creation", ErrInvalidOverride)
	}
	if l.revocation != nil {
		return OverrideLease{}, fmt.Errorf("%w: override is already revoked", ErrInvalidOverride)
	}
	copy := OverrideLease{intent: cloneIntent(l.intent)}
	copy.revocation = &OverrideRevocation{Actor: actor, Reason: reason, RevokedAt: at}
	return copy, nil
}

func (l OverrideLease) Active(now time.Time) bool {
	if err := l.intent.Validate(); err != nil || l.intent.Expired(now) {
		return false
	}
	return l.revocation == nil || now.Before(l.revocation.RevokedAt)
}
