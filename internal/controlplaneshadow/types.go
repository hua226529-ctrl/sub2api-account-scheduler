package controlplaneshadow

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
)

type Path string

const (
	PathReconcilePolicyPause  Path = "reconcile_policy_pause"
	PathReconcilePolicyResume Path = "reconcile_policy_resume"
	PathReconcilePolicyLoad   Path = "reconcile_policy_load"
	PathManualPause           Path = "manual_pause"
	PathManualResume          Path = "manual_resume"
	PathForceResume           Path = "force_resume"
	PathForceSetLoad          Path = "force_set_load"
	PathPinLoad               Path = "pin_load"
	PathAgentPause            Path = "agent_pause"
	PathAgentResume           Path = "agent_resume"
	PathAgentSetLoad          Path = "agent_set_load"
)

func (p Path) Valid() bool {
	switch p {
	case PathReconcilePolicyPause, PathReconcilePolicyResume, PathReconcilePolicyLoad,
		PathManualPause, PathManualResume, PathForceResume, PathForceSetLoad, PathPinLoad,
		PathAgentPause, PathAgentResume, PathAgentSetLoad:
		return true
	default:
		return false
	}
}

func (p Path) String() string {
	if p.Valid() {
		return string(p)
	}
	return "unknown_shadow_path(" + strconv.Quote(string(p)) + ")"
}

type ValidationStatus string

const (
	ValidationNotMapped ValidationStatus = "not_mapped"
	ValidationValid     ValidationStatus = "valid"
	ValidationInvalid   ValidationStatus = "invalid"
)

type ArbiterStatus string

const (
	ArbiterNotRun   ArbiterStatus = "not_run"
	ArbiterSelected ArbiterStatus = "selected"
	ArbiterNoWinner ArbiterStatus = "no_winner"
)

const (
	DetailInvalidPath             = "invalid_path"
	DetailConversionResultInvalid = "conversion_result_invalid"
	DetailIntentValidationFailed  = "intent_validation_failed"
	DetailArbiterNoWinner         = "arbiter_no_winner"
	DetailShadowPanic             = "shadow_panic"
)

type Action struct {
	Path               Path
	Producer           controlplane.Producer
	LegacyOperation    controlplane.Operation
	LegacyResource     string
	LegacyDesiredState string
	ObservedAt         time.Time
}

func NewAccountSchedulableAction(path Path, producer controlplane.Producer, accountID int64, value bool, observedAt time.Time) Action {
	return Action{
		Path: path, Producer: producer, LegacyOperation: controlplane.OperationSetAccountSchedulable,
		LegacyResource: accountResource(accountID), LegacyDesiredState: schedulableDesired(value), ObservedAt: observedAt,
	}
}

func NewAccountLoadFactorAction(path Path, producer controlplane.Producer, accountID int64, value *int, observedAt time.Time) Action {
	return Action{
		Path: path, Producer: producer, LegacyOperation: controlplane.OperationSetAccountLoadFactor,
		LegacyResource: accountResource(accountID), LegacyDesiredState: loadFactorDesired(value), ObservedAt: observedAt,
	}
}

type Observation struct {
	Path               Path
	Producer           controlplane.Producer
	Authority          controlplane.Authority
	LegacyOperation    controlplane.Operation
	LegacyResource     string
	LegacyDesiredState string
	ConversionStatus   controlplanebridge.ConversionStatus
	GapCode            controlplanebridge.GapCode
	IntentID           string
	IdempotencyKey     string
	MappedOperation    controlplane.Operation
	MappedResource     string
	MappedDesiredState string
	ValidationStatus   ValidationStatus
	ArbiterStatus      ArbiterStatus
	Match              bool
	ObservedAt         time.Time
	PanicRecovered     bool
	SafeDetailCode     string
}

type Summary struct {
	Total           int
	Mapped          int
	Incomplete      int
	Unsupported     int
	Invalid         int
	Mismatches      int
	ObserverPanics  uint64
	RecoveredPanics int
	GapCounts       map[controlplanebridge.GapCode]int
	PathCounts      map[Path]int
}

func Summarize(observations []Observation, observerPanics uint64) Summary {
	summary := newSummary()
	summary.ObserverPanics = observerPanics
	for _, observation := range observations {
		addObservation(&summary, observation)
	}
	return summary
}

func newSummary() Summary {
	return Summary{GapCounts: make(map[controlplanebridge.GapCode]int), PathCounts: make(map[Path]int)}
}

func addObservation(summary *Summary, observation Observation) {
	summary.Total++
	switch observation.ConversionStatus {
	case controlplanebridge.ConversionMapped:
		summary.Mapped++
	case controlplanebridge.ConversionIncomplete:
		summary.Incomplete++
	case controlplanebridge.ConversionUnsupported:
		summary.Unsupported++
	case controlplanebridge.ConversionInvalid:
		summary.Invalid++
	}
	if observation.ConversionStatus == controlplanebridge.ConversionMapped && !observation.Match {
		summary.Mismatches++
	}
	if observation.PanicRecovered {
		summary.RecoveredPanics++
	}
	if observation.GapCode != controlplanebridge.GapNone {
		summary.GapCounts[observation.GapCode]++
	}
	summary.PathCounts[observation.Path]++
}

func cloneSummary(summary Summary) Summary {
	copy := summary
	copy.GapCounts = make(map[controlplanebridge.GapCode]int, len(summary.GapCounts))
	for gap, count := range summary.GapCounts {
		copy.GapCounts[gap] = count
	}
	copy.PathCounts = make(map[Path]int, len(summary.PathCounts))
	for path, count := range summary.PathCounts {
		copy.PathCounts[path] = count
	}
	return copy
}

func accountResource(accountID int64) string {
	return "account:" + strconv.FormatInt(accountID, 10)
}

func schedulableDesired(value bool) string {
	return "schedulable:" + strconv.FormatBool(value)
}

func loadFactorDesired(value *int) string {
	if value == nil {
		return "load_factor:default"
	}
	return "load_factor:" + strconv.Itoa(*value)
}

func intentDesired(intent controlplane.Intent) string {
	if value, ok := intent.DesiredState.Schedulable(); ok {
		return schedulableDesired(value)
	}
	if value, configured, ok := intent.DesiredState.LoadFactor(); ok {
		if !configured {
			return loadFactorDesired(nil)
		}
		return loadFactorDesired(&value)
	}
	if tier, ok := intent.DesiredState.GroupTier(); ok {
		return "group_tier:" + tier
	}
	return fmt.Sprintf("unknown_desired_state:%s", intent.Operation.String())
}
