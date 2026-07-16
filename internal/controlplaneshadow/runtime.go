package controlplaneshadow

import (
	"sync/atomic"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
)

type Adapter func() controlplanebridge.ConversionResult

type runtimeState struct {
	observerPanics atomic.Uint64
}

type Runtime struct {
	observer Observer
	state    *runtimeState
}

func NewRuntime(observer Observer) Runtime {
	if observer == nil {
		observer = NoopObserver{}
	}
	return Runtime{observer: observer, state: &runtimeState{}}
}

func (r Runtime) Enabled() (enabled bool) {
	if r.observer == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			r.recordObserverPanic()
			enabled = false
		}
	}()
	return r.observer.Enabled()
}

func (r Runtime) PanicCount() uint64 {
	if r.state == nil {
		return 0
	}
	return r.state.observerPanics.Load()
}

func (r Runtime) Observe(action Action, adapter Adapter) {
	if !r.Enabled() {
		return
	}
	observation := Observation{
		Path: action.Path, Producer: action.Producer,
		LegacyOperation: action.LegacyOperation, LegacyResource: action.LegacyResource,
		LegacyDesiredState: action.LegacyDesiredState, ObservedAt: action.ObservedAt,
		ValidationStatus: ValidationNotMapped, ArbiterStatus: ArbiterNotRun,
	}
	defer func() {
		if recover() != nil {
			observation.ConversionStatus = controlplanebridge.ConversionInvalid
			observation.ValidationStatus = ValidationInvalid
			observation.ArbiterStatus = ArbiterNotRun
			observation.Match = false
			observation.PanicRecovered = true
			observation.SafeDetailCode = DetailShadowPanic
			r.deliver(observation)
		}
	}()

	if !action.Path.Valid() {
		observation.ConversionStatus = controlplanebridge.ConversionInvalid
		observation.ValidationStatus = ValidationInvalid
		observation.SafeDetailCode = DetailInvalidPath
		r.deliver(observation)
		return
	}
	if adapter == nil {
		observation.ConversionStatus = controlplanebridge.ConversionInvalid
		observation.ValidationStatus = ValidationInvalid
		observation.SafeDetailCode = DetailConversionResultInvalid
		r.deliver(observation)
		return
	}

	result := adapter()
	observation.ConversionStatus = result.Status
	observation.GapCode = result.GapCode
	if result.Status != controlplanebridge.ConversionMapped {
		if result.Validate() != nil {
			observation.ConversionStatus = controlplanebridge.ConversionInvalid
			observation.ValidationStatus = ValidationInvalid
			observation.SafeDetailCode = DetailConversionResultInvalid
		}
		r.deliver(observation)
		return
	}
	if result.Intent == nil {
		observation.ConversionStatus = controlplanebridge.ConversionInvalid
		observation.ValidationStatus = ValidationInvalid
		observation.SafeDetailCode = DetailConversionResultInvalid
		r.deliver(observation)
		return
	}
	intent := *result.Intent
	observation.Authority = intent.Authority
	observation.IntentID = intent.ID
	observation.IdempotencyKey = intent.IdempotencyKey
	observation.MappedOperation = intent.Operation
	observation.MappedResource = intent.Resource.String()
	observation.MappedDesiredState = intentDesired(intent)
	if intent.Validate() != nil {
		observation.ConversionStatus = controlplanebridge.ConversionInvalid
		observation.ValidationStatus = ValidationInvalid
		observation.SafeDetailCode = DetailIntentValidationFailed
		r.deliver(observation)
		return
	}
	observation.ValidationStatus = ValidationValid
	results := controlplane.Arbitrate(action.ObservedAt, []controlplane.Intent{intent})
	if len(results) != 1 || results[0].Winner == nil {
		observation.ArbiterStatus = ArbiterNoWinner
		observation.SafeDetailCode = DetailArbiterNoWinner
		r.deliver(observation)
		return
	}
	winner := *results[0].Winner
	observation.ArbiterStatus = ArbiterSelected
	observation.MappedOperation = winner.Operation
	observation.MappedResource = winner.Resource.String()
	observation.MappedDesiredState = intentDesired(winner)
	observation.Match = observation.LegacyResource == observation.MappedResource &&
		observation.LegacyOperation == observation.MappedOperation &&
		observation.LegacyDesiredState == observation.MappedDesiredState
	r.deliver(observation)
}

func (r Runtime) deliver(observation Observation) {
	defer func() {
		if recover() != nil {
			r.recordObserverPanic()
		}
	}()
	r.observer.Observe(observation)
}

func (r Runtime) recordObserverPanic() {
	if r.state != nil {
		r.state.observerPanics.Add(1)
	}
}
