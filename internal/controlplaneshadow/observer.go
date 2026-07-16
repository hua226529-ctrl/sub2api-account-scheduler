package controlplaneshadow

import (
	"log/slog"
	"sync"
)

type Observer interface {
	Enabled() bool
	Observe(Observation)
}

type NoopObserver struct{}

func (NoopObserver) Enabled() bool       { return false }
func (NoopObserver) Observe(Observation) {}

type CaptureObserver struct {
	mu           sync.RWMutex
	observations []Observation
}

func NewCaptureObserver() *CaptureObserver { return &CaptureObserver{} }

func (*CaptureObserver) Enabled() bool { return true }

func (o *CaptureObserver) Observe(observation Observation) {
	o.mu.Lock()
	o.observations = append(o.observations, observation)
	o.mu.Unlock()
}

func (o *CaptureObserver) Observations() []Observation {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return append([]Observation(nil), o.observations...)
}

func (o *CaptureObserver) Reset() {
	o.mu.Lock()
	o.observations = nil
	o.mu.Unlock()
}

func (o *CaptureObserver) Summary(observerPanics uint64) Summary {
	return Summarize(o.Observations(), observerPanics)
}

type LoggingObserver struct {
	logger  *slog.Logger
	mu      sync.Mutex
	summary Summary
}

func NewLoggingObserver(logger *slog.Logger) *LoggingObserver {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingObserver{logger: logger, summary: newSummary()}
}

func (*LoggingObserver) Enabled() bool { return true }

func (o *LoggingObserver) Observe(observation Observation) {
	o.mu.Lock()
	addObservation(&o.summary, observation)
	summary := cloneSummary(o.summary)
	o.mu.Unlock()
	o.logger.Info("controlplane_shadow",
		"path", observation.Path.String(),
		"producer", observation.Producer.String(),
		"authority", observation.Authority.String(),
		"operation", observation.LegacyOperation.String(),
		"resource", observation.LegacyResource,
		"conversion_status", observation.ConversionStatus.String(),
		"gap_code", observation.GapCode.String(),
		"match", observation.Match,
		"validation_status", string(observation.ValidationStatus),
		"arbiter_status", string(observation.ArbiterStatus),
		"panic_recovered", observation.PanicRecovered,
		"safe_detail_code", observation.SafeDetailCode,
		"total", summary.Total,
		"mapped", summary.Mapped,
		"incomplete", summary.Incomplete,
		"unsupported", summary.Unsupported,
		"invalid", summary.Invalid,
		"mismatches", summary.Mismatches,
		"path_count", summary.PathCounts[observation.Path],
		"gap_count", summary.GapCounts[observation.GapCode])
}

func (o *LoggingObserver) Summary() Summary {
	o.mu.Lock()
	defer o.mu.Unlock()
	return cloneSummary(o.summary)
}
