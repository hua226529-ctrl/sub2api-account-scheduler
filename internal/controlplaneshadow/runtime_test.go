package controlplaneshadow

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplanebridge"
)

func TestNoopRuntimeIsDisabledAndDoesNotConvert(t *testing.T) {
	runtime := NewRuntime(NoopObserver{})
	if runtime.Enabled() {
		t.Fatal("noop runtime is enabled")
	}
	var conversions atomic.Int64
	runtime.Observe(NewAccountSchedulableAction(PathManualPause, controlplane.ProducerAdminUI, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		conversions.Add(1)
		return mappedSchedulable(t, false)
	})
	if conversions.Load() != 0 || runtime.PanicCount() != 0 {
		t.Fatalf("disabled runtime performed work: conversions=%d panics=%d", conversions.Load(), runtime.PanicCount())
	}
}

func TestMappedIntentIsValidatedArbitratedAndCompared(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	action := NewAccountSchedulableAction(PathReconcilePolicyPause, controlplane.ProducerPolicyScheduler, 123, false, fixedShadowTime())
	runtime.Observe(action, func() controlplanebridge.ConversionResult { return mappedSchedulable(t, false) })

	observation := onlyObservation(t, capture.Observations())
	if observation.ConversionStatus != controlplanebridge.ConversionMapped || !observation.Match {
		t.Fatalf("mapped observation = %+v", observation)
	}
	if observation.ValidationStatus != ValidationValid || observation.ArbiterStatus != ArbiterSelected {
		t.Fatalf("validation/arbiter status = %s/%s", observation.ValidationStatus, observation.ArbiterStatus)
	}
	if observation.IntentID == "" || observation.IdempotencyKey == "" || observation.MappedResource != observation.LegacyResource ||
		observation.MappedOperation != observation.LegacyOperation || observation.MappedDesiredState != observation.LegacyDesiredState {
		t.Fatalf("mapped fields do not match legacy action: %+v", observation)
	}
}

func TestMismatchAndConversionFailureOnlyRecord(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	runtime.Observe(NewAccountSchedulableAction(PathReconcilePolicyResume, controlplane.ProducerPolicyScheduler, 123, true, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return mappedSchedulable(t, false)
	})
	runtime.Observe(NewAccountSchedulableAction(PathManualResume, controlplane.ProducerAdminUI, 123, true, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptManualResume(controlplanebridge.AccountActionInput{AccountID: 123})
	})

	observations := capture.Observations()
	if len(observations) != 2 || observations[0].Match || observations[1].Match {
		t.Fatalf("unexpected match results: %+v", observations)
	}
	if observations[1].ConversionStatus != controlplanebridge.ConversionIncomplete || observations[1].GapCode != controlplanebridge.GapAmbiguousManualResume || observations[1].IntentID != "" {
		t.Fatalf("manual resume gap = %+v", observations[1])
	}
}

func TestInvalidMappedIntentIsContained(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	invalid := mappedSchedulable(t, false)
	invalid.Intent.Actor = ""
	runtime.Observe(NewAccountSchedulableAction(PathManualPause, controlplane.ProducerAdminUI, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return invalid
	})
	observation := onlyObservation(t, capture.Observations())
	if observation.ConversionStatus != controlplanebridge.ConversionInvalid || observation.ValidationStatus != ValidationInvalid || observation.Match {
		t.Fatalf("invalid mapped intent escaped containment: %+v", observation)
	}
}

func TestInvalidLoadFactorConversionIsRecordedWithoutExecution(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	invalid := 101
	context := controlplanebridge.LegacyContext{
		StableSourceNamespace: controlplanebridge.SourceAdministratorRequest,
		StableSourceID:        "request:invalid-load",
		Actor:                 "administrator:web",
		Reason:                "invalid load test",
		CreatedAt:             fixedShadowTime(),
	}
	expiresAt := fixedShadowTime().Add(time.Hour)
	context.ExpiresAt = &expiresAt
	runtime.Observe(NewAccountLoadFactorAction(PathForceSetLoad, controlplane.ProducerAdminUI, 123, &invalid, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptTemporaryAdministratorLoadFactor(controlplanebridge.AccountLoadFactorInput{
			Context: context, AccountID: 123, LoadFactor: &invalid,
		})
	})
	observation := onlyObservation(t, capture.Observations())
	if observation.ConversionStatus != controlplanebridge.ConversionInvalid || observation.GapCode != controlplanebridge.GapInvalidDesiredState || observation.Match {
		t.Fatalf("invalid load observation = %+v", observation)
	}
}

func TestDefaultLoadFactorMatchesLegacyAction(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	action := NewAccountLoadFactorAction(PathReconcilePolicyLoad, controlplane.ProducerPolicyScheduler, 123, nil, fixedShadowTime())
	runtime.Observe(action, func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptPolicyAccountLoadFactor(controlplanebridge.AccountLoadFactorInput{
			Context: completePolicyContext(), AccountID: 123, LoadFactor: nil,
		})
	})
	observation := onlyObservation(t, capture.Observations())
	if !observation.Match || observation.LegacyDesiredState != "load_factor:default" || observation.MappedDesiredState != "load_factor:default" {
		t.Fatalf("default load observation = %+v", observation)
	}
}

func TestAdapterAndObserverPanicsAreIsolated(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	runtime.Observe(NewAccountSchedulableAction(PathAgentPause, controlplane.ProducerAgentOperator, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		panic("secret adapter panic")
	})
	observation := onlyObservation(t, capture.Observations())
	if !observation.PanicRecovered || observation.SafeDetailCode != DetailShadowPanic || observation.Match {
		t.Fatalf("adapter panic observation = %+v", observation)
	}

	panicking := NewRuntime(panicObserver{})
	panicking.Observe(NewAccountSchedulableAction(PathAgentPause, controlplane.ProducerAgentOperator, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return mappedSchedulable(t, false)
	})
	if panicking.PanicCount() != 1 {
		t.Fatalf("observer panic count = %d", panicking.PanicCount())
	}

	var businessPanic any
	func() {
		defer func() { businessPanic = recover() }()
		runtime.Observe(NewAccountSchedulableAction(PathAgentPause, controlplane.ProducerAgentOperator, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
			return mappedSchedulable(t, false)
		})
		panic("business panic")
	}()
	if businessPanic != "business panic" {
		t.Fatalf("shadow recover swallowed business panic: %v", businessPanic)
	}
}

func TestCaptureObserverCopiesAndSupportsConcurrentReads(t *testing.T) {
	capture := NewCaptureObserver()
	runtime := NewRuntime(capture)
	const count = 32
	var wait sync.WaitGroup
	for index := range count {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			runtime.Observe(NewAccountSchedulableAction(PathAgentPause, controlplane.ProducerAgentOperator, int64(index+1), false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
				return mappedSchedulableForAccount(t, int64(index+1), false)
			})
			_ = capture.Observations()
		}(index)
	}
	wait.Wait()
	observations := capture.Observations()
	if len(observations) != count {
		t.Fatalf("captured observations = %d, want %d", len(observations), count)
	}
	observations[0].LegacyResource = "modified"
	if capture.Observations()[0].LegacyResource == "modified" {
		t.Fatal("capture observer exposed mutable storage")
	}
	capture.Reset()
	if len(capture.Observations()) != 0 {
		t.Fatal("capture reset did not clear observations")
	}
}

func TestLoggingObserverProducesSafeSummary(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	observer := NewLoggingObserver(logger)
	runtime := NewRuntime(observer)
	context := completePolicyContext()
	context.StableSourceID = "test-token-secret"
	context.Reason = "administrator-key-secret raw-chat-secret"
	context.EvidenceRefs = []string{"model-key-secret", "full-evidence-secret"}
	runtime.Observe(NewAccountSchedulableAction(PathReconcilePolicyPause, controlplane.ProducerPolicyScheduler, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptPolicyAccountSchedulable(controlplanebridge.AccountSchedulableInput{Context: context, AccountID: 123, Schedulable: false})
	})
	expiresAt := fixedShadowTime().Add(time.Hour)
	grantID := "grant-consumption-secret"
	adminContext := controlplanebridge.LegacyContext{
		StableSourceNamespace: controlplanebridge.SourceAdministratorGrantConsumption,
		StableSourceID:        grantID,
		Actor:                 "administrator:agent",
		Reason:                "grant raw reason secret",
		CreatedAt:             fixedShadowTime(),
		ExpiresAt:             &expiresAt,
	}
	runtime.Observe(NewAccountSchedulableAction(PathForceResume, controlplane.ProducerAgentOperator, 123, true, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptAgentAdministratorAccountSchedulable(controlplanebridge.AccountSchedulableInput{
			Context: adminContext, AccountID: 123, Schedulable: true,
		}, controlplanebridge.AdministratorAuthorization{
			IdentityVerified: true, ExactGrant: true, GrantConsumed: true, GrantConsumptionID: grantID,
		})
	})

	logged := output.String()
	for _, secret := range []string{
		"administrator-key-secret", "raw-chat-secret", "model-key-secret", "full-evidence-secret",
		"grant raw reason secret", grantID, context.StableSourceID,
	} {
		if secret != "" && strings.Contains(logged, secret) {
			t.Fatalf("shadow log exposed %q: %s", secret, logged)
		}
	}
	summary := observer.Summary()
	if summary.Total != 2 || summary.Mapped != 2 || summary.Mismatches != 0 ||
		summary.PathCounts[PathReconcilePolicyPause] != 1 || summary.PathCounts[PathForceResume] != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestSummaryCountsStatusesGapsPathsAndPanics(t *testing.T) {
	observer := NewCaptureObserver()
	runtime := NewRuntime(observer)
	runtime.Observe(NewAccountSchedulableAction(PathManualPause, controlplane.ProducerAdminUI, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return mappedSchedulable(t, false)
	})
	runtime.Observe(NewAccountSchedulableAction(PathManualResume, controlplane.ProducerAdminUI, 123, true, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		return controlplanebridge.AdaptManualResume(controlplanebridge.AccountActionInput{AccountID: 123})
	})
	runtime.Observe(NewAccountSchedulableAction(PathAgentPause, controlplane.ProducerAgentOperator, 123, false, fixedShadowTime()), func() controlplanebridge.ConversionResult {
		panic("contained")
	})
	summary := Summarize(observer.Observations(), runtime.PanicCount())
	if summary.Total != 3 || summary.Mapped != 1 || summary.Incomplete != 1 || summary.Invalid != 1 || summary.ObserverPanics != 0 || summary.RecoveredPanics != 1 {
		t.Fatalf("summary status counts = %+v", summary)
	}
	if summary.GapCounts[controlplanebridge.GapAmbiguousManualResume] != 1 || summary.PathCounts[PathAgentPause] != 1 {
		t.Fatalf("summary gap/path counts = %+v", summary)
	}
}

func TestActionContextIsCopiedAndNamespaced(t *testing.T) {
	expiresAt := fixedShadowTime().Add(time.Hour)
	wantExpiresAt := expiresAt
	input := ActionContext{
		StableSourceNamespace: controlplanebridge.SourceAgentAction,
		StableSourceID:        "goal:1/step:2/action:3",
		CreatedAt:             fixedShadowTime(),
		ExpiresAt:             &expiresAt,
		EvidenceRefs:          []string{"packet:1", "monitor:2"},
	}
	ctx := WithActionContext(context.Background(), input)
	input.EvidenceRefs[0] = "modified"
	*input.ExpiresAt = input.ExpiresAt.Add(time.Hour)
	got := ActionContextFrom(ctx)
	if !reflect.DeepEqual(got.EvidenceRefs, []string{"packet:1", "monitor:2"}) || !got.ExpiresAt.Equal(wantExpiresAt) {
		t.Fatalf("action context was not copied: %+v", got)
	}
	got.EvidenceRefs[0] = "modified-again"
	if ActionContextFrom(ctx).EvidenceRefs[0] != "packet:1" {
		t.Fatal("action context getter exposed mutable evidence")
	}
}

func TestAllAccountPathsAreStableAndValid(t *testing.T) {
	paths := []Path{
		PathReconcilePolicyPause, PathReconcilePolicyResume, PathReconcilePolicyLoad,
		PathManualPause, PathManualResume, PathForceResume, PathForceSetLoad, PathPinLoad,
		PathAgentPause, PathAgentResume, PathAgentSetLoad,
	}
	seen := make(map[Path]bool, len(paths))
	for _, path := range paths {
		if !path.Valid() || path.String() == "" || seen[path] {
			t.Fatalf("invalid or duplicate path %q", path)
		}
		seen[path] = true
	}
	if Path("arbitrary").Valid() {
		t.Fatal("arbitrary path was accepted")
	}
}

func TestShadowPackageKeepsDependencyAndWorkerBoundaries(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	directory := filepath.Dir(sourceFile)
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		source := string(contents)
		for _, forbidden := range []string{
			"/internal/store", "/internal/sub2api", "/internal/httpserver", "/internal/agent",
			"/internal/balance", "/internal/failover",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s imports forbidden dependency %q", entry.Name(), forbidden)
			}
		}
		for _, worker := range []string{"go func(", "time.NewTicker(", "time.Tick("} {
			if strings.Contains(source, worker) {
				t.Fatalf("%s starts background work with %q", entry.Name(), worker)
			}
		}
	}

	projectRoot := filepath.Clean(filepath.Join(directory, "..", ".."))
	for _, relative := range []string{"internal/httpserver", "internal/sub2api"} {
		err := filepath.WalkDir(filepath.Join(projectRoot, relative), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
				return nil
			}
			contents, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(contents), "/internal/controlplaneshadow") {
				t.Fatalf("%s duplicates the Engine shadow observation boundary", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

type panicObserver struct{}

func (panicObserver) Enabled() bool       { return true }
func (panicObserver) Observe(Observation) { panic("observer secret") }

func mappedSchedulable(t *testing.T, value bool) controlplanebridge.ConversionResult {
	return mappedSchedulableForAccount(t, 123, value)
}

func mappedSchedulableForAccount(t *testing.T, accountID int64, value bool) controlplanebridge.ConversionResult {
	t.Helper()
	return controlplanebridge.AdaptPolicyAccountSchedulable(controlplanebridge.AccountSchedulableInput{
		Context: completePolicyContext(), AccountID: accountID, Schedulable: value,
	})
}

func completePolicyContext() controlplanebridge.LegacyContext {
	return controlplanebridge.LegacyContext{
		StableSourceNamespace: controlplanebridge.SourcePolicyDecision,
		StableSourceID:        "policy-decision:17/snapshot:41/action:1",
		Actor:                 "scheduler",
		Reason:                "fixed shadow test decision",
		PolicyVersion:         "policy:v17",
		SnapshotVersion:       "snapshot:v41",
		CreatedAt:             fixedShadowTime(),
	}
}

func fixedShadowTime() time.Time {
	return time.Date(2026, 7, 17, 3, 0, 0, 0, time.UTC)
}

func onlyObservation(t *testing.T, observations []Observation) Observation {
	t.Helper()
	if len(observations) != 1 {
		t.Fatalf("observations = %d, want 1: %+v", len(observations), observations)
	}
	return observations[0]
}
