package reconcile

import (
	"testing"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

func TestFilterReconcileAccountIDsUsesCurrentBoundSnapshot(t *testing.T) {
	monitor := model.Monitor{ID: 10}
	engine := &Engine{snapshot: model.Snapshot{Bindings: []model.ResolvedBinding{
		{Account: model.Account{ID: 225}, Monitor: &monitor, State: "bound"},
		{Account: model.Account{ID: 295}, State: "unmatched"},
		{Account: model.Account{ID: 296}, Monitor: &monitor, State: "excluded"},
		{Account: model.Account{ID: 297, Type: "oauth"}, Monitor: &monitor, State: "bound"},
	}}}
	accepted, ignored := engine.FilterReconcileAccountIDs(297, 296, 225, 295, 225)
	if len(accepted) != 1 || accepted[0] != 225 {
		t.Fatalf("accepted=%v", accepted)
	}
	if len(ignored) != 3 || ignored[0] != 295 || ignored[1] != 296 || ignored[2] != 297 {
		t.Fatalf("ignored=%v", ignored)
	}
}
