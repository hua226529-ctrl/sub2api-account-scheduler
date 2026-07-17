package accountcontrol

import (
	"context"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/controlplane"
	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

// executor is the only production transport adapter permitted to write account
// state in Sub2API. The service owns ordering, journaling, guard and readback.
type executor struct {
	transport Transport
}

func (e executor) write(ctx context.Context, intent controlplane.Intent) error {
	accountID, _ := intent.Resource.AccountID()
	switch intent.Operation {
	case controlplane.OperationSetAccountSchedulable:
		value, _ := intent.DesiredState.Schedulable()
		_, err := e.transport.SetSchedulable(ctx, accountID, value)
		return err
	case controlplane.OperationSetAccountLoadFactor:
		value, configured, _ := intent.DesiredState.LoadFactor()
		var desired *int
		if configured {
			desired = &value
		}
		_, err := e.transport.UpdateLoadFactor(ctx, accountID, desired)
		return err
	default:
		return errUnsupportedOperation
	}
}

func findAccount(accounts []model.Account, accountID int64) (model.Account, bool) {
	for _, account := range accounts {
		if account.ID == accountID {
			return account, true
		}
	}
	return model.Account{}, false
}
