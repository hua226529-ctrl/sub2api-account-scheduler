package controlplane

import (
	"strconv"
	"strings"

	"github.com/hua226529-ctrl/sub2api-account-scheduler/internal/model"
)

type Producer string

const (
	ProducerPolicyScheduler    Producer = "policy_scheduler"
	ProducerAgentOperator      Producer = "agent_operator"
	ProducerAdminUI            Producer = "admin_ui"
	ProducerFailoverController Producer = "failover_controller"
	ProducerCostOptimizer      Producer = "cost_optimizer"
)

func (p Producer) Valid() bool {
	switch p {
	case ProducerPolicyScheduler, ProducerAgentOperator, ProducerAdminUI,
		ProducerFailoverController, ProducerCostOptimizer:
		return true
	default:
		return false
	}
}

func (p Producer) String() string {
	if p.Valid() {
		return string(p)
	}
	return "unknown_producer(" + strconv.Quote(string(p)) + ")"
}

type Authority string

const (
	AuthorityManualHold           Authority = "manual_hold"
	AuthorityAdministratorCommand Authority = "administrator_command"
	AuthorityEmergencyAutomation  Authority = "emergency_automation"
	AuthorityAutonomousAgent      Authority = "autonomous_agent"
	AuthorityActivePolicy         Authority = "active_policy"
	AuthorityOptimization         Authority = "optimization"
)

// authorityRank is the single authority-ordering rule used by the package.
func authorityRank(authority Authority) (int, bool) {
	switch authority {
	case AuthorityManualHold:
		return 0, true
	case AuthorityAdministratorCommand:
		return 1, true
	case AuthorityEmergencyAutomation:
		return 2, true
	case AuthorityAutonomousAgent:
		return 3, true
	case AuthorityActivePolicy:
		return 4, true
	case AuthorityOptimization:
		return 5, true
	default:
		return 0, false
	}
}

func (a Authority) Valid() bool {
	_, ok := authorityRank(a)
	return ok
}

func (a Authority) String() string {
	if a.Valid() {
		return string(a)
	}
	return "unknown_authority(" + strconv.Quote(string(a)) + ")"
}

type ResourceKind string

const (
	ResourceAccount     ResourceKind = "account"
	ResourceUpstreamKey ResourceKind = "upstream_key"
)

type Resource struct {
	kind             ResourceKind
	accountID        int64
	upstreamSourceID int64
	upstreamKeyID    string
}

func NewAccountResource(accountID int64) (Resource, error) {
	resource := Resource{kind: ResourceAccount, accountID: accountID}
	if err := resource.Validate(); err != nil {
		return Resource{}, err
	}
	return resource, nil
}

func NewUpstreamKeyResource(sourceID int64, keyID string) (Resource, error) {
	resource := Resource{kind: ResourceUpstreamKey, upstreamSourceID: sourceID, upstreamKeyID: strings.TrimSpace(keyID)}
	if err := resource.Validate(); err != nil {
		return Resource{}, err
	}
	return resource, nil
}

func (r Resource) Validate() error {
	switch r.kind {
	case ResourceAccount:
		if r.accountID <= 0 || r.upstreamSourceID != 0 || r.upstreamKeyID != "" {
			return invalidIntent("resource", "account resource requires one positive account ID")
		}
	case ResourceUpstreamKey:
		if r.accountID != 0 || r.upstreamSourceID <= 0 || strings.TrimSpace(r.upstreamKeyID) == "" {
			return invalidIntent("resource", "upstream key resource requires source ID and key ID")
		}
	default:
		return invalidIntent("resource", "unknown resource kind")
	}
	return nil
}

func (r Resource) Kind() ResourceKind { return r.kind }

func (r Resource) AccountID() (int64, bool) {
	return r.accountID, r.kind == ResourceAccount
}

func (r Resource) UpstreamKey() (sourceID int64, keyID string, ok bool) {
	return r.upstreamSourceID, r.upstreamKeyID, r.kind == ResourceUpstreamKey
}

func (r Resource) String() string {
	switch r.kind {
	case ResourceAccount:
		return "account:" + strconv.FormatInt(r.accountID, 10)
	case ResourceUpstreamKey:
		return "upstream_key:" + strconv.FormatInt(r.upstreamSourceID, 10) + ":" + strconv.Quote(r.upstreamKeyID)
	default:
		return "unknown_resource(" + strconv.Quote(string(r.kind)) + ")"
	}
}

type Operation string

const (
	OperationSetAccountSchedulable   Operation = "set_account_schedulable"
	OperationSetAccountLoadFactor    Operation = "set_account_load_factor"
	OperationSetUpstreamKeyGroupTier Operation = "set_upstream_key_group_tier"
)

func (o Operation) Valid() bool {
	switch o {
	case OperationSetAccountSchedulable, OperationSetAccountLoadFactor, OperationSetUpstreamKeyGroupTier:
		return true
	default:
		return false
	}
}

func (o Operation) String() string {
	if o.Valid() {
		return string(o)
	}
	return "unknown_operation(" + strconv.Quote(string(o)) + ")"
}

type desiredStateKind uint8

const (
	desiredStateSchedulable desiredStateKind = iota + 1
	desiredStateLoadFactor
	desiredStateGroupTier
)

type DesiredState struct {
	kind          desiredStateKind
	schedulable   bool
	loadFactor    int
	loadFactorSet bool
	groupTier     string
}

func schedulableState(value bool) DesiredState {
	return DesiredState{kind: desiredStateSchedulable, schedulable: value}
}

func loadFactorState(value *int) (DesiredState, error) {
	state := DesiredState{kind: desiredStateLoadFactor}
	if value == nil {
		return state, nil
	}
	if *value < 1 || *value > 100 {
		return DesiredState{}, invalidIntent("desired_state.load_factor", "load factor must be nil or between 1 and 100")
	}
	state.loadFactor = *value
	state.loadFactorSet = true
	return state, nil
}

func groupTierState(tier string) (DesiredState, error) {
	tier = strings.ToLower(strings.TrimSpace(tier))
	switch tier {
	case model.GroupTierMain, model.GroupTierBackup, model.GroupTierEmergency:
		return DesiredState{kind: desiredStateGroupTier, groupTier: tier}, nil
	default:
		return DesiredState{}, invalidIntent("desired_state.group_tier", "group tier must use an existing model tier")
	}
}

func (s DesiredState) Schedulable() (bool, bool) {
	return s.schedulable, s.kind == desiredStateSchedulable
}

// LoadFactor returns configured=false for the valid "restore upstream default"
// state and ok=false when this is not a load-factor desired state.
func (s DesiredState) LoadFactor() (value int, configured bool, ok bool) {
	return s.loadFactor, s.loadFactorSet, s.kind == desiredStateLoadFactor
}

func (s DesiredState) GroupTier() (string, bool) {
	return s.groupTier, s.kind == desiredStateGroupTier
}

func (s DesiredState) validateFor(resource Resource, operation Operation) error {
	if err := resource.Validate(); err != nil {
		return err
	}
	switch operation {
	case OperationSetAccountSchedulable:
		if resource.kind != ResourceAccount || s.kind != desiredStateSchedulable {
			return invalidIntent("desired_state", "schedulable operation requires account and schedulable state")
		}
	case OperationSetAccountLoadFactor:
		if resource.kind != ResourceAccount || s.kind != desiredStateLoadFactor {
			return invalidIntent("desired_state", "load-factor operation requires account and load-factor state")
		}
		if s.loadFactorSet && (s.loadFactor < 1 || s.loadFactor > 100) {
			return invalidIntent("desired_state.load_factor", "load factor must be nil or between 1 and 100")
		}
	case OperationSetUpstreamKeyGroupTier:
		if resource.kind != ResourceUpstreamKey || s.kind != desiredStateGroupTier {
			return invalidIntent("desired_state", "group-tier operation requires upstream key and group-tier state")
		}
		if _, err := groupTierState(s.groupTier); err != nil {
			return err
		}
	default:
		return invalidIntent("operation", "unknown operation")
	}
	return nil
}

func (s DesiredState) canonical() string {
	switch s.kind {
	case desiredStateSchedulable:
		return "schedulable:" + strconv.FormatBool(s.schedulable)
	case desiredStateLoadFactor:
		if !s.loadFactorSet {
			return "load_factor:default"
		}
		return "load_factor:" + strconv.Itoa(s.loadFactor)
	case desiredStateGroupTier:
		return "group_tier:" + s.groupTier
	default:
		return "invalid_desired_state"
	}
}

type ConflictKey struct {
	Resource  Resource
	Operation Operation
}

func (k ConflictKey) String() string {
	return k.Resource.String() + "#" + k.Operation.String()
}
