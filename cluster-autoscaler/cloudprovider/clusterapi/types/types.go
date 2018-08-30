package types

type CloudProviderManager interface {
	MachineSetWrapperForNode(nodeName string) (MachineSetWrapper, error)
	MachineSetWrappers() (nGroups []MachineSetWrapper)
	SetMachineSetSize(msw MachineSetWrapper, size int) error
	DeleteNodes(msw MachineSetWrapper, nodes []string) error
	MachineSetWrapperNodes(msw MachineSetWrapper) ([]string, error)
	Refresh() error
	Cleanup() error
}

const (
	MachineSetMinSize = "sigs.k8s.io/cluster-autoscaler-node-group-min-size"
	MachineSetMaxSize = "sigs.k8s.io/cluster-autoscaler-node-group-max-size"
)

type MachineSetWrapper interface {
	Id() string
	Name() string
	Namespace() string
	MinSize() int
	MaxSize() int
	Replicas() int
}
