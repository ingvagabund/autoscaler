package nodegroup

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/types"
	schedulercache "k8s.io/kubernetes/pkg/scheduler/cache"
)

func NewNodeGroup(manager types.CloudProviderManager, machineSetWrapper types.MachineSetWrapper, minSize, maxSize, curSize int) *NodeGroup {
	return &NodeGroup{
		manager:           manager,
		machineSetWrapper: machineSetWrapper,
		minSize:           minSize,
		maxSize:           maxSize,
		curSize:           curSize,
	}
}

// NodeGroup is implementation of the NodeGroup interface
type NodeGroup struct {
	manager           types.CloudProviderManager
	machineSetWrapper types.MachineSetWrapper

	minSize int
	maxSize int
	curSize int
}

// MaxSize returns maximum size of the node group.
func (ng *NodeGroup) MaxSize() int {
	return ng.maxSize
}

// MinSize returns minimum size of the node group.
func (ng *NodeGroup) MinSize() int {
	return ng.minSize
}

// TargetSize returns the current target size of the node group. It is possible that the
// number of nodes in Kubernetes is different at the moment but should be equal
// to Size() once everything stabilizes (new nodes finish startup and registration or
// removed nodes are deleted completely). Implementation required.
func (ng *NodeGroup) TargetSize() (int, error) {
	return ng.curSize, nil
}

// IncreaseSize increases the size of the node group. To delete a node you need
// to explicitly name it and use DeleteNode. This function should wait until
// node group size is updated. Implementation required.
func (ng *NodeGroup) IncreaseSize(delta int) error {
	if delta <= 0 {
		return fmt.Errorf("size increase must be positive")
	}
	size := ng.curSize
	if size+delta > ng.maxSize {
		return fmt.Errorf("size increase too large - desired:%d max:%d", size+delta, ng.maxSize)
	}
	return ng.manager.SetMachineSetSize(ng.machineSetWrapper, size+delta)
}

// DeleteNodes deletes nodes from this node group. Error is returned either on
// failure or if the given node doesn't belong to this node group. This function
// should wait until node group size is updated. Implementation required.
func (ng *NodeGroup) DeleteNodes(nodes []*apiv1.Node) error {
	size := ng.curSize
	if int(size) <= ng.MinSize() {
		return fmt.Errorf("min size reached, nodes will not be deleted")
	}

	var nodeNames []string
	for _, node := range nodes {
		msw, err := ng.manager.MachineSetWrapperForNode(node.Name)
		if err != nil {
			return err
		}
		if msw.Id() != ng.machineSetWrapper.Id() {
			return fmt.Errorf("%s belongs to a different asg than %s", node.Name, msw.Id())
		}
		nodeNames = append(nodeNames, node.Name)
	}

	return ng.manager.DeleteNodes(ng.machineSetWrapper, nodeNames)
}

// DecreaseTargetSize decreases the target size of the node group. This function
// doesn't permit to delete any existing node and can be used only to reduce the
// request for new nodes that have not been yet fulfilled. Delta should be negative.
// It is assumed that cloud provider will not delete the existing nodes when there
// is an option to just decrease the target. Implementation required.
func (ng *NodeGroup) DecreaseTargetSize(delta int) error {
	if delta >= 0 {
		return fmt.Errorf("size decrease size must be negative")
	}

	size := ng.curSize
	nodes, err := ng.manager.MachineSetWrapperNodes(ng.machineSetWrapper)
	if err != nil {
		return err
	}
	if int(size)+delta < len(nodes) {
		return fmt.Errorf("attempt to delete existing nodes targetSize:%d delta:%d existingNodes: %d",
			size, delta, len(nodes))
	}

	return ng.manager.SetMachineSetSize(ng.machineSetWrapper, size+delta)
}

// Id returns an unique identifier of the node group.
func (ng *NodeGroup) Id() string {
	return ng.machineSetWrapper.Id()
}

// Debug returns a string containing all information regarding this node group.
func (ng *NodeGroup) Debug() string {
	return fmt.Sprintf("%s (%d:%d)", ng.Id(), ng.MinSize(), ng.MaxSize())
}

// Nodes returns a list of all nodes that belong to this node group.
func (ng *NodeGroup) Nodes() ([]string, error) {
	return ng.manager.MachineSetWrapperNodes(ng.machineSetWrapper)
}

// TemplateNodeInfo returns a schedulercache.NodeInfo structure of an empty
// (as if just started) node. This will be used in scale-up simulations to
// predict what would a new node look like if a node group was expanded. The returned
// NodeInfo is expected to have a fully populated Node object, with all of the labels,
// capacity and allocatable information as well as all pods that are started on
// the node by default, using manifest (most likely only kube-proxy). Implementation optional.
func (ng *NodeGroup) TemplateNodeInfo() (*schedulercache.NodeInfo, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Exist checks if the node group really exists on the cloud provider side. Allows to tell the
// theoretical node group from the real one. Implementation required.
func (ng *NodeGroup) Exist() bool {
	return true
}

// Create creates the node group on the cloud provider side. Implementation optional.
func (ng *NodeGroup) Create() (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// Delete deletes the node group on the cloud provider side.
// This will be executed only for autoprovisioned node groups, once their size drops to 0.
// Implementation optional.
func (ng *NodeGroup) Delete() error {
	return cloudprovider.ErrNotImplemented
}

// Autoprovisioned returns true if the node group is autoprovisioned. An autoprovisioned group
// was created by CA and can be deleted when scaled to 0.
func (ng *NodeGroup) Autoprovisioned() bool {
	return false
}

var _ cloudprovider.NodeGroup = &NodeGroup{}
