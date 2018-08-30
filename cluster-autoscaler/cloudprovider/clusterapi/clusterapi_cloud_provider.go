package clusterapi

import (
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/internal"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/nodegroup"
	"k8s.io/autoscaler/cluster-autoscaler/utils/errors"

	apiv1 "k8s.io/api/core/v1"

	"io"
)

const (
	// ProviderName is the cloud provider name for kubemark
	ProviderName = "clusterapi"
)

func BuildCloudProvider(cloudProviderManager *internal.Manager, resourceLimiter *cloudprovider.ResourceLimiter) (*CloudProvider, error) {
	return &CloudProvider{
		manager:         cloudProviderManager,
		resourceLimiter: resourceLimiter,
	}, nil
}

type CloudProvider struct {
	manager         *internal.Manager
	resourceLimiter *cloudprovider.ResourceLimiter
}

// Name returns name of the cloud provider.
func (cp *CloudProvider) Name() string {
	return ProviderName
}

// NodeGroups returns all node groups configured for this cloud provider.
func (cp *CloudProvider) NodeGroups() []cloudprovider.NodeGroup {
	mswItems := cp.manager.MachineSetWrappers()
	var nodeGroups []cloudprovider.NodeGroup
	for _, msw := range mswItems {
		nodeGroups = append(nodeGroups, nodegroup.NewNodeGroup(cp.manager, msw, msw.MinSize(), msw.MaxSize(), msw.Replicas()))
	}
	return nodeGroups
}

// NodeGroupForNode returns the node group for the given node, nil if the node
// should not be processed by cluster autoscaler, or non-nil error if such
// occurred. Must be implemented.
func (cp *CloudProvider) NodeGroupForNode(node *apiv1.Node) (cloudprovider.NodeGroup, error) {
	msw, err := cp.manager.MachineSetWrapperForNode(node.Name)
	if err != nil {
		return nil, err
	}
	return nodegroup.NewNodeGroup(cp.manager, msw, msw.MinSize(), msw.MaxSize(), msw.Replicas()), nil
}

// Pricing returns pricing model for this cloud provider or error if not available.
// Implementation optional.
func (cp *CloudProvider) Pricing() (cloudprovider.PricingModel, errors.AutoscalerError) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetAvailableMachineTypes get all machine types that can be requested from the cloud provider.
// Implementation optional.
func (cp *CloudProvider) GetAvailableMachineTypes() ([]string, error) {
	return []string{}, nil
}

// NewNodeGroup builds a theoretical node group based on the node definition provided. The node group is not automatically
// created on the cloud provider side. The node group is not returned by NodeGroups() until it is created.
// Implementation optional.
func (cp *CloudProvider) NewNodeGroup(machineType string, labels map[string]string, systemLabels map[string]string,
	taints []apiv1.Taint, extraResources map[string]resource.Quantity) (cloudprovider.NodeGroup, error) {
	return nil, cloudprovider.ErrNotImplemented
}

// GetResourceLimiter returns struct containing limits (max, min) for resources (cores, memory etc.).
func (cp *CloudProvider) GetResourceLimiter() (*cloudprovider.ResourceLimiter, error) {
	return cp.resourceLimiter, nil
}

// Cleanup cleans up open resources before the cloud provider is destroyed, i.e. go routines etc.
func (cp *CloudProvider) Cleanup() error {
	return cp.manager.Cleanup()
}

// Refresh is called before every main loop and can be used to dynamically update cloud provider state.
// In particular the list of node groups returned by NodeGroups can change as a result of CloudProvider.Refresh().
func (cp *CloudProvider) Refresh() error {
	return cp.manager.Refresh()
}

var _ cloudprovider.CloudProvider = &CloudProvider{}

// CreateAwsManager constructs awsManager object.
func CreateManager(configReader io.Reader, discoveryOpts cloudprovider.NodeGroupDiscoveryOptions) (*internal.Manager, error) {
	return internal.CreateManagerInternal(configReader, discoveryOpts)
}
