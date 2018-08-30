package internal

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/golang/glog"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider/clusterapi/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	v1alpha1apis "sigs.k8s.io/cluster-api/pkg/apis/cluster/v1alpha1"
	"sigs.k8s.io/cluster-api/pkg/client/clientset_generated/clientset"
	"sigs.k8s.io/cluster-api/pkg/client/clientset_generated/clientset/typed/cluster/v1alpha1"
)

type Manager struct {
	ClusterClient v1alpha1.ClusterV1alpha1Interface
	KubeClient    *kubernetes.Clientset

	machineSetsCacheMux    sync.Mutex
	machineSetsCache       map[string]*MachineSetWrapperImpl
	nodesToMachineSetCache map[string]string
	machineSetToNodes      map[string][]string
}

type MachineSetWrapperImpl struct {
	// Keep the machineSet for its name and labels at least
	machineSet *v1alpha1apis.MachineSet
	minSize    int
	maxSize    int
	replicas   int
}

func NewMachineSetWrapper(machineSet *v1alpha1apis.MachineSet) (*MachineSetWrapperImpl, error) {
	var minSize int
	var maxSize int
	if size, exists := machineSet.Labels[types.MachineSetMinSize]; exists {
		u, err := strconv.ParseUint(size, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse %q label of machineset %q: %v", types.MachineSetMinSize, machineSet.Name, err)
		}
		minSize = int(u)
	} else {
		return nil, fmt.Errorf("Can't fine %q label of machineset %q", types.MachineSetMinSize, machineSet.Name)
	}
	if size, exists := machineSet.Labels[types.MachineSetMaxSize]; exists {
		u, err := strconv.ParseUint(size, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Unable to parse %q label of machineset %q: %v", types.MachineSetMaxSize, machineSet.Name, err)
		}
		maxSize = int(u)
	} else {
		return nil, fmt.Errorf("Can't fine %q label of machineset %q", types.MachineSetMaxSize, machineSet.Name)
	}

	return &MachineSetWrapperImpl{
		machineSet: machineSet,
		minSize:    minSize,
		maxSize:    maxSize,
		replicas:   int(machineSet.Status.Replicas),
	}, nil
}

func (ms *MachineSetWrapperImpl) Id() string {
	return fmt.Sprintf("%v/%v", ms.machineSet.Namespace, ms.machineSet.Name)
}

func (ms *MachineSetWrapperImpl) Name() string {
	return ms.machineSet.Name
}

func (ms *MachineSetWrapperImpl) Namespace() string {
	return ms.machineSet.Namespace
}

func (ms *MachineSetWrapperImpl) MinSize() int {
	return ms.minSize
}

func (ms *MachineSetWrapperImpl) MaxSize() int {
	return ms.maxSize
}

func (ms *MachineSetWrapperImpl) Replicas() int {
	return ms.replicas
}

var _ types.MachineSetWrapper = &MachineSetWrapperImpl{}

func CreateManagerInternal(configReader io.Reader, discoveryOpts cloudprovider.NodeGroupDiscoveryOptions) (*Manager, error) {
	kubeconfig, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		kubeconfigPath := os.Getenv("KUBECONFIG")
		kubeconfig, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, err
		}
	}

	capiclient, err := clientset.NewForConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("could not create client for talking to the apiserver: %v", err)
	}

	kubeClient, err := kubernetes.NewForConfig(kubeconfig)
	if err != nil {
		glog.Fatalf("could not create kubernetes client to talk to the apiserver: %v", err)
	}

	cm := &Manager{
		ClusterClient:          capiclient.ClusterV1alpha1(),
		KubeClient:             kubeClient,
		machineSetsCache:       make(map[string]*MachineSetWrapperImpl, 0),
		nodesToMachineSetCache: make(map[string]string, 0),
		machineSetToNodes:      make(map[string][]string, 0),
	}

	return cm, nil
}

func (m *Manager) MachineSetWrapperForNode(nodeName string) (types.MachineSetWrapper, error) {
	m.machineSetsCacheMux.Lock()
	defer m.machineSetsCacheMux.Unlock()

	msID, exists := m.nodesToMachineSetCache[nodeName]
	if !exists {
		return nil, fmt.Errorf("Unable to find a node group for the node %q", nodeName)
	}

	return m.machineSetsCache[msID], nil
}

func (m *Manager) MachineSetWrappers() (nGroups []types.MachineSetWrapper) {
	m.machineSetsCacheMux.Lock()
	defer m.machineSetsCacheMux.Unlock()
	for _, item := range m.machineSetsCache {
		nGroups = append(nGroups, item)
	}
	return
}

func (m *Manager) Refresh() error {
	// List all known machine sets and cache the node -> machineSet

	// TODO(jchaloup): based on the configuration set the label selector
	// TODO(jchaloup): if node group discovery set, use labels to search for all relevant machinesets
	// TODO(jchaloup): if any node group specified explicitely, add them to the search
	// for now, just take all machinesets
	machineSets, err := m.ClusterClient.MachineSets("default").List(v1.ListOptions{})
	if err != nil {
		return fmt.Errorf("unable to list machinesets in the default namespace: %v", err)
	}

	localMachineSetCache := make(map[string]*MachineSetWrapperImpl, 0)
	localNodesToMachineSetCache := make(map[string]string, 0)
	localMachineSetToNodes := make(map[string][]string, 0)

	for _, item := range machineSets.Items {
		fmt.Printf("machineSet: %#v\n", item)
		msw, err := NewMachineSetWrapper(&item)
		if err != nil {
			glog.Errorf("unable to convert %q machineset: %v", msw.Name(), err)
		}
		localMachineSetCache[fmt.Sprintf("%v/%v", item.Namespace, item.Name)] = msw

		if len(item.Spec.Selector.MatchLabels) == 0 {
			// TODO(jchaloup): process the Selector.MatchExpressions as well
			glog.Warning("machineset %q has no label selector", item.Name)
			continue
		}
		// Get machine of the machineset
		machines, err := m.ClusterClient.Machines("default").List(v1.ListOptions{
			LabelSelector: labels.SelectorFromSet(item.Spec.Selector.MatchLabels).String(),
		})
		if err != nil {
			glog.Error("unable to list machines of machineset %q: %v", item.Name, err)
			continue
		}

		msId := fmt.Sprintf("%v/%v", item.Namespace, item.Name)

		localMachineSetToNodes[msId] = []string{}

		for _, machine := range machines.Items {
			fmt.Printf("machine: %#v\n", machine)
			if machine.Status.NodeRef == nil {
				glog.Error("Status.NodeRef of machine %q is nil")
			}
			if machine.Status.NodeRef.Kind != "Node" {
				glog.Error("Status.NodeRef of machine %q does not reference a node (rather %q)", msId, machine.Status.NodeRef.Kind)
			}
			localNodesToMachineSetCache[machine.Status.NodeRef.Name] = msId
			localMachineSetToNodes[msId] = append(localMachineSetToNodes[msId], machine.Status.NodeRef.Name)
		}
	}

	m.machineSetsCacheMux.Lock()
	defer m.machineSetsCacheMux.Unlock()
	m.machineSetsCache = localMachineSetCache
	m.nodesToMachineSetCache = localNodesToMachineSetCache
	m.machineSetToNodes = localMachineSetToNodes

	return nil
}

func (m *Manager) Cleanup() error {
	// Nothing to clean up for the moment
	// Maybe later run a go routine that's actually going to refresh?
	return nil
}

func (m *Manager) SetMachineSetSize(msw types.MachineSetWrapper, size int) error {
	ms, err := m.ClusterClient.MachineSets(msw.Namespace()).Get(msw.Name(), v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Unable to get machineset %q: %v", msw.Id(), err)
	}
	newMachineSet := ms.DeepCopy()
	replicas := int32(size)
	newMachineSet.Spec.Replicas = &replicas

	_, err = m.ClusterClient.MachineSets(msw.Namespace()).Update(newMachineSet)
	if err != nil {
		return fmt.Errorf("Unable to update number of replicas of machineset %q: %v", msw.Id(), err)
	}
	// The cache will get updated in the next cycle by calling Refresh
	return nil
}

func (m *Manager) DeleteNodes(msw types.MachineSetWrapper, nodes []string) error {
	panic("Can't delete nodes. Not yet implemented")
}

func (m *Manager) MachineSetWrapperNodes(msw types.MachineSetWrapper) ([]string, error) {
	m.machineSetsCacheMux.Lock()
	defer m.machineSetsCacheMux.Unlock()
	nodes, exists := m.machineSetToNodes[msw.Id()]
	if !exists {
		return nil, fmt.Errorf("Unable to list nodes for %q node group", msw.Id())
	}
	return nodes, nil
}
