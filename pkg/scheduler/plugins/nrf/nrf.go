package nrf

import (
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const PluginName = "nrf"

type nrfPlugin struct {
	arguments      framework.Arguments
	cpuFraction    float64
	memoryFraction float64
	nodesMap       map[string]nodePercentage
}

type nodePercentage struct {
	cpu    float64
	memory float64
}

func New(args framework.Arguments) framework.Plugin {
	return &nrfPlugin{
		arguments:      args,
		cpuFraction:    1.0,
		memoryFraction: 1.0,
		nodesMap:       make(map[string]nodePercentage),
	}
}

func countUsageOnNode(node *api.NodeInfo) (error, nodePercentage) {
	memUsage := 0.0
	cpuUsage := 0.0
	for _, t := range node.Tasks {
		if t.Pod.Status.Phase == "Running" && t.Pod.Spec.SchedulerName == scheduler.Name {
			memUsage = memUsage + t.Resreq.Memory
			cpuUsage = cpuUsage + t.Resreq.MilliCPU
		}
	}

	return nil, nodePercentage{
		memory: memUsage / node.Allocatable.Memory,
		cpu:    cpuUsage / node.Allocatable.MilliCPU,
	}
}

func (nrf *nrfPlugin) CreateNodesMap(ssn *framework.Session) {
	for _, node := range ssn.Nodes {
		err, usg := countUsageOnNode(node.Clone())
		if err != nil {
			nrf.nodesMap[node.Name] = usg
		}
	}
}

func (nrf *nrfPlugin) Name() string {
	return PluginName
}

func (nrf *nrfPlugin) OnSessionOpen(ssn *framework.Session) {
	ssn.TotalResource.Memory = ssn.TotalResource.Memory * nrf.memoryFraction
	ssn.TotalResource.MilliCPU = ssn.TotalResource.MilliCPU * nrf.cpuFraction
	nrf.CreateNodesMap(ssn)
	klog.V(1).Infof("Nodes map: %v", nrf.nodesMap)
}

func (nrf *nrfPlugin) OnSessionClose(ssn *framework.Session) {
}
