/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package nodeorder

import (
	"fmt"

	"github.com/golang/glog"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/priorities"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
	"k8s.io/kubernetes/pkg/scheduler/cache"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/plugins/util"
)

const (
	// NodeAffinityWeight is the key for providing Node Affinity Priority Weight in YAML
	NodeAffinityWeight = "nodeaffinity.weight"
	// PodAffinityWeight is the key for providing Pod Affinity Priority Weight in YAML
	PodAffinityWeight = "podaffinity.weight"
	// LeastRequestedWeight is the key for providing Least Requested Priority Weight in YAML
	LeastRequestedWeight = "leastrequested.weight"
	// BalancedResourceWeight is the key for providing Balanced Resource Priority Weight in YAML
	BalancedResourceWeight = "balancedresource.weight"
)

type nodeOrderPlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

func getInterPodAffinityScore(name string, interPodAffinityScore schedulerapi.HostPriorityList) int {
	for _, hostPriority := range interPodAffinityScore {
		if hostPriority.Host == name {
			return hostPriority.Score
		}
	}
	return 0
}

func generateNodeMapAndSlice(nodes map[string]*api.NodeInfo) (map[string]*cache.NodeInfo, []*v1.Node) {
	var nodeMap map[string]*cache.NodeInfo
	var nodeSlice []*v1.Node
	nodeMap = make(map[string]*cache.NodeInfo)
	for _, node := range nodes {
		nodeInfo := cache.NewNodeInfo(node.Pods()...)
		nodeInfo.SetNode(node.Node)
		nodeMap[node.Name] = nodeInfo
		nodeSlice = append(nodeSlice, node.Node)
	}
	return nodeMap, nodeSlice
}

type cachedNodeInfo struct {
	session *framework.Session
}

func (c *cachedNodeInfo) GetNodeInfo(name string) (*v1.Node, error) {
	node, found := c.session.Nodes[name]
	if !found {
		for _, cacheNode := range c.session.Nodes {
			pods := cacheNode.Pods()
			for _, pod := range pods {
				if pod.Spec.NodeName == "" {
					return cacheNode.Node, nil
				}
			}
		}
		return nil, fmt.Errorf("failed to find node <%s>", name)
	}

	return node.Node, nil
}

//New function returns prioritizePlugin object
func New(aruguments framework.Arguments) framework.Plugin {
	return &nodeOrderPlugin{pluginArguments: aruguments}
}

func (pp *nodeOrderPlugin) Name() string {
	return "nodeorder"
}

type priorityWeight struct {
	leastReqWeight          int
	nodeAffinityWeight      int
	podAffinityWeight       int
	balancedRescourceWeight int
}

func calculateWeight(args framework.Arguments) priorityWeight {
	/*
	   User Should give priorityWeight in this format(nodeaffinity.weight, podaffinity.weight, leastrequested.weight, balancedresource.weight).
	   Currently supported only for nodeaffinity, podaffinity, leastrequested, balancedresouce priorities.

	   actions:
	   - name: enqueue
	   - name: reclaim
	   - name: allocate
	   - name: backfill
	   - name: preempt
	   tiers:
	   - plugins:
	     - name: priority
	     - name: gang
	     - name: conformance
	   - plugins:
	     - name: drf
	     - name: predicates
	     - name: proportion
	     - name: nodeorder
	       arguments:
	         nodeaffinity.weight: 2
	         podaffinity.weight: 2
	         leastrequested.weight: 2
	         balancedresource.weight: 2
	*/

	// Values are initialized to 1.
	weight := priorityWeight{
		leastReqWeight:          1,
		nodeAffinityWeight:      1,
		podAffinityWeight:       1,
		balancedRescourceWeight: 1,
	}

	// Checks whether nodeaffinity.weight is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.nodeAffinityWeight, NodeAffinityWeight)

	// Checks whether podaffinity.weight is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.podAffinityWeight, PodAffinityWeight)

	// Checks whether leastrequested.weight is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.leastReqWeight, LeastRequestedWeight)

	// Checks whether balancedresource.weight is provided or not, if given, modifies the value in weight struct.
	args.GetInt(&weight.balancedRescourceWeight, BalancedResourceWeight)

	return weight
}

func (pp *nodeOrderPlugin) OnSessionOpen(ssn *framework.Session) {
	nodeOrderFn := func(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {

		weight := calculateWeight(pp.pluginArguments)

		pl := &util.PodLister{
			Session: ssn,
		}

		nl := &util.NodeLister{
			Session: ssn,
		}

		cn := &cachedNodeInfo{
			session: ssn,
		}

		var nodeMap map[string]*cache.NodeInfo
		var nodeSlice []*v1.Node
		var interPodAffinityScore schedulerapi.HostPriorityList

		nodeMap, nodeSlice = generateNodeMapAndSlice(ssn.Nodes)

		nodeInfo := cache.NewNodeInfo(node.Pods()...)
		nodeInfo.SetNode(node.Node)
		var score = 0.0

		//TODO: Add ImageLocalityPriority Function once priorityMetadata is published
		//Issue: #74132 in kubernetes ( https://github.com/kubernetes/kubernetes/issues/74132 )

		host, err := priorities.LeastRequestedPriorityMap(task.Pod, nil, nodeInfo)
		if err != nil {
			glog.Warningf("Least Requested Priority Failed because of Error: %v", err)
			return 0, err
		}
		// If leastReqWeight in provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
		score = score + float64(host.Score*weight.leastReqWeight)

		host, err = priorities.BalancedResourceAllocationMap(task.Pod, nil, nodeInfo)
		if err != nil {
			glog.Warningf("Balanced Resource Allocation Priority Failed because of Error: %v", err)
			return 0, err
		}
		// If balancedRescourceWeight in provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
		score = score + float64(host.Score*weight.balancedRescourceWeight)

		host, err = priorities.CalculateNodeAffinityPriorityMap(task.Pod, nil, nodeInfo)
		if err != nil {
			glog.Warningf("Calculate Node Affinity Priority Failed because of Error: %v", err)
			return 0, err
		}
		// If nodeAffinityWeight in provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
		score = score + float64(host.Score*weight.nodeAffinityWeight)

		mapFn := priorities.NewInterPodAffinityPriority(cn, nl, pl, v1.DefaultHardPodAffinitySymmetricWeight)
		interPodAffinityScore, err = mapFn(task.Pod, nodeMap, nodeSlice)
		if err != nil {
			glog.Warningf("Calculate Inter Pod Affinity Priority Failed because of Error: %v", err)
			return 0, err
		}
		hostScore := getInterPodAffinityScore(node.Name, interPodAffinityScore)
		// If podAffinityWeight in provided, host.Score is multiplied with weight, if not, host.Score is added to total score.
		score = score + float64(hostScore*weight.podAffinityWeight)

		glog.V(4).Infof("Total Score for that node is: %d", score)
		return score, nil
	}
	ssn.AddNodeOrderFn(pp.Name(), nodeOrderFn)
}

func (pp *nodeOrderPlugin) OnSessionClose(ssn *framework.Session) {
}
