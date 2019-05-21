/*
Copyright 2017 The Kubernetes Authors.

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

package reserve

import (
	"fmt"

	"github.com/golang/glog"

	"github.com/kubernetes-sigs/kube-batch/pkg/apis/scheduling/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"
)

type reserveAction struct {
	ssn *framework.Session
}

func New() *reserveAction {
	return &reserveAction{}
}

func (ra *reserveAction) Name() string {
	return "reserve"
}

func (ra *reserveAction) Initialize() {}

func isStarvationJob(job *api.JobInfo) bool {
   	return true
}

func (ra *reserveAction) Execute(ssn *framework.Session) {
	glog.V(3).Infof("Enter Reserve ...")
	defer glog.V(3).Infof("Leaving Reserve ...")

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		if job.PodGroup.Status.Phase == v1alpha1.PodGroupPending {
			continue
		}

		if !isStarvationJob(job) {
			continue
		}

		queue, found := ssn.Queues[job.Queue]
		if found {
			queues.Push(queue)
		} else {
			glog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		if _, found := jobsMap[job.Queue]; !found {
			jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
		}

		glog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		jobsMap[job.Queue].Push(job)
	}

	glog.V(3).Infof("Try to reserve resource to %d Queues", len(jobsMap))

	pendingTasks := map[api.JobID]*util.PriorityQueue{}

	allNodes := util.GetNodeList(ssn.Nodes)

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		if !task.InitResreq.LessEqual(node.Allocatable.Clone()) {
			return fmt.Errorf("task <%s/%s> ResourceFit failed on node <%s>",
				task.Namespace, task.Name, node.Name)
		}

		return ssn.PredicateFn(task, node)
	}

	for {
		if queues.Empty() {
			break
		}

		queue := queues.Pop().(*api.QueueInfo)
		if ssn.Overused(queue) {
			glog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		jobs, found := jobsMap[queue.UID]

		glog.V(3).Infof("Try to reserve resource to Jobs in Queue <%v>", queue.Name)

		if !found || jobs.Empty() {
			glog.V(4).Infof("Can not find jobs for queue %s.", queue.Name)
			continue
		}

		job := jobs.Pop().(*api.JobInfo)
		if _, found := pendingTasks[job.UID]; !found {
			tasks := util.NewPriorityQueue(ssn.TaskOrderFn)
			for _, task := range job.TaskStatusIndex[api.Pending] {
				// Skip BestEffort task in 'allocate' action.
				if task.Resreq.IsEmpty() {
					glog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
						task.Namespace, task.Name)
					continue
				}

				tasks.Push(task)
			}
			pendingTasks[job.UID] = tasks
		}
		tasks := pendingTasks[job.UID]

		glog.V(3).Infof("Try to reserve resource to %d tasks of Job <%v/%v>",
			tasks.Len(), job.Namespace, job.Name)

		for !tasks.Empty() {
			task := tasks.Pop().(*api.TaskInfo)

			glog.V(3).Infof("There are <%d> nodes for Job <%v/%v>",
				len(ssn.Nodes), job.Namespace, job.Name)

			predicateNodes := util.PredicateNodes(task, allNodes, predicateFn)
			if len(predicateNodes) == 0 {
				break
			}

			nodeS := calculateNodeScore(task, predicateNodes[0])
			nodeScores := util.PrioritizeNodes(task, predicateNodes, ssn.NodeOrderMapFn, ssn.NodeOrderReduceFn)

			node := util.SelectBestNode(nodeScores)
			// Allocate idle resource to the task.
			if task.InitResreq.LessEqual(node.Idle) {
				glog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
					task.Namespace, task.Name, node.Name)
				if err := ssn.Allocate(task, node.Name); err != nil {
					glog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
						task.UID, node.Name, ssn.UID, err)
				}
			} else {
				//store information about missing resources
				job.NodesFitDelta[node.Name] = node.Idle.Clone()
				job.NodesFitDelta[node.Name].FitDelta(task.InitResreq)
				glog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s> with limited resources",
					task.Namespace, task.Name, node.Name)

				// Allocate releasing resource to the task if any.
				if task.InitResreq.LessEqual(node.Releasing) {
					glog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
						task.Namespace, task.Name, node.Name, task.InitResreq, node.Releasing)
					if err := ssn.Pipeline(task, node.Name); err != nil {
						glog.Errorf("Failed to pipeline Task %v on %v in Session %v",
							task.UID, node.Name, ssn.UID)
					}
				}
			}

			if ssn.JobReady(job) {
				jobs.Push(job)
				break
			}
		}

		// Added Queue back until no job in Queue.
		queues.Push(queue)
	}
}


func calculateNodeScore(task *api.TaskInfo, node *api.NodeInfo) (int, error) {
	allocatableResources := node.Allocatable.Clone()
	usedResources := node.Used.Clone()

	cpuScore, memScore, scalarScore := calculateResourceScore(usedResources, allocatableResources)
	glog.V(3).Infof("calculateResourceScore")

	resourceScore := (cpuScore + memScore) / 2

	nodeScore := (resourceScore + 2*scalarScore) / 3

	return nodeScore, nil

}

func calculateResourceScore(used *api.Resource, allocatable *api.Resource) (int, int, int) {
	cpuScore := calculateScore(used.MilliCPU, allocatable.MilliCPU)
	memScore := calculateScore(used.Memory, allocatable.Memory)

	scalarScore := 0.0
	length := 0
	for name, quant := range allocatable.ScalarResources {
		var usedScalar float64
		if used, ok := used.ScalarResources[name]; ok {
			usedScalar = used
		}

		scalarScore += float64(calculateScore(usedScalar, quant))
		length ++
	}

	if length == 0 {
		scalarScore = 0.0
	} else {
		scalarScore = float64(scalarScore) / float64(length)
	}

	return cpuScore, memScore, int(scalarScore)
}

func calculateScore(used float64, allocatable float64) int {
	if allocatable == 0 {
		return 0
	}

	if used > allocatable {
		return 0
	}

	score := used *100 / allocatable

	return 100 - int(score)
}

func calculateScoreReduce()

func (ra *reserveAction) UnInitialize() {}
