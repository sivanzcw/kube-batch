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
package task_topology

import (
	"volcano.sh/volcano/pkg/apis/batch/v1alpha1"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"

	tutil "volcano.sh/apollo/pkg/util/plugins/task-topology"
)

const (
	OutOfBucket = -1

	Weight = "task-topology.weight"
)

func calculateWeight(args framework.Arguments) int {
	/*
	   User Should give taskTopologyWeight in this format(task-topology.weight).

	   actions: "enqueue, reclaim, allocate, backfill, preempt"
	   tiers:
	   - plugins:
	     - name: task-topology
	       arguments:
	         task-topology.weight: 10
	*/
	// Values are initialized to 1.
	weight := 1

	args.GetInt(&weight, Weight)

	return weight
}

func getTaskName(task *api.TaskInfo) string {
	return task.Pod.Annotations[v1alpha1.TaskSpecKey]
}

func addAffinity(m map[string]map[string]tutil.Empty, src, dst string) {
	srcMap, ok := m[src]
	if !ok {
		srcMap = make(map[string]tutil.Empty)
		m[src] = srcMap
	}
	srcMap[dst] = tutil.Empty{}
}

func allTaskHaveNode(job *api.JobInfo) bool {
	return len(job.TaskStatusIndex[api.Pending]) == 0
}

type TaskOrder struct {
	tasks []*api.TaskInfo

	manager *JobManager
}

func (p *TaskOrder) Len() int { return len(p.tasks) }

func (p *TaskOrder) Swap(l, r int) {
	p.tasks[l], p.tasks[r] = p.tasks[r], p.tasks[l]
}

func (p *TaskOrder) Less(l, r int) bool {
	L := p.tasks[l]
	R := p.tasks[r]

	LHasNode := L.NodeName != ""
	RHasNode := R.NodeName != ""
	if LHasNode || RHasNode {
		// the task bounded would have high priority
		if LHasNode != RHasNode {
			return !LHasNode
		}
		// all bound, any order is alright
		return L.NodeName > R.NodeName
	}

	result := p.manager.taskAffinityOrder(L, R)
	// they have the same taskAffinity order, any order is alright
	if result == 0 {
		return L.Name > R.Name
	}
	return result < 0
}
