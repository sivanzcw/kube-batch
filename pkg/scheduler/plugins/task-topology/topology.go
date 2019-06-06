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
	"encoding/json"
	"time"

	"github.com/golang/glog"

	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"

	tutil "volcano.sh/apollo/pkg/util/plugins/task-topology"
)

type taskTopologyPlugin struct {
	arguments framework.Arguments

	weight   int
	managers map[api.JobID]*JobManager
}

// New function returns taskTopologyPlugin object
func New(arguments framework.Arguments) framework.Plugin {
	return &taskTopologyPlugin{
		arguments: arguments,

		weight:   calculateWeight(arguments),
		managers: make(map[api.JobID]*JobManager),
	}
}

func (p *taskTopologyPlugin) Name() string {
	return tutil.Name
}

// TaskOrderFn returns -1 to make l prior to r.
//
// for example:
// A:
//  | bucket1   | bucket2   | out of bucket
//  | a1 a3     | a2        | a4
// B:
//  | bucket1   | out of bucket
//  | b1 b2     | b3
// the right task order should be:
//   a1 a3 a2 b1 b2 a4 b3
func (p *taskTopologyPlugin) TaskOrderFn(l interface{}, r interface{}) int {
	lv := l.(*api.TaskInfo)
	rv := r.(*api.TaskInfo)

	lvJobManager := p.managers[lv.Job]
	rvJobManager := p.managers[rv.Job]

	var lvBucket, rvBucket *Bucket
	if lvJobManager != nil {
		lvBucket = lvJobManager.GetBucket(lv)
	}
	if rvJobManager != nil {
		rvBucket = rvJobManager.GetBucket(rv)
	}

	// the one have bucket would always prior to another
	lvInBucket := lvBucket != nil
	rvInBucket := rvBucket != nil
	if lvInBucket != rvInBucket {
		if lvInBucket {
			return -1
		}
		return 1
	}

	// comparison between job is not the duty of this plugin
	if lv.Job != rv.Job {
		return 0
	}

	// task out of bucket have no order
	if !lvInBucket && !rvInBucket {
		return 0
	}

	// the big bucket should prior to small one
	lvHasTask := len(lvBucket.tasks)
	rvHasTask := len(rvBucket.tasks)
	if lvHasTask != rvHasTask {
		if lvHasTask > rvHasTask {
			return -1
		}
		return 1
	}

	lvBucketIndex := lvBucket.index
	rvBucketIndex := rvBucket.index
	// in the same bucket, the affinityOrder is ok
	if lvBucketIndex == rvBucketIndex {
		affinityOrder := lvJobManager.taskAffinityOrder(lv, rv)
		return -affinityOrder
	}

	// the old bucket should prior to young one
	if lvBucketIndex < rvBucketIndex {
		return -1
	}
	return 1
}

func (p *taskTopologyPlugin) calcBucketScore(task *api.TaskInfo, node *api.NodeInfo) (int, *JobManager, error) {
	// task could never fits the node
	maxResource := node.Idle.Clone().Add(node.Releasing)
	if req := task.Resreq; req != nil && maxResource.Less(req) {
		return 0, nil, nil
	}

	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		return 0, nil, nil
	}

	bucket := jobManager.GetBucket(task)
	// task out of bucket
	if bucket == nil {
		return 0, jobManager, nil
	}

	// 1. bound task in bucket is the base score of this node
	score := bucket.node[node.Name]

	// 2. task inter/self anti-affinity should be calculated
	if nodeTaskSet := jobManager.nodeTaskSet[node.Name]; nodeTaskSet != nil {
		taskName := getTaskName(task)
		affinityScore := jobManager.checkTaskSetAffinity(taskName, nodeTaskSet, true)
		if affinityScore < 0 {
			score += affinityScore
		}
	}
	glog.V(4).Infof("task %s/%s, node %s, additional score %d, task %d", task.Namespace, task.Name, node.Name, score, len(bucket.tasks))

	// 3. the other tasks in bucket take into considering
	score += len(bucket.tasks)
	if bucket.request == nil || bucket.request.LessEqual(maxResource) {
		return score, jobManager, nil
	}

	remains := bucket.request.Clone()
	// randomly (by map) take out task to make the bucket fits the node
	for bucketTaskId, bucketTask := range bucket.tasks {
		// current task should kept in bucket
		if bucketTaskId == task.Pod.UID || bucketTask.Resreq == nil {
			continue
		}
		remains.Sub(bucketTask.Resreq)
		score--
		if remains.LessEqual(maxResource) {
			break
		}
	}
	// here, the bucket remained request will always fit the maxResource
	return score, jobManager, nil
}

func (p *taskTopologyPlugin) NodeOrderFn(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
	score, jobManager, err := p.calcBucketScore(task, node)
	if err != nil {
		return 0, err
	}
	fScore := float64(score * p.weight)
	if jobManager != nil && jobManager.bucketMaxSize != 0 {
		fScore = fScore * float64(schedulerapi.MaxPriority) / float64(jobManager.bucketMaxSize)
	}
	glog.V(4).Infof("task %s/%s at node %s has bucket score %d, score %f", task.Namespace, task.Name, node.Name, score, fScore)
	return fScore, nil
}

func (p *taskTopologyPlugin) AllocateFunc(event *framework.Event) {
	task := event.Task

	jobManager, hasManager := p.managers[task.Job]
	if !hasManager {
		return
	}
	jobManager.TaskBound(task)
}

func (p *taskTopologyPlugin) initBucket(ssn *framework.Session) {
	for jobId, job := range ssn.Jobs {
		group := job.PodGroup
		jobTopologyStr, ok := group.Annotations[tutil.JobAffinityKey]
		if !ok {
			continue
		}

		if allTaskHaveNode(job) {
			glog.V(4).Infof("%s job %s/%s all task have node", tutil.Name, job.Namespace, job.Name)
			continue
		}

		var jobTopology tutil.TaskTopology
		err := json.Unmarshal([]byte(jobTopologyStr), &jobTopology)
		if err != nil {
			glog.Errorf("get task topology for group %s/%s failed, with err %v", group.Namespace, group.Name, err)
			continue
		}

		manager := NewJobManager(jobId)
		manager.ApplyTaskTopology(&jobTopology)
		manager.ConstructBucket(job.Tasks)

		p.managers[job.UID] = manager

		if glog.V(4) {
			glog.Info(manager.String())
		}
	}
}

func (p *taskTopologyPlugin) OnSessionOpen(ssn *framework.Session) {
	start := time.Now()
	glog.V(3).Infof("start to init task topology plugin, weight[%d], defined order %v", p.weight, affinityPriority)

	p.initBucket(ssn)

	ssn.AddTaskOrderFn(p.Name(), p.TaskOrderFn)

	ssn.AddNodeOrderFn(p.Name(), p.NodeOrderFn)

	ssn.AddEventHandler(&framework.EventHandler{
		AllocateFunc: p.AllocateFunc,
	})

	glog.V(3).Infof("finished to init task topology plugin, using time %v", time.Since(start))
}

func (p *taskTopologyPlugin) OnSessionClose(ssn *framework.Session) {
	p.managers = nil
}
