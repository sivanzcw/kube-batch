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
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
)

type reqAction int

const (
	reqSub reqAction = iota
	reqAdd
)

type Bucket struct {
	index       int
	tasks       map[types.UID]*api.TaskInfo
	taskNameSet map[string]int

	// reqScore is score of resource
	// now, we regard 1 CPU and 1 GPU and 1Gi memory as the same score.
	reqScore float64
	request  *api.Resource

	boundTask int
	node      map[string]int
}

func NewBucket() *Bucket {
	return &Bucket{
		index:       0,
		tasks:       make(map[types.UID]*api.TaskInfo),
		taskNameSet: make(map[string]int),

		reqScore: 0,
		request:  api.EmptyResource(),

		boundTask: 0,
		node:      make(map[string]int),
	}
}

func (b *Bucket) CalcResReq(req *api.Resource, action reqAction) {
	if req == nil {
		return
	}

	cpu := req.MilliCPU
	// treat 1Mi the same as 1m cpu 1m gpu
	mem := req.Memory / 1024 / 1024
	score := cpu + mem
	for _, request := range req.ScalarResources {
		score += request
	}

	switch action {
	case reqSub:
		b.reqScore -= score
		b.request.Sub(req)
	case reqAdd:
		b.reqScore += score
		b.request.Add(req)
	}
}

func (b *Bucket) AddTask(taskName string, task *api.TaskInfo) {
	b.taskNameSet[taskName] += 1
	if task.NodeName != "" {
		b.node[task.NodeName]++
		b.boundTask++
		return
	}

	b.tasks[task.Pod.UID] = task
	b.CalcResReq(task.Resreq, reqAdd)
}

func (b *Bucket) TaskBound(task *api.TaskInfo) {
	b.node[task.NodeName]++
	b.boundTask++

	delete(b.tasks, task.Pod.UID)
	b.CalcResReq(task.Resreq, reqSub)
}
