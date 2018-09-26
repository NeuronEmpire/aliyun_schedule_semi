package main

func (r *ResourceManagement) analysis() {
	jobTrees := 0
	for _, v := range r.JobConfigDAG {
		if len(v.Children) > 0 {
			jobTrees++
		}
	}

	jobs := 0
	leafs := 0
	for _, v := range r.JobConfigMap {
		if v == nil {
			continue
		}

		jobs += v.InstanceCount

		if v.Children == nil || len(v.Children) == 0 {
			leafs += v.InstanceCount
		}
	}

	r.log("instances=%d\n", len(r.InstanceDeployConfigList))
	r.log("jobs=%d,totalJobs=%d,rootJobs=%d,trees=%d,leafs=%d\n",
		len(r.JobConfigMap), jobs, len(r.JobConfigDAG), jobTrees, leafs)
	r.log("maxMachineId=%d,maxInstanceId=%d,maxJobInstanceId=%d\n",
		r.MaxMachineId, r.MaxInstanceId, r.MaxJobInstanceId)
}
