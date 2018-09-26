package main

import (
	"fmt"
)

func (r *ResourceManagement) firstFitInstances() (err error) {
	r.log("firstFitInstances start\n")

	//初始状态
	for _, config := range r.InstanceDeployConfigList {
		m := r.MachineMap[config.MachineId]
		m.AddInstance(r.InstanceMap[config.InstanceId])
		r.DeployMap[config.InstanceId] = m
	}

	//对机器排序，高配置在前，分高在前
	SortMachineByConfigAndCpuCost(r.MachineList)

	//移出非部署目标机器的实例
	instances := MachinesGetInstances(r.MachineList[r.DeployedMachineCount:])
	for _, instance := range instances {
		r.DeployMap[instance.InstanceId].RemoveInstance(instance.InstanceId)
	}

	//FirstFit进要部署的目标机器
	SortInstanceByTotalMaxLowWithInference(instances, 16)
	for i, instance := range instances {
		//if i > 0 && i%10000 == 0 {
		//	r.log("firstFitInstances %d\n", i)
		//}

		deployed := false
		for _, m := range r.MachineList[:r.DeployedMachineCount] {
			if m.ConstraintCheck(instance, 1) {
				m.AddInstance(instance)
				r.DeployMap[instance.InstanceId] = m
				//更新部署数量,todo 这里注释掉是因为手工指定了机器数量，之后放开
				//if machineIndex+1 > r.DeployedMachineCount {
				//	r.DeployedMachineCount = machineIndex + 1
				//}
				deployed = true
				break
			}
		}
		if !deployed {
			return fmt.Errorf(fmt.Sprintf("firstFitInstances failed,%d instanceId=%d,appId=%d",
				i, instance.InstanceId, instance.Config.AppId))
		}
	}

	r.log("firstFitInstances deployedMachineCount=%d,reDeployedInstance=%d,score=%f\n",
		r.DeployedMachineCount, len(instances), MachinesGetScore(r.MachineList))

	return nil
}
