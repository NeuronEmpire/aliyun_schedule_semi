package main

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

//优化两台机器的实例－随机部署，不要使用，虽然能够快速下降，但下降到一定限度后比较慢
func (r *ResourceManagement) instanceDeployRandomBest(machines []*Machine, instances []*Instance) (bestPos []int, bestCost float64) {
	random := rand.New(rand.NewSource(int64(len(instances))))

	machineCount := len(machines)
	instanceCount := len(instances)
	pos := make([]int, instanceCount)
	bestPos = make([]int, instanceCount)
	bestCost = math.MaxFloat64

	for totalLoop := 0; totalLoop < 1000; totalLoop++ {
		deploy := make([]*Machine, len(machines))
		for i := 0; i < len(deploy); i++ {
			deploy[i] = NewMachine(r, machines[i].MachineId, machines[i].Config)
		}
		failed := false
		for instanceIndex := 0; instanceIndex < instanceCount; instanceIndex++ {
			instance := instances[instanceIndex]
			machineIndex := random.Intn(machineCount)
			m := deploy[machineIndex]
			if !m.ConstraintCheck(instance, 1) {
				failed = true
				continue
			}
			m.AddInstance(instance)
			pos[instanceIndex] = machineIndex
		}
		if !failed {
			totalCost := float64(0)
			for _, m := range deploy {
				totalCost += m.GetCpuCost()
			}

			//fmt.Println("BEST", totalCost)

			//最优解
			if totalCost < bestCost {
				//fmt.Println("BEST", bestCost, totalCost)
				bestCost = totalCost
				for i, v := range pos {
					bestPos[i] = v
				}
				//fmt.Println(bestPos)
			}
		}
	}

	return bestPos, bestCost
}

//todo 可以前若干个随机，剩余的穷举，避免错过更多
func (r *ResourceManagement) instanceDeployForceBest(machines []*Machine, instances []*Instance, deadLoop int) (bestPos []int, bestCost float64) {
	e := deadLoop
	if e > 4 {
		e = 4
	}
	totalLoopLimit := 1024 * 8 * int(math.Pow(float64(2), float64(e)))

	machineCount := len(machines)
	instanceCount := len(instances)
	pos := make([]int, instanceCount)
	bestPos = make([]int, instanceCount)
	bestCost = math.MaxFloat64
	deploy := make([]*Machine, len(machines))
	for i := 0; i < len(deploy); i++ {
		deploy[i] = NewMachine(r, machines[i].MachineId, machines[i].Config)
	}

	totalLoop := 0
	for instanceIndex := 0; instanceIndex < instanceCount; instanceIndex++ {
		instance := instances[instanceIndex]
		added := false

		for ; pos[instanceIndex] < machineCount; pos[instanceIndex]++ {
			totalLoop++
			machineIndex := pos[instanceIndex]
			m := deploy[machineIndex]
			if !m.ConstraintCheck(instance, 1) {
				continue
			}
			m.AddInstance(instance)
			added = true
			break
		}

		if added {
			//有效解,回退
			if instanceIndex == instanceCount-1 {
				totalCost := float64(0)
				for _, m := range deploy {
					totalCost += m.GetCpuCost()
				}

				//fmt.Println("BEST", bestCost, totalCost, pos)

				//最优解
				if totalCost < bestCost {
					//fmt.Println("BEST", bestCost, totalCost)
					bestCost = totalCost
					for i, v := range pos {
						bestPos[i] = v
					}
					//fmt.Println(bestPos)
				}

				//回退
				deploy[pos[instanceIndex]].RemoveInstance(instance.InstanceId)
				pos[instanceIndex] = 0
			}
		} else {
			//回退
			pos[instanceIndex] = 0
		}

		end := false
		if !added || instanceIndex == instanceCount-1 {
			for {
				//已到最后
				instanceIndex--
				if instanceIndex < 0 {
					end = true
					break
				}

				deploy[pos[instanceIndex]].RemoveInstance(instances[instanceIndex].InstanceId)
				pos[instanceIndex]++
				if pos[instanceIndex] < machineCount {
					//进位成功
					instanceIndex--
					break
				} else {
					pos[instanceIndex] = 0
				}
			}
		}

		if end || (instanceCount > 20 && totalLoop > totalLoopLimit) {
			break
		}
	}

	return bestPos, bestCost
}

func (r *ResourceManagement) instanceScheduleRandomThenForce(machines []*Machine, instances []*Instance) (bestPos []int, bestCost float64) {
	random := rand.New(rand.NewSource(int64(len(instances))))

	machineCount := len(machines)
	instanceCount := len(instances)
	pos := make([]int, instanceCount)
	bestPos = make([]int, instanceCount)
	bestCost = math.MaxFloat64

	randomInstanceCount := instanceCount - 18
	if randomInstanceCount < 0 {
		randomInstanceCount = 0
	}

	loopMax := 1024
	for loop := 0; loop < loopMax; loop++ {
		//fmt.Println("loop", loop)
		deploy := make([]*Machine, len(machines))
		for i := 0; i < len(deploy); i++ {
			deploy[i] = NewMachine(r, machines[i].MachineId, machines[i].Config)
		}

		//随机放置前几个
		randomFailed := false
		for instanceIndex := 0; instanceIndex < randomInstanceCount; instanceIndex++ {
			instance := instances[instanceIndex]
			machineIndex := random.Intn(machineCount)
			m := deploy[machineIndex]
			if !m.ConstraintCheck(instance, 1) {
				randomFailed = true
				break
			}
			m.AddInstance(instance)
			pos[instanceIndex] = machineIndex
		}
		if randomFailed {
			//fmt.Println("randomFailed")
			continue
		}

		//穷举剩下的
		for instanceIndex := randomInstanceCount; instanceIndex < instanceCount; instanceIndex++ {
			instance := instances[instanceIndex]
			added := false
			for ; pos[instanceIndex] < machineCount; pos[instanceIndex]++ {
				machineIndex := pos[instanceIndex]
				m := deploy[machineIndex]
				if !m.ConstraintCheck(instance, 1) {
					continue
				}
				m.AddInstance(instance)
				added = true
				break
			}
			if added {
				//有效解,回退
				if instanceIndex == instanceCount-1 {
					totalCost := float64(0)
					for _, m := range deploy {
						totalCost += m.GetCpuCost()
					}

					//最优解
					//fmt.Println("BEST", bestCost, totalCost, pos)
					if totalCost < bestCost {
						bestCost = totalCost
						for i, v := range pos {
							bestPos[i] = v
						}
						//fmt.Println("BEST", bestCost, totalCost)
						//fmt.Println(bestPos)
					}

					//回退
					deploy[pos[instanceIndex]].RemoveInstance(instance.InstanceId)
					pos[instanceIndex] = 0
				}
			} else {
				//回退
				pos[instanceIndex] = 0
			}

			end := false
			if !added || instanceIndex == instanceCount-1 {
				for {
					//已到最后
					instanceIndex--
					if instanceIndex < randomInstanceCount {
						end = true
						break
					}

					deploy[pos[instanceIndex]].RemoveInstance(instances[instanceIndex].InstanceId)
					pos[instanceIndex]++
					if pos[instanceIndex] < machineCount {
						//进位成功
						instanceIndex--
						break
					} else {
						pos[instanceIndex] = 0
					}
				}
			}
			if end {
				//fmt.Println("end")
				break
			}
		}

		break
	}

	return bestPos, bestCost
}

func (r *ResourceManagement) scheduleTwoMachine(machines []*Machine, deadLoop int) (ok bool) {
	instances := make([]*Instance, 0)
	pos := make([]int, 0)
	for i, m := range machines {
		instances = append(instances, m.InstanceList[:m.InstanceListCount]...)
		for j := 0; j < m.InstanceListCount; j++ {
			pos = append(pos, i)
		}
	}

	cost := float64(0)
	for _, m := range machines {
		cost += m.GetCpuCost()
	}

	bestPos, bestCost := r.instanceDeployForceBest(machines, instances, deadLoop)
	if bestCost >= cost {
		return false
	}

	//将需要移动的实例迁出
	for i, instance := range instances {
		if bestPos[i] == pos[i] {
			continue
		}
		machines[pos[i]].RemoveInstance(instance.InstanceId)
	}

	//迁入目标机器
	for i, instance := range instances {
		if bestPos[i] == pos[i] {
			continue
		}

		m := machines[bestPos[i]]
		m.AddInstance(instance)
		r.DeployMap[instance.InstanceId] = m
	}

	return true
}

func (r *ResourceManagement) parallelScheduleMachines(machines []*Machine, deadLoop int) (has bool) {
	//两两分组并行调度
	wg := &sync.WaitGroup{}
	max := len(machines)
	if len(machines)%2 == 1 {
		max = len(machines) - 1
	}
	for i := 0; i < max; i += 2 {
		batchMachines := []*Machine{machines[i], machines[i+1]}
		wg.Add(1)
		go func() {
			defer wg.Done()

			ok := r.scheduleTwoMachine(batchMachines, deadLoop)
			if ok {
				has = true
			}
		}()
	}

	wg.Wait()

	return has
}

//从指定的机器中一大一小地随机一批机器
func (r *ResourceManagement) randomMachines(pool []*Machine, count int, bigSmall []float64, smallBig []float64) (machines []*Machine) {
	machines = make([]*Machine, 0)
	for i := 0; i < count; i++ {
		var table []float64
		if len(machines)%2 == 0 {
			table = bigSmall
		} else {
			table = smallBig
		}

		maxP := table[len(table)-1]
		r := r.Rand.Float64() * maxP
		for machineIndex, p := range table {
			if p < r {
				continue
			}

			if MachinesContains(machines, pool[machineIndex].MachineId) {
				if machineIndex == count-1 {
					machineIndex = -1
				}
				continue
			}

			machines = append(machines, pool[machineIndex])
			break
		}
	}

	return machines
}

//计算实例部署最佳机器自动增长数量
func (r *ResourceManagement) instanceDeployCheckMachinesScale() (machineCountAllocate int) {
	h1 := 0
	h2 := 0
	h3 := 0
	for _, m := range r.MachineList[:r.DeployedMachineCount] {
		if m.GetCpuCost() > ScaleLimitH1 {
			h1++
		}
		if m.GetCpuCost() > ScaleLimitH2 {
			h2++
		}
		if m.GetCpuCost() > ScaleLimitH3 {
			h3++
		}
	}

	count := (h1-h2)/ScaleRatioH1 + (h2-h3)/ScaleRatioH2 + h3/ScaleRatioH3

	r.log("instanceDeployCheckMachinesScale h1=%4d,h2=%4d,h3=%4d,count=%4d\n", h1, h2, h3, count)

	return count
}

func (r *ResourceManagement) tryOutputE() {
	r.InstanceScheduleSeconds = time.Now().Sub(r.InstanceScheduleStartTime).Seconds()

	machines := MachinesCloneWithInstances(r.MachineList)

	r.InstanceMergeStartTime = time.Now()
	instanceMoveCommands, err := NewInstanceMerge(r).Run()
	if err != nil {
		r.log("tryOutputE merge failed,err=%s\n", err.Error())
		return
	}
	r.InstanceMergeSeconds = time.Now().Sub(r.InstanceMergeStartTime).Seconds()

	r.InstanceDeployScore = MachinesGetScore(machines)

	jobDeployCommands := make([]*JobDeployCommand, 0)

	//验证结果
	err = NewReplay(r, instanceMoveCommands, jobDeployCommands).Run()
	if err != nil {
		r.log("tryOutputE replay failed,err=%s\n", err.Error())
		return
	}

	//输出结果
	err = r.output(machines, instanceMoveCommands, jobDeployCommands)
	if err != nil {
		r.log("tryOutputE output failed,err=%s\n", err.Error())
		return
	}
}

func (r *ResourceManagement) instanceSchedule() (err error) {
	startCost := MachinesGetScore(r.MachineList)
	totalLoop := 0
	for scaleCount := 0; ; scaleCount++ {
		currentCost := MachinesGetScore(r.MachineList)
		r.log("instanceSchedule scale=%2d start cost=%f\n", scaleCount, currentCost)
		pTableBigSmall := randBigSmall(r.DeployedMachineCount)
		pTableSmallBig := randSmallBig(r.DeployedMachineCount)
		loop := 1
		deadLoop := 0
		for ; ; loop++ {
			if r.Dataset == "e" {
				if totalLoop == 0 ||
					totalLoop == 128 ||
					totalLoop == 256 ||
					totalLoop == 512 ||
					totalLoop == 1024 ||
					totalLoop == 2048 ||
					totalLoop%4096 == 0 {
					r.InstanceTotalLoop = totalLoop
					r.tryOutputE()
				}
			} else if totalLoop > r.GetDatasetInstanceLoop() {
				return nil
			}
			totalLoop++

			SortMachineByCpuCost(r.MachineList[:r.DeployedMachineCount])
			machinesByCpu := r.randomMachines(r.MachineList[:r.DeployedMachineCount], 64, pTableBigSmall, pTableSmallBig)
			ok := r.parallelScheduleMachines(machinesByCpu, deadLoop)
			if !ok {
				if deadLoop > 16 {
					r.log("instanceSchedule scale=%2d dead loop=%8d,totalLoop=%8d\n", scaleCount, deadLoop, totalLoop)
					if r.Dataset != "e" {
						return nil
					}
				}
				deadLoop++
				continue
			}
			deadLoop = 0

			if totalLoop > 0 && totalLoop%100 == 0 {
				r.log("instanceSchedule scale=%2d loop=%8d,totalLoop=%8d %d %f %f\n",
					scaleCount, loop, totalLoop, r.DeployedMachineCount, startCost, MachinesGetScore(r.MachineList))
			}
		}
	}

	return nil
}
