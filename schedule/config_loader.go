package main

import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

func trimApp(s string) string {
	return s[4:]
}

func trimMachine(s string) string {
	return s[8:]
}

func trimInstance(s string) string {
	return s[5:]
}

func LoadCsv(file string) (data [][]string, err error) {
	f, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(f), "\n")
	for _, l := range lines {
		if strings.TrimSpace(l) == "" {
			continue
		}

		data = append(data, strings.Split(l, ","))
	}

	return data, nil
}

func LoadAppInterferenceConfig(file string) (result []*AppInterferenceConfig, err error) {
	data, err := LoadCsv(file)
	if err != nil {
		return nil, err
	}

	result = make([]*AppInterferenceConfig, len(data))
	for i, v := range data {
		if len(v) < 3 {
			return nil, fmt.Errorf("loadAppInterference data row len<3")
		}

		item := &AppInterferenceConfig{}
		column := 0

		item.AppId1, err = strconv.Atoi(trimApp(v[column]))
		if err != nil {
			return nil, err
		}
		column++

		item.AppId2, err = strconv.Atoi(trimApp(v[column]))
		if err != nil {
			return nil, err
		}
		column++

		item.Interference, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		result[i] = item
	}

	return result, nil
}

func LoadAppResourceConfig(file string) (result []*AppResourcesConfig, err error) {
	data, err := LoadCsv(file)
	if err != nil {
		return nil, err
	}

	result = make([]*AppResourcesConfig, len(data))
	for i, v := range data {
		if len(v) < 6 {
			return nil, fmt.Errorf("loadAppResource data row len<6")
		}

		item := &AppResourcesConfig{}
		column := 0

		item.AppId, err = strconv.Atoi(trimApp(v[column]))
		if err != nil {
			return nil, err
		}
		column++

		cpuTokens := strings.Split(v[column], "|")
		if len(cpuTokens) != len(item.Cpu) {
			return nil,
				fmt.Errorf("loadAppResource cpu len %d failed,required %d", len(cpuTokens), len(item.Cpu))
		}
		for tokenIndex, token := range cpuTokens {
			item.Cpu[tokenIndex], err = strconv.ParseFloat(token, 64)
			if err != nil {
				return nil, err
			}
		}
		column++

		memTokens := strings.Split(v[column], "|")
		if len(memTokens) != len(item.Mem) {
			return nil,
				fmt.Errorf("loadAppResource mem len %d failed,required %d", len(cpuTokens), len(item.Cpu))
		}
		for tokenIndex, token := range memTokens {
			item.Mem[tokenIndex], err = strconv.ParseFloat(token, 64)
			if err != nil {
				return nil, err
			}
		}
		column++

		fDisk, err := strconv.ParseFloat(v[column], 64)
		if err != nil {
			return nil, err
		}
		if float64(int(fDisk)) != fDisk {
			return nil, fmt.Errorf("disk not integer %f", fDisk)
		}

		item.Disk = int(fDisk)
		column++

		item.P, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		item.M, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		item.PM, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		result[i] = item
	}

	return result, nil
}

func LoadInstanceDeployConfig(file string) (result []*InstanceDeployConfig, err error) {
	data, err := LoadCsv(file)
	if err != nil {
		return nil, err
	}

	result = make([]*InstanceDeployConfig, len(data))
	for i, v := range data {
		item := &InstanceDeployConfig{}
		column := 0

		item.InstanceId, err = strconv.Atoi(trimInstance(v[column]))
		if err != nil {
			return nil, err
		}
		column++

		item.AppId, err = strconv.Atoi(trimApp(v[column]))
		if err != nil {
			return nil, err
		}
		column++

		if v[column] != "" {
			item.MachineId, err = strconv.Atoi(trimMachine(v[column]))
			if err != nil {
				return nil, err
			}
		}

		column++

		result[i] = item
	}

	return result, nil
}

func LoadMachineResourcesConfig(file string) (result []*MachineResourcesConfig, err error) {
	data, err := LoadCsv(file)
	if err != nil {
		return nil, err
	}

	result = make([]*MachineResourcesConfig, len(data))
	for i, v := range data {
		item := &MachineResourcesConfig{}
		column := 0

		item.MachineId, err = strconv.Atoi(trimMachine(v[column]))
		if err != nil {
			return nil, err
		}
		column++

		item.Cpu, err = strconv.ParseFloat(v[column], 64)
		if err != nil {
			return nil, err
		}
		column++

		item.Mem, err = strconv.ParseFloat(v[column], 64)
		if err != nil {
			return nil, err
		}
		column++

		fDisk, err := strconv.ParseFloat(v[column], 64)
		if err != nil {
			return nil, err
		}
		if float64(int(fDisk)) != fDisk {
			return nil, fmt.Errorf("disk not integer %f", fDisk)
		}
		item.Disk = int(fDisk)
		column++

		item.P, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		item.M, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		item.PM, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		result[i] = item
	}

	return result, nil
}

func LoadAppConfig(resourceConfigFile string, inferenceConfigFile string) (
	appResourcesConfigMap []*AppResourcesConfig, appInferenceConfigMap [][]int, err error) {
	appResourcesConfigList, err := LoadAppResourceConfig(resourceConfigFile)
	if err != nil {
		return nil, nil, err
	}
	appInferenceConfigList, err := LoadAppInterferenceConfig(inferenceConfigFile)
	if err != nil {
		return nil, nil, err
	}

	maxAppId := 0
	for _, v := range appResourcesConfigList {
		v.CalcTimedResourceStatistics()

		for _, inference := range appInferenceConfigList {
			if inference.AppId1 == v.AppId || inference.AppId2 == v.AppId {
				v.InferenceAppCount++
			}
		}

		if v.AppId > maxAppId {
			maxAppId = v.AppId
		}
	}

	appResourcesConfigMap = make([]*AppResourcesConfig, maxAppId+1)
	for _, v := range appResourcesConfigList {
		appResourcesConfigMap[v.AppId] = v
	}

	appInferenceConfigMap = make([][]int, maxAppId+1)
	for i := 0; i < len(appInferenceConfigMap); i++ {
		appInferenceConfigMap[i] = make([]int, maxAppId+1)
		for j := 0; j < len(appInferenceConfigMap[i]); j++ {
			appInferenceConfigMap[i][j] = -1
		}
	}
	for _, v := range appInferenceConfigList {
		appResource1 := appResourcesConfigMap[v.AppId1]
		if appResource1 == nil {
			return nil, nil,
				fmt.Errorf("LoadAppConfig app %d not exists", v.AppId1)
		}

		appResource2 := appResourcesConfigMap[v.AppId2]
		if appResource2 == nil {
			return nil, nil,
				fmt.Errorf("SaveAppInterferenceConfig app %d not esists", v.AppId2)
		}

		appInferenceConfigMap[v.AppId1][v.AppId2] = v.Interference
	}

	return appResourcesConfigMap, appInferenceConfigMap, nil
}

func LoadJobConfig(file string) (result []*JobConfig, err error) {
	data, err := LoadCsv(file)
	if err != nil {
		return nil, err
	}

	nameId := NewNameId()

	result = make([]*JobConfig, len(data))
	for i, v := range data {
		item := &JobConfig{}
		column := 0
		item.RealJobId = v[column]
		item.JobId = nameId.GetId(item.RealJobId)
		column++

		item.Cpu, err = strconv.ParseFloat(v[column], 64)
		if err != nil {
			return nil, err
		}
		column++

		item.Mem, err = strconv.ParseFloat(v[column], 64)
		if err != nil {
			return nil, err
		}
		column++

		item.InstanceCount, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		item.ExecMinutes, err = strconv.Atoi(v[column])
		if err != nil {
			return nil, err
		}
		column++

		item.PreJobs = make([]int, 0)
		for ; column < len(v); column++ {
			if strings.TrimSpace(v[column]) == "" {
				continue
			}

			id := nameId.GetId(v[column])
			item.PreJobs = append(item.PreJobs, id)
		}

		result[i] = item
	}

	return result, nil
}

func LoadJobDAG(file string) (jobConfigMap []*JobConfig, jobConfigDAG []*JobConfig, err error) {
	list, err := LoadJobConfig(file)
	if err != nil {
		return nil, nil, err
	}

	//JobId->JobConfig映射
	jobConfigMap = make([]*JobConfig, len(list)+1)
	for _, v := range list {
		jobConfigMap[v.JobId] = v
	}

	//构造DAG
	jobConfigDAG = make([]*JobConfig, 0)
	for _, v := range jobConfigMap {
		if v == nil || len(v.PreJobs) == 0 {
			continue
		}

		for _, preJobId := range v.PreJobs {
			parent := jobConfigMap[preJobId]
			parent.Children = append(parent.Children, v)
			v.Parents = append(v.Parents, parent)
		}
	}
	for _, v := range jobConfigMap {
		if v != nil && len(v.Parents) == 0 {
			jobConfigDAG = append(jobConfigDAG, v)
		}
	}

	return jobConfigMap, jobConfigDAG, nil
}
