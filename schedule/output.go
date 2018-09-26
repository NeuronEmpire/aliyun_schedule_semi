package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

func (r *ResourceManagement) output(
	machines []*Machine, instanceMoveCommands []*InstanceMoveCommand, jobDeployCommands []*JobDeployCommand) (err error) {
	//输出结果
	outputFile := fmt.Sprintf(r.OutputDir+"/%s", time.Now().Format("20060102_150405"))
	buf := bytes.NewBufferString("")
	if instanceMoveCommands != nil {
		for _, v := range instanceMoveCommands {
			buf.WriteString(fmt.Sprintf("%d,inst_%d,machine_%d\n", v.Round, v.InstanceId, v.MachineId))
		}
	}
	if jobDeployCommands != nil {
		for _, v := range jobDeployCommands {
			buf.WriteString(fmt.Sprintf("%s,machine_%d,%d,%d\n", v.JobId, v.MachineId, v.StartMinutes, v.Count))
		}
	}
	err = ioutil.WriteFile(outputFile+".csv", buf.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}

	totalMachineCount := 0
	for _, m := range machines {
		if m.InstanceListCount > 0 || m.JobListCount > 0 {
			totalMachineCount++
		}
	}

	totalJobCount := 0
	for _, config := range r.JobConfigMap {
		if config != nil {
			totalJobCount += config.InstanceCount
		}
	}

	totalJobWithInstance := 0
	for _, m := range r.MachineList {
		if m.InstanceListCount > 0 {
			totalJobWithInstance += m.JobListCount
		}
	}

	//输出结果说明
	timeCost := time.Now().Sub(r.StartTime).Seconds()
	costReal := MachinesGetScoreReal(machines)
	summaryBuf := bytes.NewBufferString("")
	summaryBuf.WriteString(fmt.Sprintf("%f\n", costReal))
	summaryBuf.WriteString(fmt.Sprintf("输出文件路径=%s\n", outputFile))
	summaryBuf.WriteString(fmt.Sprintf("机器数量=%d\n", totalMachineCount))
	summaryBuf.WriteString(fmt.Sprintf("实例迭代次数配置=%d\n", r.GetDatasetInstanceLoop()))
	summaryBuf.WriteString(fmt.Sprintf("实例迭代次数=%d\n", r.InstanceTotalLoop))
	summaryBuf.WriteString(fmt.Sprintf("实际得分=%f\n", costReal))
	summaryBuf.WriteString(fmt.Sprintf("查表得分=%f\n", MachinesGetScore(machines)))
	summaryBuf.WriteString(fmt.Sprintf("实例部署得分=%f\n", r.InstanceDeployScore))
	summaryBuf.WriteString(fmt.Sprintf("任务部署得分=%f\n", r.JobDeployScore))
	summaryBuf.WriteString(fmt.Sprintf("总时间消耗=%f\n", timeCost))
	summaryBuf.WriteString(fmt.Sprintf("实例部署秒数=%f\n", r.InstanceScheduleSeconds))
	summaryBuf.WriteString(fmt.Sprintf("实例迁移秒数=%f\n", r.InstanceMergeSeconds))
	summaryBuf.WriteString(fmt.Sprintf("任务部署秒数=%f\n", r.JobDeploySeconds))
	summaryBuf.WriteString(fmt.Sprintf("任务部署完成总时间消耗（不包括无限优化）=%f\n", r.JobDeployTotalSeconds))
	summaryBuf.WriteString(fmt.Sprintf("实例迁移次数=%d\n", len(instanceMoveCommands)))
	summaryBuf.WriteString(fmt.Sprintf("任务打包CPU上限=%d,任务打包MEM上限=%d\n", JobPackCpu, JobPackMem))
	summaryBuf.WriteString(fmt.Sprintf("任务调度CPU上限上移步长=%f\n", JobScheduleCpuLimitStep))
	summaryBuf.WriteString(fmt.Sprintf("任务部署数量=%d,总任务数=%d\n", len(jobDeployCommands), totalJobCount))
	summaryBuf.WriteString(fmt.Sprintf("任务无限优化轮数=%d\n", r.JobMergeRound))
	err = ioutil.WriteFile(outputFile+"_summary.csv", summaryBuf.Bytes(), os.ModePerm)
	if err != nil {
		return err
	}

	//更新最佳结果
	var bestScore float64
	update := false
	bestSummaryFile := r.OutputDir + "/best_summary.csv"
	bestSummary, err := ioutil.ReadFile(bestSummaryFile)
	if err != nil {
		update = true
	} else {
		tokens := strings.Split(string(bestSummary), "\n")
		if len(tokens) == 0 {
			update = true
		} else {
			bestScore, err = strconv.ParseFloat(tokens[0], 64)
			if err != nil {
				update = true
			} else {
				if costReal < bestScore {
					update = true
				} else {
					update = false
				}
			}
		}
	}
	if update {
		r.log("output update best cost=%f,old=%f\n", costReal, bestScore)
		bestFile := r.OutputDir + "/best.csv"
		err = ioutil.WriteFile(bestFile, buf.Bytes(), os.ModePerm)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(bestSummaryFile, summaryBuf.Bytes(), os.ModePerm)
		if err != nil {
			return err
		}
	}

	return nil
}
