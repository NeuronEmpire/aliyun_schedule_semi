#如何运行
- 请将输入数据放到./data目录
- 需安装Go 1.10及以上
- 运行run.sh
- 如需单独运行某数据集，请在main文件的main函数中注释其他数据集
- 如需部署到其他机器运行，请参考deploy.sh

#查看结果
- 各数据集的结果在./_output/目录中对应的子目录中
- best.csv,best_summary为某个数据集最优解及其统计信息
- 5份数据综合的结果在./_output/submit/文件夹中，每半小时生成一次

#参数配置
- 各类参数配置在constants.go文件中
- 除了机器数量MachineA,MachineB,MachineC,MachineD,MachineE,其他参数一般不需要修改