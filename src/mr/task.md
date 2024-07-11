1. 实现一个分布式的MapReduce，包含Coordinator和Worker。一个Coordinator和多个Worker（并行）
2. Coordinator和Worker通过RPC通信，Worker循环处理向Coordinator请求task，执行任务读取一个或多个文件，处理并输出一个或多个文件，然后请求新的任务 
3. Coordinator应该通知Worker 任务超时没完成，然后将任务分给不同的worker

rules
1. map 应该将中见文件写成nReduce个，供reduce读取。
2. 第X个reduce task 输出的文件应该为mr-out-X
3. 输出格式"%v %v"
4. Map的输出放在当前路径，稍后给Reduce读取
5. mr/coordinator.go应该实现Done()方法，当返回true代表Coordinator完成，随后将退出
6. job完成，worker应该退出。一个简单的方法时使用call()方法的返回值，如果worker跟Coordinator联系不上
则假设Coordinator已经退出了，因为job已经完成了。


coordinator创建
简单的分配，一个文件创建一个map，输出nReduce个文件供reduce读取
map需要保存的信息 编号 对应的文件，需要输出的文件个数 output-x-y  x为map编号  y为reduce编号
reduce编号即可


RPC结构定义
Args: worker Id
Replay: Task结构

Task:
type: 类型 map Or Reduce
id: 编码 Map编号 reduce编号 (reduce1读取的是 output-x-1 output-x-y x为mapId y为reduceId)  
file_nums: 输出文件个数 nReduce
file_names:


	