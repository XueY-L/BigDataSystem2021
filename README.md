# BigDataSystem2021
大数据系统基础（B）课程实验归档（教师：王智）

* EXP1 Linux基础
  * 略
* EXP2 Hadoop并行编程
  * MapReduce实现存在bug：name_node按照dfs_blk_size分割数据集，对于所造数字数据集，可能存在某个数被切开读取的情况。需要修改文件以实现按行分割，FAT表项也需做相应修改。
* EXP3 Spark机器学习
  * 比实验二简单点，但scala语法太难写
* EXP4 
  * 因同学呼声太高取消了

* FinalProject
  * Mars & Ray & TF 三个分布式平台的对比
  * 任务选取为矩阵乘法 & 类似FL的机器学习任务
