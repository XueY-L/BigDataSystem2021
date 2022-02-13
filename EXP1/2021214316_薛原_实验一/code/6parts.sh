#!/bin/bash --login
pssh "mkdir -p ~/multi-nodes"             # 在thumm01-thumm06节点的主目录下创建multi-nodes目录
# 删除每个节点之前分到的文本
# pssh "cd ~/multi-nodes"
# pssh "rm -rf ~/multi-nodes/part6_task8_1 ~/multi-nodes/part6_task8_2 ~/multi-nodes/result6_task8_1 ~/multi-nodes/result6_task8_2"
# cd ~/multi-nodes

# 将文本切成6部分
# lines=`cat ~/BigDataset.txt | wc -l`     # 计算BigDataset.txt的行数
# lines_per_node=$(($lines/6+1))              # 将BigDataset.txt划分为12部分，计算每部的行数
# split -l $lines_per_node ../BigDataset.txt -d part6  # 划分BigDataset.txt为part00-part12

# 将不同的部分分别传至不同的节点，每个节点传两个文本
# for ((i=0;i<6;i=i+1));do
#     scp part60$i thumm0$(($i+1)):~/multi-nodes/part6 &
# done
# wait  # 等待节点传输完成

# 让每个节点运行任务，将结果保存在各自的~/multi-nodes/result_task8文件中
pssh "grep '^t' ~/multi-nodes/part6 > ~/multi-nodes/result6"