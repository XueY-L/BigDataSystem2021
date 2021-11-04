import os
import socket
import time
from io import StringIO
from numpy.core import numeric

import pandas as pd

from common import *


class Client:
    def __init__(self):
        self.name_node_sock = socket.socket()
        self.name_node_sock.connect((name_node_host, name_node_port))
    
    def __del__(self):
        self.name_node_sock.close()
    
    def ls(self, dfs_path):
        try:
            # TODO: 向NameNode发送请求，查看dfs_path下文件或者文件夹信息
            request = "ls {}".format(dfs_path)
            self.name_node_sock.send(bytes(request, encoding='utf-8'))
            response = self.name_node_sock.recv(BUF_SIZE)
            print(response)
        except Exception as e:
            print(e)
    
    def copyFromLocal(self, local_path, dfs_path):
        file_size = os.path.getsize(local_path)
        print("File size: {}".format(file_size))
        
        request = "new_fat_item {} {}".format(dfs_path, file_size)
        print("Request: {}".format(request))
        
        # 从NameNode获取一张FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        
        # 根据FAT表逐个向目标DataNode发送数据块
        fp = open(local_path)
        blk_no = 0
        data = None
        for idx, row in fat.iterrows():
            try:
                if idx == 0 or row['blk_no'] != blk_no:
                    data = fp.read(int(row['blk_size']))
                    blk_no = row['blk_no']
                data_node_sock = socket.socket()
                data_node_sock.connect((row['host_name'], data_node_port))
                blk_path = dfs_path + ".blk{}".format(row['blk_no'])
                
                request = "store {}".format(blk_path)
                data_node_sock.send(bytes(request, encoding='utf-8'))
                time.sleep(0.2)  # 两次传输需要间隔一s段时间，避免粘包
                data_node_sock.send(bytes(data, encoding='utf-8'))
                data_node_sock.close()
                continue
            except Exception as e:
                pass
            self.format()
        fp.close()
    
    def copyToLocal(self, dfs_path, local_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # TODO: 从NameNode获取一张FAT表；打印FAT表；根据FAT表逐个从目标DataNode请求数据块，写入到本地文件中
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
  
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))
        
        # 根据FAT表逐个从目标DataNode搞下数据块
        fp = open(local_path,'w')
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
    
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            request = "load {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8')) 
            time.sleep(0.2) 
            # 两次传输需要间隔⼀段时间，避免粘包 
            data = data_node_sock.recv(BUF_SIZE)
            data = str(data, encoding='utf-8')
            fp.write(data)
            data_node_sock.close()
        fp.close()

    def rm(self, dfs_path):
        request = "rm_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        
        # 从NameNode获取改文件的FAT表，获取后删除
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)
        
        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 根据FAT表逐个向目标DataNode发送要删的数据块
        for idx, row in fat.iterrows():
            data_node_sock = socket.socket()
            data_node_sock.connect((row['host_name'], data_node_port))
            blk_path = dfs_path + ".blk{}".format(row['blk_no'])
            
            request = "rm {}".format(blk_path)
            data_node_sock.send(bytes(request, encoding='utf-8'))
            time.sleep(0.2)  # 两次传输需要间隔一s段时间，避免粘包
            response = data_node_sock.recv(BUF_SIZE)
            print(response)
            data_node_sock.close()

    def format(self):
        request = "format"
        print(request)
        
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        print(str(self.name_node_sock.recv(BUF_SIZE), encoding='utf-8'))
        
        for host in host_list:
            data_node_sock = socket.socket()
            data_node_sock.connect((host, data_node_port))
            
            data_node_sock.send(bytes("format", encoding='utf-8'))
            print(str(data_node_sock.recv(BUF_SIZE), encoding='utf-8'))
            
            data_node_sock.close()

    def mprd(self, dfs_path):
        request = "get_fat_item {}".format(dfs_path)
        print("Request: {}".format(request))
        # 从NameNode获取一张FAT表；打印FAT表
        self.name_node_sock.send(bytes(request, encoding='utf-8'))
        fat_pd = self.name_node_sock.recv(BUF_SIZE)

        # 打印FAT表，并使用pandas读取
        fat_pd = str(fat_pd, encoding='utf-8')
        print("Fat: \n{}".format(fat_pd))
        fat = pd.read_csv(StringIO(fat_pd))

        # 记录结果
        sum_xi, sum_xi2, sum_cnt = 0,0,0
        # 计数器，因为重复存储repli可以跳过
        tik = 0
        for idx, row in fat.iterrows():  # todo-跳几行访问，一个块处理一次就行了
            # 更新计数器
            if tik % dfs_replication != 0:
                tik += 1
                continue


            host = row['host_name']
            blk = row['blk_no']
            reducer_sock = socket.socket()
            reducer_sock.connect(('localhost', reducer_port))  # reducer和client都跑在thumm01上
            req = str(host) + " " + str(blk)
            print("Request_to_Reducer: {}".format(req))
            reducer_sock.send(bytes(req, encoding='utf-8'))

            # 接收三个返回结果
            xi = reducer_sock.recv(BUF_SIZE)
            xi = str(xi, encoding='utf-8')
            sum_xi += int(xi)
            
            cnt = reducer_sock.recv(BUF_SIZE)
            cnt = str(cnt, encoding='utf-8')
            sum_cnt += int(cnt)

            xi2 = reducer_sock.recv(BUF_SIZE)
            xi2 = str(xi2, encoding='utf-8')
            sum_xi2 += int(xi2)

            print(sum_xi, sum_xi2, sum_cnt)

            # 更新计数器
            tik += 1
        
        # 整合计算最终结果
        mean = sum_xi / sum_cnt
        var = sum_xi2/sum_cnt - mean**2
        print(mean, var)


# 解析命令行参数并执行对于的命令
import sys

argv = sys.argv
argc = len(argv) - 1

client = Client()

cmd = argv[1]
if cmd == '-ls':
    if argc == 2:
        dfs_path = argv[2]
        client.ls(dfs_path)
    else:
        print("Usage: python client.py -ls <dfs_path>")
elif cmd == "-rm":
    if argc == 2:
        dfs_path = argv[2]
        client.rm(dfs_path)
    else:
        print("Usage: python client.py -rm <dfs_path>")
elif cmd == "-copyFromLocal":
    if argc == 3:
        local_path = argv[2]
        dfs_path = argv[3]
        client.copyFromLocal(local_path, dfs_path)
    else:
        print("Usage: python client.py -copyFromLocal <local_path> <dfs_path>")
elif cmd == "-copyToLocal":
    if argc == 3:
        dfs_path = argv[2]
        local_path = argv[3]
        client.copyToLocal(dfs_path, local_path)
    else:
        print("Usage: python client.py -copyFromLocal <dfs_path> <local_path>")
elif cmd == "-format":
    client.format()
elif cmd == "-mprd":  # MapReduce
    if argc == 2:
        dfs_path = argv[2]
        client.mprd(dfs_path)
    else:
        print("Usage: python client.py -mprd <dfs_path>")
else:
    print("Undefined command: {}".format(cmd))
    print("Usage: python client.py <-ls | -copyFromLocal | -copyToLocal | -rm | -format> other_arguments")
