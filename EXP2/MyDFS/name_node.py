# coding=UTF-8
import math
import os
import socket
import time
import threading

import numpy as np
from numpy.core.fromnumeric import trace
import pandas as pd


from common import *


# NameNode功能
# 1. 保存文件的块存放位置信息
# 2. ls ： 获取文件/目录信息
# 3. get_fat_item： 获取文件的FAT表项
# 4. new_fat_item： 根据文件大小创建FAT表项
# 5. rm_fat_item： 删除一个FAT表项
# 6. format: 删除所有FAT表项

class NameNode:
    def run(self):  # 启动NameNode
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", name_node_port))
            listen_fd.listen(5)
            print("Name node started")
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("connected by {}".format(addr))
                
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(128), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print("Request: {}".format(request))
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    
                    if cmd == "ls":  # 若指令类型为ls, 则返回DFS上对于文件、文件夹的内容
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.ls(dfs_path)
                    elif cmd == "get_fat_item":  # 指令类型为获取FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.get_fat_item(dfs_path)
                    elif cmd == "new_fat_item":  # 指令类型为新建FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        file_size = int(request[2])
                        response = self.new_fat_item(dfs_path, file_size)
                    elif cmd == "rm_fat_item":  # 指令类型为删除FAT表项
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm_fat_item(dfs_path)
                    elif cmd == "format":
                        response = self.format()
                    else:  # 其他位置指令
                        response = "Undefined command: " + " ".join(request)
                    
                    print("Response: {}".format(response))
                    sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
                    break
                except Exception as e:  # 如果出错则打印错误信息
                    print(e)
                finally:
                    sock_fd.close()  # 释放连接
        except KeyboardInterrupt:  # 如果运行时按Ctrl+C则退出程序
            pass
        except Exception as e:  # 如果出错则打印错误信息
            print(e)
        finally:
            listen_fd.close()  # 释放连接
    
    def ls(self, dfs_path):
        local_path = name_node_dir + dfs_path
        # 如果文件不存在，返回错误信息
        if not os.path.exists(local_path):
            return "No such file or directory: {}".format(dfs_path)
        
        if os.path.isdir(local_path):
            # 如果目标地址是一个文件夹，则显示该文件夹下内容
            dirs = os.listdir(local_path)
            response = " ".join(dirs)
        else:
            # 如果目标是文件则显示文件的FAT表信息
            with open(local_path) as f:
                response = f.read()
        
        return response
    
    def get_fat_item(self, dfs_path):
        # 获取FAT表内容
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        return response.to_csv(index=False)
    
    def new_fat_item(self, dfs_path, file_size):
        nb_blks = int(math.ceil(file_size / dfs_blk_size))
        print(file_size, nb_blks)
        
        # todo 如果dfs_replication为复数时可以新增host_name的数目
        data_pd = pd.DataFrame(columns=['blk_no', 'host_name', 'blk_size'])
        num_row = 0
        for i in range(nb_blks):
            blk_no = i
            host_name = np.random.choice(host_list, size=dfs_replication, replace=False)
            blk_size = min(dfs_blk_size, file_size - i * dfs_blk_size)
            for j in host_name:
                data_pd.loc[num_row] = [blk_no, j, blk_size]
                num_row += 1
        # 获取本地路径
        local_path = name_node_dir + dfs_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 保存FAT表为CSV文件
        data_pd.to_csv(local_path, index=False)
        # 同时返回CSV内容到请求节点
        return data_pd.to_csv(index=False)
    
    def rm_fat_item(self, dfs_path):
        local_path = name_node_dir + dfs_path
        response = pd.read_csv(local_path)
        os.remove(local_path)
        return response.to_csv(index=False)
    
    def format(self):
        format_command = "rm -rf {}/*".format(name_node_dir)
        os.system(format_command)
        return "Format namenode successfully~"


def name_node_run():
    # 创建NameNode并启动
    name_node = NameNode()
    name_node.run()

def heart_beat():
    dead_host_list=[]
    live_host_list= host_list
    while True:
        for host in live_host_list:
            try:
                sock = socket.socket()
                sock.connect((host, data_node_port))
                request = "Still alive?"
                sock.send(bytes(request, encoding='utf-8'))
                ans = sock.recv(4096)
                print(ans)
                continue
            except Exception as e:  # data_node挂掉后对丢失的文件做备份
                pass
                # print(e)
            fat_path = "~/EXP2/MyDFS/dfs/name/test.txt"
            fat = pd.read_csv(fat_path)
            for idx, row in fat.iterrows():
                if row['host_name'] == host:  # 找到挂掉的host所在行
                    blk = row['blk_no']  # 挂掉的host存的块号
                    # 再遍历一遍，找到相同的块号且活着的节点，把该块数据down到本地
                    store_blk = [host]  # 代表存blk数据的host有哪些
                    for idx2, row2 in fat.iterrows():
                        if row2['blk_no'] == blk and row2['host_name'] != host:
                            alive_host = row2['host_name']
                            store_blk.append(alive_host)
                            down_cmd = "scp -r "+str(alive_host)+":~/EXP2/MyDFS/dfs/data/test.txt.blk"+str(blk) \
                                + " ~/EXP2/MyDFS/dfs/data/temp/"
                            os.system(down_cmd)
                    # 把数据发到活着的节点
                    back_up_host = np.random.choice([x for x in live_host_list if x not in store_blk], 
                                    size=1, replace=False)
                    up_cmd = "scp -r "+"~/EXP2/MyDFS/dfs/data/temp/test.txt.blk"+str(blk)+" "\
                        +str(back_up_host[0])+":~/EXP2/MyDFS/dfs/data/"
                    os.system(up_cmd)
                    # 修改FAT表这一行，并更新本地存储
                    row['host_name'] = back_up_host[0]
                    fat.to_csv("~/EXP2/MyDFS/dfs/name/heartbeat_fat.txt", index=False)
            dead_host_list.append(host)
            print(host,"is dead. Data are backed up in",back_up_host)
        # 把dead的节点从host_list里删除，并开始下一轮HeartBeat
        live_host_list = [x for x in live_host_list if x not in dead_host_list]
        time.sleep(0.2)



thread1 = threading.Thread(target=name_node_run)
thread2 = threading.Thread(target=heart_beat)

thread1.start()
thread2.start()

