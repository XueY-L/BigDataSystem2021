import os
import socket

from common import *


# DataNode支持的指令有:
# 1. load 加载数据块
# 2. store 保存数据块
# 3. rm 删除数据块
# 4. format 删除所有数据块

class DataNode:
    def run(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", data_node_port))
            listen_fd.listen(5)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))
                
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    print(request)
                    
                    cmd = request[0]  # 指令第一个为指令类型
                    
                    if cmd == "load":  # 加载数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.load(dfs_path)
                    elif cmd == "store":  # 存储数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.store(sock_fd, dfs_path)
                    elif cmd == "rm":  # 删除数据块
                        dfs_path = request[1]  # 指令第二个参数为DFS目标地址
                        response = self.rm(dfs_path)
                    elif cmd == "format":  # 格式化DFS
                        response = self.format()
                    else:
                        response = "Undefined command: " + " ".join(request)
                    
                    sock_fd.send(bytes(response, encoding='utf-8'))
                except KeyboardInterrupt:
                    break
                finally:
                    sock_fd.close()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            print(e)
        finally:
            listen_fd.close()
    
    def load(self, dfs_path):
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 读取本地数据
        with open(local_path) as f:
            chunk_data = f.read(dfs_blk_size)
        
        return chunk_data
    
    def store(self, sock_fd, dfs_path):
        # 从Client获取块数据
        chunk_data = sock_fd.recv(BUF_SIZE)
        # 本地路径
        local_path = data_node_dir + dfs_path
        # 若目录不存在则创建新目录
        os.system("mkdir -p {}".format(os.path.dirname(local_path)))
        # 将数据块写入本地文件
        with open(local_path, "wb") as f:
            f.write(chunk_data)
        
        return "Store chunk {} successfully~".format(local_path)
    
    def rm(self, dfs_path):
        local_path = data_node_dir + dfs_path
        rm_command = "rm -rf " + local_path
        os.system(rm_command)
        
        return "Remove chunk {} successfully~".format(local_path)
    
    def format(self):
        format_command = "rm -rf {}/*".format(data_node_dir)
        os.system(format_command)
        
        return "Format datanode successfully~"


# 创建DataNode对象并启动
data_node = DataNode()
data_node.run()
