import os
import socket
import time
from io import StringIO

import pandas as pd

from common import *

class Reducer:
    def run(self):
        # 创建一个监听的socket
        listen_fd = socket.socket()
        try:
            # 监听端口
            listen_fd.bind(("0.0.0.0", reducer_port))
            listen_fd.listen(5)
            while True:
                # 等待连接，连接后返回通信用的套接字
                sock_fd, addr = listen_fd.accept()
                print("Received request from {}".format(addr))
                try:
                    # 获取请求方发送的指令
                    request = str(sock_fd.recv(BUF_SIZE), encoding='utf-8')
                    request = request.split()  # 指令之间使用空白符分割
                    if len(request) != 2:
                        print("Invalid command: "+ " ".join(request))
                    else:
                        print(request)
                        
                        host_name = request[0]  # 指令第一个为指令类型
                        blk_no = request[1]
                        
                        # 根据从cliet.py收到的host_name和blk_no，去对应host的datanode里请求数据
                        data_node_sock = socket.socket()
                        data_node_sock.connect((host_name, data_node_port))
                
                        blk_path = "/FAT.txt.blk{}".format(blk_no)
                        request = "load {}".format(blk_path)
                        data_node_sock.send(bytes(request, encoding='utf-8')) 
                        time.sleep(0.2)  # 两次传输需要间隔⼀段时间，避免粘包
                        # 收到该块数据
                        data = data_node_sock.recv(BUF_SIZE)
                        data = str(data, encoding='utf-8')
                        with open('./temp_data.txt','w') as f:  # 暂存下来再读取，属于是不会怎么把data读成pd了
                            f.write(data)
                        data = pd.read_csv('./temp_data.txt', names=['rdm_data'])
                        # 需要返回该块数据的和、平方和、数量
                        print("块",blk_no,"和",data['rdm_data'].sum())
                        print("块",blk_no,"数量",data['rdm_data'].count())
                        # 不会搞平方和，用笨方法了
                        square_sum=0
                        for index, row in data.iterrows():
                            square_sum += row['rdm_data']**2
                        print("块",blk_no,"平方和",square_sum)

                        # 结果返回到client
                        # 以str的形式发list太麻烦了，改为发三个str（数）
                        # response = [data['rdm_data'].sum(), data['rdm_data'].count(), square_sum]
                        sock_fd.send(bytes(str(data['rdm_data'].sum()), encoding='utf-8'))
                        time.sleep(0.2)  # 两次传输需要间隔⼀段时间，避免粘包
                        sock_fd.send(bytes(str(data['rdm_data'].count()), encoding='utf-8'))
                        time.sleep(0.2)  # 两次传输需要间隔⼀段时间，避免粘包
                        sock_fd.send(bytes(str(square_sum), encoding='utf-8'))

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

# 创建Reducer对象并启动
reducer = Reducer()
reducer.run()