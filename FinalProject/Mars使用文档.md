# Mars使用文档

https://docs.pymars.org/zh_CN/latest/installation/deploy.html#deploy

## 1. VM运行Mars

### 1.1 VM配置

1. 这是个分布式ML的平台，所以起码开三台VM。

2. 每台VM配两个网卡，一个NAT，一个内部网络。

3. 对每个内部网络网卡设一下ip，保证在同一网段。

   ```shell
   sudo ifconfig enp0s8 192.168.0.x
   ```

4. 配完互相ping一下，防止没生效。

### 1.2 安装&启动步骤

1. 在VM上装好conda、虚拟环境

2. 每台机子上都装mars（conda找不到这个库）

   ```python
   pip install pymars
   ```

3. 选择一台作为Supervisor机器，另外的是Worker机器。

4. 启动Supervisor

   ```shell
   mars-supervisor -H <Supervisor_ip> -p <supervisor_port> -w <web_port>
   ```

​		比如我用的。后两个port应该是随便设

```shell
mars-supervisor -H 192.168.0.1 -p 4316 -w 8909
```

此时命令行显示“Mars Web started at 0.0.0.0:8909”

5. 启动Worker

   ```shell
   mars-worker -H <host_name> -p <worker_port> -s <supervisor_ip>:<supervisor_port>
   ```

​		我用的

```shell
mars-worker -H 192.168.0.2 -p 7869 -s 192.168.0.1:4316
```

### 1.3 运行

在Supervisor上再开个terminal，跑python文件就行了，但要用mars库来创建文件，比如

```python
import mars
import mars.tensor as mt
import mars.dataframe as md
# create a default session that connects to the cluster
mars.new_session('http://<web_ip>:<web_port>')  # 表现为该网站上多了个session
a = mt.random.rand(2000, 2000, chunk_size=200)
b = mt.inner(a, a)
b.execute()  # submit tensor to cluster
df = md.DataFrame(a).sum()
df.execute()  # submit DataFrame to cluster
```

### 1.4 查看结果

打开一个浏览器窗口，输入 `http://<web_ip>:<web_port>` 来打开 Mars UI。我用的http://0.0.0.0:8909

## 2. 一些特点/接口

1. 系统架构：一个Supervisor，若干Worker。Supervisor只负责将图构建出来，没有Worker就无法执行，progress一直是0%
2. worker可以指定使用CPU核的数量，用--n-cpu
3. worker中用于网络传输的进程数，默认为1  --n-io-process
4. worker指定使用的CUDA设备的序号，默认都用  --cuda-devices

4. Mars 默认延迟执行，需要调用 `.execute()` 方法来触发计算，不过，Mars 也支持 [Eager 执行](https://docs.pymars.org/zh_CN/latest/getting_started/eager.html#eager-mode)，如果打开了 eager 模式，每次创建 tensor、DataFrame 等 Mars 对象的时候，会自动触发执行。

5. 可以异步执行

   ```python
   import mars.tensor as mt
   >>> a = mt.random.rand(100, 10)
   >>> future = a.mean().execute(wait=False)
   >>> future.done()
   True
   >>> future.result()
   0.49123541512823077
   ```

6. [GPU执行](https://docs.pymars.org/zh_CN/latest/getting_started/gpu.html)

   Worker可以直接绑定在GPU上







## 3. 服务器使用Mars

### 3.1 端口映射

为了在本地看到UI呈现，需要将docker里的端口映射到宿主机（即gnode1）再映射到本地。

### 3.1.1 docker-->gnode1

在从image run的时候用-p指令

sudo docker run -it -p 4316:8909 7c3a1c75d307 /bin/bash   将docker的8909端口映射到宿主机的4316端口

mars-supervisor -w 8080 docker里开mars时要指定web port与上一条一致

### 3.1.2 gnode1-->本地

将gnode1的4316端口映射到本地的4316端口，浏览器输入http://localhost:4316/就行

ssh -p 8001 yxue@10.103.10.151 -L 4316:localhost:4316 



## 4. 实验指令

1. 新建supervisor容器 `sudo docker run --network mars --name="mars_supervisor" -it -p 4316:8909 72978e886a05 /bin/bash`
2. 新建worker容器 `sudo docker run --network mars --name="mars_worker" -it 72978e886a05 /bin/bash`
3. supervisor运行：`mars-supervisor -H 172.18.0.2 -p 4316 -w 8909`
4. worker运行：`mars-worker -H 172.18.0.3 -p 7869 -s 172.18.0.2:4316`





## 5. Bugs

1. DashBoard中CPU Usage计算方法为`resourceStats.cpu_total - resourceStats.cpu_avail` ，即CPU总量-available的CPU量。如果给VM分配4个核，指定worker用3个，初始CPU Usage显示为-1，Total为3（代码指定的值），得`cpu_avail=4`，说明cpu_avail探测的可能是该VM所有的核数量。

   ----

   任务跑起来是2.7+/3，由于total=3恒定，所以此时的cpu_avail=0.3，而到底用了==2.7个还是3.7==呢（个人觉得是3.7个），可以测运行时间看看。这是==有问题==的，说明这条命令并没有用，仍然用的是所有的核。

   * 由6-1测试结果得知，worker一直用的4个核。

   ----

   

## 6. Measurement

1. 对应bug1，观察worker是否一直用VM所有的核

   * 测试方案：给VM分配4个核，worker的命令--n-cpu分别设为1、2、3、4、5、6，观察时间是否一致，如果四者一致那可能说明确实用的四个核。（supervisor分配1 core）

   下午跑的==真数据==：

   | num_core |    1    |    2    |    3    |    4    |    5    |    6    |
   | :------: | :-----: | :-----: | :-----: | :-----: | :-----: | :-----: |
   |    1#    | 34.7943 | 32.1567 | 33.3097 | 35.2492 | 35.0469 | 35.8451 |
   |    2#    | 35.1274 | 32.9362 | 36.3087 | 34.9391 | 35.2659 | 36.1214 |
   |    3#    | 35.1407 | 33.8585 | 35.7395 | 34.6190 | 37.5406 | 36.8275 |
   |    4#    | 34.8862 | 33.5506 | 33.8338 | 36.2131 | 35.6113 | 35.4202 |
   |    5#    | 36.6292 | 34.0463 | 35.9189 | 35.5458 | 35.3613 | 36.7030 |

   也用top指令看了，确实4个核都用上了，即--n-cpu是没用的worker参数。	

   

   早上跑的==假数据==：

   | num_core |    1    |    2    |    3    |    4    |    5    | 6       |
   | :------: | :-----: | :-----: | :-----: | :-----: | :-----: | :------ |
   |    1#    | 44.2777 | 53.1749 | 55.8961 | 55.6090 | 57.2059 | 56.6778 |
   |    2#    | 44.7802 | 53.5689 | 56.2123 | 54.9151 | 57.8765 | 56.3676 |
   |    3#    | 47.7167 | 54.4620 | 56.4651 | 56.2586 | 56.7681 |         |
   |    4#    | 44.6201 | 55.6790 | 57.8119 | 56.2923 | 56.6975 |         |
   |    5#    | 45.9046 | 53.7909 | 57.5248 | 57.3715 | 56.6784 |         |

​		但==为什么指定一个core的时间更短？==

2. 实验1的集群是一个supervisor（1 core）一个worker（4 cores），发现supervisor好像不干活，worker的CPU Usage整挺高的。所以想测试supervisor到底干不干活，以及给supervisor更多的CPU资源会不会加速整体的执行。

   * 测试方案：给supervisor分配1～4cores，观察时间和CPU Usage。worker不指定--n-cpu命令，即全用4 cores。

   | num_core |    1    |    2    |    3    |    4    |
   | :------: | :-----: | :-----: | :-----: | :-----: |
   |    1#    | 36.8431 | 38.9556 | 39.0354 | 36.3359 |
   |    2#    | 38.8646 | 36.8601 | 38.9937 | 35.7548 |
   |    3#    | 37.8384 | 37.1894 | 37.4851 | 36.7326 |
   |    4#    | 37.6913 | 37.5820 | 37.6705 | 36.5255 |
   |    5#    | 37.1199 | 37.1561 | 37.5709 | 35.9645 |

   * supervisor的CPU Usage一直在0～2之间（num_core > 1），当num_core=1时，其一直在0～1之间
   * 结论：supervisor的core增加但时间不变，且没有指定使用多少核的cmd参数，且CPU Usage较低。怀疑它可能是不作为一个worker，只是个调度节点。
