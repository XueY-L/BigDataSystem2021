# Docker 命令

## image

1. 查看服务器内所有镜像 `sudo docker image ls`

2. 下载镜像 `docker pull <image_name>:<tag>`



## container

1. 查看正在运行的container `sudo docker container ls`

2. 查看所有container `sudo docker container ls -a`
3. 从image创建container `sudo docker run -it <image_name>:<tag> /bin/bash `
4. 退出container: Ctrl+D/`exit`
5. 重启container `sudo docker start <container_id>`
6. 进入container `sudo docker exec -it 97f7e919dd8c /bin/bash`
   * terminal多开也用这个命令，拿到container_ID就行

7. 每次重新进入container，ID都会变

8. 

##  container间互通

### container装ifconfig、ping

解决报错`E: Unable to locate package xxx` 

用`apt-get update`同步 /etc/apt/sources.list 和 /etc/apt/sources.list.d 中列出的源的索引，这样才能获取到最新的软件包。

再执行`sudo apt-get install net-tools`，就能用ifconfig了

再用 `apt install iputils-ping` 装一下ping

### 新建bridge网络

1. 查看现在的网络 `docker network ls `
2. 创建 `docker network create -d bridge <network_name>`

3. 在实例化容器时指定bridge `docker run --network <network_name> -it <image_name>:<tag> `

   在同一个bridge里的容器间会相互连接，可以ping