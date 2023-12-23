#!/bin/bash

# 启动 Python 程序并将命令发送到其标准输入
python MiniDFS_python/main.py << EOF
put MiniDFS_python/111.txt /copy.txt
list
read /copy.txt /home/arthur/copy111.txt
fetch 1 1 /home/arthur/copy111_1.txt
locate 1 1