from .filetree import *
from .utils import *
import hashlib

class NameServer:
    def __init__(self, numReplicate):
        self.dataServers = []  # 存储数据服务器
        self.fileTree = FileTree()  # 文件树
        self.numReplicate = numReplicate  # 复制数
        self.idCnt = 0  # 文件ID计数
        self.meta = dict()

    def add(self, server):
        self.dataServers.append(server)

    def parse_cmd(self):
        cmd = input("MiniDFS> ")  
        parameters = cmd.split()  # 使用 split() 方法将命令行分割成参数列表
        return parameters

    def handle_put(self, parameters):
        if len(parameters) != 3:
            print("usage: put source_file_path des_file_path")
            return

        try:
            # print(parameters[1])
            with open(parameters[1], 'rb') as is_file:
                buf = is_file.read()
        except FileNotFoundError:
            print(f"open file error: file {parameters[1]}")
            return

        if self.fileTree.insert_node(parameters[2], True) is False:
            print(f"create file error \n.maybe the file: {parameters[2]} exists")
            return

        total_size = len(buf)
        server_sizes = [server.size for server in self.dataServers]
        idx = argsort(server_sizes)
        id_count = self.idCnt
        
        for i in range(self.numReplicate):
            with self.dataServers[idx[i]].mutex:
                self.meta[parameters[2]] = (id_count + 1, total_size)
                self.dataServers[idx[i]].cmd = "put"
                self.dataServers[idx[i]].fid = id_count + 1
                self.dataServers[idx[i]].bufSize = total_size
                self.dataServers[idx[i]].buf = buf
                self.dataServers[idx[i]].finish = False
                self.dataServers[idx[i]].cv.notify_all()
        self.idCnt += 1

    def handle_read_fetch(self, parameters):
        if len(parameters) != 3 and len(parameters) != 4:
            print("usage: read source_file_path dest_file_path")
            print("usage: fetch FileID Offset dest_file_path")
            return

        if parameters[0] == "read" and parameters[1] not in self.meta:
            print("error: no such file in miniDFS.")
            return
        # print("in read / fetch function")
        for i in range(4):
            with self.dataServers[i].mutex:
                self.dataServers[i].cmd = parameters[0]
                if parameters[0] == "read":
                    meta_data = self.meta[parameters[1]]
                    self.dataServers[i].fid = meta_data[0]
                    self.dataServers[i].buf_size = meta_data[1]
                else:
                    self.dataServers[i].fid = int(parameters[1])
                    self.dataServers[i].offset = int(parameters[2])
                self.dataServers[i].finish = False
                self.dataServers[i].cv.notify_all()
        # print("out handle read")

    def handle_locate(self, parameters):
        if len(parameters) != 3:
            print("usage: locate fileID Offset")
            return

        for i in range(4):
            with self.dataServers[i].mutex:
                self.dataServers[i].cmd = "locate"
                self.dataServers[i].fid = int(parameters[1])
                self.dataServers[i].offset = int(parameters[2])
                self.dataServers[i].finish = False
                self.dataServers[i].cv.notify_all()

    def run(self):
        while True:
            parameters = self.parse_cmd()

            if not parameters:
                print("input a blank line")
                continue

            if parameters[0] == "quit":
                # for i in range(4):
                #     self.dataServers[i].mutex.release()
                exit(0)
            elif parameters[0] in ("list", "ls"):
                if len(parameters) != 1:
                    print("usage: list (list all the files in name server)")
                else:
                    print("file\tFileID\tChunkNumber")
                    self.fileTree.list_all(self.meta)
                continue
            elif parameters[0] == "put":
                self.handle_put(parameters)
            elif parameters[0] in ("read", "fetch"):
                self.handle_read_fetch(parameters)
                
            elif parameters[0] == "locate":
                self.handle_locate(parameters)
                
            else:
                print("wrong command.")

            # print("finish cmd parse episode 1")
            for server in self.dataServers:
                with server.mutex:
                    server.cv.wait_for(lambda: server.finish)
                    server.cv.notify_all()
            
            if parameters[0] in ["read", "fetch"]:
                md5 = hashlib.md5()
                pre_checksum = None
                for i in range(4):
                    if self.dataServers[i].bufSize:
                        if parameters[0] == "read":
                            file_path = parameters[2]
                        elif parameters[0] == "fetch":
                            file_path = parameters[3]
                        
                        try:
                            print("For Output filepath: " + file_path)
                            with open(file_path, 'wb') as os_file:
                                os_file.write(self.dataServers[i].buf[:self.dataServers[i].bufSize])
                            md5.update(self.dataServers[i].buf[:self.dataServers[i].bufSize])
                            md5_checksum = md5.hexdigest()
                            if not pre_checksum:
                                pre_checksum = md5_checksum
                            else:
                                if pre_checksum != md5_checksum:
                                    print(pre_checksum, md5_checksum)
                                    print("error: unequal checksum for files from different dataServers. File got may be wrong.")
                                    pre_checksum = md5_checksum

                        except FileNotFoundError:
                            print("create file failed. maybe wrong directory.")
                        
                        del self.dataServers[i].buf
                # print(pre_checksum)
            elif parameters[0] == "put":
                print(f"Upload success. The file ID is {self.idCnt}")

            elif parameters[0] in ("locate", "ls"):
                not_found = True
                for i in range(4):
                    if self.dataServers[i].bufSize:
                        not_found = False
                        print(f"found FileID {parameters[1]} offset {parameters[2]} at {self.dataServers[i].get_name()}")
                if not_found:
                    print(f"not found FileID {parameters[1]} offset {parameters[2]}")

