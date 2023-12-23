import os
import threading
import queue

chunkSize = 2 * 1024 * 1024

class DataServer:
    def __init__(self, name):
        self.name = name
        self.size = 0
        self.mutex = threading.Lock()
        self.cv = threading.Condition(self.mutex)
        self.cmd = ""
        self.fid = 0
        self.bufSize = 0
        self.offset = 0
        self.buf = None
        self.finish = True
        os.system("mkdir -p " + self.name)

    def run(self):
        while True:
            with self.mutex:
                self.cv.wait_for(lambda: not self.finish)
                # print("dataserver begin process")
                if self.cmd == "put":
                    self.size += self.bufSize / 1024.0 / 1024.0
                    self.put()
                elif self.cmd == "read":
                    self.read()
                elif self.cmd == "locate":
                    self.locate()
                elif self.cmd == "fetch":
                    self.fetch()
                self.finish = True
                self.cv.notify_all()
                # print("here in dataserver run")

    def put(self):
        start = 0
        # print(start, self.bufSize)
        while start < self.bufSize:
            offset = start // chunkSize
            file_path = f"{self.name}/{self.fid} {offset}"
            # print("dataserver" + self.name + file_path)
            with open(file_path, 'wb') as os_file:
                if not os_file:
                    print(f"create file error in dataserver: (file name) {file_path}")
                os_file.write(self.buf[start:start + chunkSize])
            start += chunkSize
        
    def read(self):
        start = 0
        self.buf = bytearray(self.bufSize)
        while start < self.bufSize:
            offset = start // chunkSize
            file_path = f"{self.name}/{self.fid} {offset}"
            try:
                with open(file_path, 'rb') as is_file:
                    data = is_file.read(chunkSize)
                    if not data:
                        del self.buf
                        self.bufSize = 0
                        break
                    self.buf[start:start + len(data)] = data
                    start += len(data)
                # print("dataserver: " + self.name + " " + file_path)
            except FileNotFoundError:
                del self.buf
                self.bufSize = 0
                break
        # print("finish read function in dataserver")

    def fetch(self):
        self.buf = bytearray(chunkSize)
        file_path = f"{self.name}/{self.fid} {self.offset}"
        try:
            with open(file_path, 'rb') as is_file:
                data = is_file.read(chunkSize)
                if not data:
                    del self.buf
                    self.bufSize = 0
                else:
                    self.buf[:len(data)] = data
                    self.bufSize = len(data)
        except FileNotFoundError:
            del self.buf
            self.bufSize = 0

    def locate(self):
        file_path = f"{self.name}/{self.fid} {self.offset}"
        try:
            with open(file_path, 'rb'):
                self.bufSize = 1
        except FileNotFoundError:
            self.bufSize = 0

    def get_name(self):
        return self.name
