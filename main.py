import threading
from core.dataserver import *
from core.nameserver import *

if __name__ == "__main__":
    numReplicate = 3  # 设置复制数目
    ns = NameServer(numReplicate)
    ds1 = DataServer("node1")
    ds2 = DataServer("node2")
    ds3 = DataServer("node3")
    ds4 = DataServer("node4")
    ns.add(ds1)
    ns.add(ds2)
    ns.add(ds3)
    ns.add(ds4)

    th1 = threading.Thread(target=ds1.run)
    th2 = threading.Thread(target=ds2.run)
    th3 = threading.Thread(target=ds3.run)
    th4 = threading.Thread(target=ds4.run)
    th1.start()
    th2.start()
    th3.start()
    th4.start()

    ns.run()
