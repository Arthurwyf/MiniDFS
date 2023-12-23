import math

class TreeNode:
    def __init__(self, value, isFile):
        self.value = value
        self.isFile = isFile
        self.parent = None
        self.firstSon = None
        self.nextSibling = None

class FileTree:
    def __init__(self):
        self._root = TreeNode("/", False)

    def insert_node(self, path, isFile):
        parent = [None]
        is_found = self.find_node(path, parent)
        if is_found:
            return False

        path_folders = path.split('/')
        new_node = TreeNode(path, isFile)
        new_node.parent = parent[-1]
        son = parent[-1].firstSon
        if not son:
            parent[-1].firstSon = new_node
        else:
            while son.nextSibling:
                son = son.nextSibling
            son.nextSibling = new_node
        while son:
            son = son.nextSibling
        son = new_node
        return True

    def find_node(self, path, last_node):
        path_folders = path.split('/')
        node = self._root.firstSon
        last_node[-1] = self._root
        for name in path_folders:
            while node and node.value != name:
                node = node.nextSibling
            if not node:
                return False
            last_node[-1] = node
            node = node.firstSon
        return True and node.isFile

    def list(self, node, meta):
        chunk_size = 2 * 1024 * 1024
        if node:
            if node.value in meta:
                print(f"{node.value}\t{meta[node.value][0]}\t{math.ceil(1.0 * meta[node.value][1] / chunk_size)}")
            else:
                # print(f"Key {node.value} not found in meta dictionary")
                pass
            
            self.list(node.firstSon, meta)
            self.list(node.nextSibling, meta)


    def list_all(self, meta):
        self.list(self._root, meta)
