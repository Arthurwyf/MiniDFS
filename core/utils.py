
def split(s, delim):
    res = [substr for substr in s.split(delim) if substr]
    return res

def argsort(vec):
    idx = list(range(len(vec)))
    idx.sort(key=lambda i: vec[i])
    return idx
