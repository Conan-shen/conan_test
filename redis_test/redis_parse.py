from rdbtools import RdbParser, RdbCallback
from rdbtools.encodehelpers import bytes_to_unicode
from datetime import datetime
import time
import pickle
from rediscluster import StrictRedisCluster
import threading
import sys

# my_host = "stage-redis-cluster.u1td7d.clustercfg.cnn1.cache.amazonaws.com.cn"
rediscluster_host = sys.argv[1]
file_loc = sys.argv[2]
nodes = [{"host": rediscluster_host, "port": "6379"}]
r = StrictRedisCluster(startup_nodes=nodes,
                       port=6379,
                       skip_full_coverage_check=True)

limit = 10


class FuncWithParams(object):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs


funcs = []


def get_ex(expiry):
    if expiry is None:
        return None
    tmp = expiry - datetime(1970, 1, 1)
    sec = tmp.total_seconds()
    now = time.time()
    if now >= sec:
        return None
    return str(int(sec - now))


class MyCallback(RdbCallback):
    ''' Simple example to show how callback works.
        See RdbCallback for all available callback methods.
        See JsonCallback for a concrete example
    '''

    def __init__(self):
        super(MyCallback, self).__init__(string_escape=None)

    def encode_key(self, key):
        return bytes_to_unicode(key, self._escape, skip_printable=True)

    def encode_value(self, val):
        return bytes_to_unicode(val, self._escape)

    def set(self, key, value, expiry, info):
        ex = get_ex(expiry)
        if ex:
            cmd = "set {} {} ex {}".format(key, value, ex)
            # r.set(key, value, ex)
            funcs.append(FuncWithParams(r.set, key, value, ex))
        else:
            cmd = "set {} {}".format(key, value)
            # r.set(key, value, ex)
            funcs.append(FuncWithParams(r.set, key, value))

    def hset(self, key, field, value):
        pass
        # print('%s.%s = %s' % (
        #     self.encode_key(key), self.encode_key(field),
        #     self.encode_value(value)))

    def sadd(self, key, member):
        pass
        # print('%s has {%s}' % (self.encode_key(key), self.encode_value(member)))

    def rpush(self, key, value):
        pass
        # print('%s has [%s]' % (self.encode_key(key), self.encode_value(value)))

    def zadd(self, key, score, member):
        # cmd = "zadd {} {} {}".format(key, score, member)
        # print(cmd)
        # r.zadd(key, score, member)
        funcs.append(FuncWithParams(r.zadd, key, score, member))


start = time.time()
callback = MyCallback()
parser = RdbParser(callback)
parser.parse(file_loc)
end = time.time()

print("cost: ", end - start)
print("len: ", len(funcs))


def work(f):
    f.func(*f.args, **f.kwargs)


for idx, f in enumerate(funcs):
    if idx % 1000 == 0:
        print(idx)
    t = threading.Thread(target=work, args=(f,))
    t.start()
end = time.time()
print("cost: ", end - start)
