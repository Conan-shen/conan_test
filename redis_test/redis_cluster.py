from redis._compat import xrange
from rediscluster import StrictRedisCluster

startup_nodes = [{"host": "127.0.0.1", "port": "6379"},
                 {"host": "127.0.0.1", "port": "6380"}]

# r = RedisCluster(host="127.0.0.1", port=6379, decode_responses=True, )
my_host = "172.19.24.63"
r = StrictRedisCluster(host=my_host,
                 port=6379, decode_responses=True)
pipe = r.pipeline(transaction=False)
pipe.set("apple", "ttt")
pipe.get("apple")
res = pipe.execute()
print("xxxx", res)




# for i in xrange(10):
#     d = str(i)
#     pipe = r.pipeline(transaction=False)
#     pipe.set(d, d)
#     pipe.incrby(d, 1)
#     pipe.execute()
#
#     pipe = r.pipeline(transaction=False)
#     pipe.set("foo-{0}".format(d), d)
#     pipe.incrby("foo-{0}".format(d), 1)
#     pipe.set("bar-{0}".format(d), d)
#     pipe.incrby("bar-{0}".format(d), 1)
#     pipe.set("bazz-{0}".format(d), d)
#     pipe.incrby("bazz-{0}".format(d), 1)
#     pipe.execute()

import encodings
Blo