from rediscluster import StrictRedisCluster
my_host = "stage-redis-cluster.u1td7d.clustercfg.cnn1.cache.amazonaws.com.cn"
nodes = [{"host": my_host, "port": "6379"}]
r = StrictRedisCluster(startup_nodes=nodes,
                       port=6379,
                       skip_full_coverage_check=True)

r.keys("*")
r.smove()
