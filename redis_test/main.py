import redis
import time


class Monitor():
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection = None

    def __del__(self):
        try:
            self.reset()
        except:
            pass

    def reset(self):
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def monitor(self):
        if self.connection is None:
            self.connection = self.connection_pool.get_connection(
                'monitor', None)
        self.connection.send_command("monitor")
        return self.listen()

    def parse_response(self):
        return self.connection.read_response()

    def listen(self):
        while True:
            yield self.parse_response()


if __name__ == '__main__':
    # redis_client = redis.StrictRedis.from_url('redis://wishpost-stage.u1td7d.ng.0001.cnn1.cache.amazonaws.com.cn:6379/0')
    pool = redis.ConnectionPool(host='wishpost-stage.u1td7d.ng.0001.cnn1.cache.amazonaws.com.cn', port=6379, db=0)
    monitor = Monitor(pool)
    commands = monitor.monitor()

    for c in commands:
        print(c)
