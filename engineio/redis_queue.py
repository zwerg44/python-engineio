import sys
import time
import pickle
try:
    import redis
except ImportError:
    redis = None
from . import exceptions
from . import packet

if sys.version_info[0] == 2:
    import Queue as __queue__ # python 3: pylint:disable=import-error
else:
    import queue as __queue__ # python 2: pylint:disable=import-error
Full = __queue__.Full
Empty = __queue__.Empty


class RedisQueue(object):
    """Simple Queue with Redis Backend"""
    def __init__(self, sid, url):
        self.key = 'queue:%s' % sid
        self.redis = redis.StrictRedis.from_url(url)

    def qsize(self):
        """Return the approximate size of the queue."""
        return self.redis.llen(self.key)

    def empty(self):
        """Return True if the queue is empty, False otherwise."""
        return self.qsize() == 0

    def put(self, item):
        """Put item into the queue."""
        pickled = pickle.dumps(item, protocol=pickle.HIGHEST_PROTOCOL)
        self.redis.rpush(self.key, pickled)

    def get(self, block=True, timeout=None):
        """
        Remove and return an item from the queue.
        If optional args block is true and timeout is None (the default), block
        if necessary until an item is available.
        """
        if block:
            try:
                item = self.redis.blpop(self.key, timeout=timeout)
            except redis.TimeoutError:
                raise Empty
            if item:
                item = item[1]
        else:
            item = self.redis.lpop(self.key)
            if item is None:
                raise Empty

        if item is not None:
            pkt = pickle.loads(item)
            return pkt
        else:
            return None

    def get_nowait(self):
        """Equivalent to get(False)."""
        return self.get(False)

    def task_done(self):
        """
        We are calling this after get()
        So nothing to do here
        """
        pass

    def join(self):
        """Block thread until empty()"""
        while not self.empty():
            time.sleep(0.2)
