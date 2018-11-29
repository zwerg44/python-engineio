import pickle
from redis_collections import Dict as RDict
from . import socket


class RedisDict(RDict):
    #TODO: redis-collections here for experements
    # wil be reimplemented
    def __init__(self, *args, **kwargs):
        self.__key = kwargs['key']
        super(RedisDict, self).__init__(*args, **kwargs)

    def bind_server(self, server):
        self.eio_server = server

    def __getitem__(self, key):
        value = super(RedisDict, self).__getitem__(key)
        if isinstance(value, socket.Socket):
            value.bind_server(self.eio_server)
        # print('__getitem__', self.__key, key, value)
        return value

    def __setitem__(self, key, value):
        super(RedisDict, self).__setitem__(key, value)
        # print('__setitem__', self.__key, key, value)

    def __del__(self):
        if hasattr(self, 'eio_server'):
            del self.eio_server

def is_picklable(obj):
    try:
        pickle.dumps(obj)
    except pickle.PicklingError:
        return False
    except TypeError:
        return False
    return True

def stored_dict(url=None, key=None, server=None, **kwargs):
    if url and url.startswith(('redis://', "rediss://")):
        import redis
        rdict = RedisDict(
            redis=redis.Redis.from_url(url),
            key=key,
        )
        if server:
            rdict.bind_server(server)
        return rdict
    else:
        return {}


