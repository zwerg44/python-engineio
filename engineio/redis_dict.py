import six
import pickle
import logging
try:
    import redis
except ImportError:
    redis = None
try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc
from . import socket

default_logger = logging.getLogger('engineio.redis_dict')

_DEBUG_REDIS_DICT = True

if _DEBUG_REDIS_DICT:
    default_logger.setLevel(logging.DEBUG)


class RedisDict(collections_abc.MutableMapping):
    '''Dictionary with Redis Backend (based on redis-collections Dict)'''
    def __init__(self, url, key):
        self.redis = redis.StrictRedis.from_url(url)
        self.key = key
        self.pickle_protocol = pickle.HIGHEST_PROTOCOL
        self.logger = default_logger

    def _pickle(self, data):
        return pickle.dumps(data, protocol=self.pickle_protocol)

    def _unpickle(self, pickled_data):
        return pickle.loads(pickled_data) if pickled_data else None

    def _same_redis(self, other):
        cls = self.__class__
        if not isinstance(other, cls):
            return False

        self_kwargs = self.redis.connection_pool.connection_kwargs
        other_kwargs = other.redis.connection_pool.connection_kwargs

        return (
            self_kwargs.get('host') == other_kwargs.get('host') and
            self_kwargs.get('port') == other_kwargs.get('port') and
            self_kwargs.get('path') == other_kwargs.get('path') and
            self_kwargs.get('db', 0) == other_kwargs.get('db', 0)
        )

    def _transaction(self, fn, *extra_keys):
        results = []

        def trans(pipe):
            results.append(fn(pipe))

        self.redis.transaction(trans, self.key, *extra_keys)
        return results[0]

    def __len__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        return pipe.hlen(self.key)

    def __iter__(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        for k, v in six.iteritems(self._data(pipe)):
            yield k

    def __contains__(self, key):
        pickled_key = self._pickle(key)
        return bool(self.redis.hexists(self.key, pickled_key))

    def __eq__(self, other):
        if not isinstance(other, collections_abc.Mapping):
            return False

        def eq_trans(pipe):
            self_items = self._iteritems(pipe)
            other_items = other.items(pipe) if use_redis else other.items()
            return dict(self_items) == dict(other_items)

        if self._same_redis(other):
            use_redis = True
            return self._transaction(eq_trans, other.key)
        else:
            use_redis = False
            return self._transaction(eq_trans)

    def __getitem__(self, key):
        pickled_key = self._pickle(key)
        pickled_value = self.redis.hget(self.key, pickled_key)
        if pickled_value is None:
            raise KeyError(key)
        value = self._unpickle(pickled_value)
        self.logger.debug('RedisDict: __getitem__ %s %s ' % (key, value))
        # print('__getitem__', key, value)
        return value

    def __setitem__(self, key, value):
        self.logger.debug('RedisDict: __setitem__ %s %s ' % (key, value))
        # print('__setitem__', key, value)
        pickled_key = self._pickle(key)
        pickled_value = self._pickle(value)
        self.redis.hset(self.key, pickled_key, pickled_value)

    def __delitem__(self, key):
        pickled_key = self._pickle(key)
        deleted_count = self.redis.hdel(self.key, pickled_key)
        if not deleted_count:
            raise KeyError(key)

    def _data(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        items = six.iteritems(pipe.hgetall(self.key))

        return {self._unpickle(k): self._unpickle(v) for k, v in items}

    def items(self, pipe=None):
        return list(self._iteritems(pipe))

    def _iteritems(self, pipe=None):
        pipe = self.redis if pipe is None else pipe
        for k, v in six.iteritems(self._data(pipe)):
            yield k, v

    def keys(self):
        return list(self.__iter__())

    def values(self):
        return [v for k, v in self._iteritems()]

    def pop(self, key, default=None):
        pickled_key = self._pickle(key)

        def pop_trans(pipe):
            pickled_value = pipe.hget(self.key, pickled_key)
            if pickled_value is None:
                if default is None:
                    raise KeyError(key)
                return default

            pipe.hdel(self.key, pickled_key)
            return self._unpickle(pickled_value)

        value = self._transaction(pop_trans)
        return value

    def popitem(self):
        def popitem_trans(pipe):
            try:
                pickled_key = pipe.hkeys(self.key)[0]
            except IndexError:
                raise KeyError
            pipe.multi()
            pipe.hget(self.key, pickled_key)
            pipe.hdel(self.key, pickled_key)
            pickled_value, __ = pipe.execute()
            return (
                self._unpickle(pickled_key), self._unpickle(pickled_value)
            )

        key, value = self._transaction(popitem_trans)
        return key, value

    def setdefault(self, key, default=None):
        def setdefault_trans(pipe):
            pickled_key = self._pickle(key)
            pipe.multi()
            pipe.hsetnx(self.key, pickled_key, self._pickle(default))
            pipe.hget(self.key, pickled_key)
            __, pickled_value = pipe.execute()
            return self._unpickle(pickled_value)

        value = self._transaction(setdefault_trans)
        return value

    def _update_helper(self, other, use_redis=False):
        def _update_helper_trans(pipe):
            data = {}

            if isinstance(other, RedisDict):
                data.update(other.iteritems(pipe))
            else:
                data.update(other)

            pickled_data = {}
            while data:
                k, v = data.popitem()
                pickled_data[self._pickle(k)] = self._pickle(v)

            if pickled_data:
                pipe.hmset(self.key, pickled_data)

        if use_redis:
            self._transaction(_update_helper_trans, other.key)
        else:
            self._transaction(_update_helper_trans)

    def update(self, other=None, **kwargs):
        if other is not None:
            if self._same_redis(other):
                self._update_helper(other, use_redis=True)
            elif hasattr(other, 'keys'):
                self._update_helper(other)
            else:
                self._update_helper({k: v for k, v in other})

        if kwargs:
            self._update_helper(kwargs)

    def copy(self):
        return dict(self).copy()

    def clear(self):
        self.redis.delete(self.key)

    def __repr__(self):
        cls_name = self.__class__.__name__
        data = self._repr_data()
        return '<redis_dict.%s at %s %s>' % (cls_name, self.key, data)

    def _repr_data(self):
        items = ('{}: {}'.format(repr(k), repr(v)) for k, v in self.items())
        return '{{{}}}'.format(', '.join(items))


class Environ(dict):
    '''Dictionary like object, autodumps to Redis on modification'''
    def __init__(self, data, sid, environ_map):
        super(Environ, self).__init__(data)
        self._sid = sid
        self._environ_map = environ_map

    def __setitem__(self, key, value):
        super(Environ, self).__setitem__(key, value)
        self._environ_map[key] = self
    
    def __del__(self):
        del self._sid
        del self._environ_map


class EnvironMap(RedisDict):
    '''RedisDict of Environ'''
    def __init__(self, url=None, key=None):
        key = 'environ'
        super(EnvironMap, self).__init__(url=url, key=key)
        self.__last_unpicklable_environ = {}

    def __getitem__(self, key):
        environ = super(EnvironMap, self).__getitem__(key)
        for key, val in self.__last_unpicklable_environ.items():
            environ[key] = val
        return Environ(environ, key, self)

    def __setitem__(self, key, environ):
        environ_copy = environ.copy()
        last_unpicklable_environ = {}
        for k, env in environ.items():
            if not is_picklable(env):
                # FIXME: don't know what to do with this
                # not_serializable = [
                #    'flask.app', wsgi.errors', 'wsgi.input', 'wsgi.file_wrapper',
                #    'x-wsgiorg.fdevent.readable', 'x-wsgiorg.fdevent.writable',
                # ]
                # For now, keep that stuff from last recieved environ
                # Maybe it is not nessasary at all to keep the whole environment copy per sid
                # It can be obtained from the Engine.IO Socket.handle_request, 
                # It is not used much, but removing of it breaks some concepts (and Flask-SocketIO)
                self.logger.warn('EnvironMap: Not serializable environ - %s' % k)
                # print('EnvironMap: Not serializable environ - %s' % k)
                last_unpicklable_environ[k] = env
                del environ_copy[k]

        self.__last_unpicklable_environ = last_unpicklable_environ
        super(EnvironMap, self).__setitem__(key, environ_copy)


class SocketMap(RedisDict):
    '''RedisDict of Sockets, deserealizes items to Sockets on access'''
    def __init__(self, url=None, key=None, server=None):
        key = 'sockets'
        self.eio_server = server
        super(SocketMap, self).__init__(url=url, key=key)

    def __getitem__(self, key):
        value = super(SocketMap, self).__getitem__(key)
        if value:
            value.bind_server(self.eio_server)
        return value

    def __del__(self):
        del self.eio_server


def is_picklable(obj):
    try:
        pickle.dumps(obj)
    except pickle.PicklingError:
        return False
    except TypeError:
        return False
    except AttributeError:
        return False
    return True
