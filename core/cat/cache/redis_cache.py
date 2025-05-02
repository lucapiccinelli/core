from cat.cache.base_cache import BaseCache
from cat.cache.cache_item import CacheItem

import pickle
import redis


class RedisCache(BaseCache):
    def __init__(self, host="localhost", port=6379, db=0):
        self._redis = redis.Redis(host=host, port=port, db=db)

    @property
    def keep_in_synch(self) -> bool:
        return True

    def insert(self, cache_item: CacheItem):
        encoded_item = pickle.dumps(cache_item)
        self._redis.set(
            cache_item.key,
            encoded_item,
        )

    def get_item(self, key) -> CacheItem:
        cache_item = self._get_and_decode(key)
        if not cache_item:
            return None

        if cache_item.is_expired():
            self.delete(key)
            return None

        return cache_item

    def get_value(self, key):
        cache_item = self._get_and_decode(key)
        if not cache_item:
            return None

        return cache_item.value

    def delete(self, key):
        print(f"clearing my conversation for {key}")
        self._redis.delete(key)

    def _get_and_decode(self, key) -> CacheItem | None:
        encoded_item = self._redis.get(key)
        if encoded_item:
            cache_item = pickle.loads(encoded_item)
            return cache_item
        else:
            return None
