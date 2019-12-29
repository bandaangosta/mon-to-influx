import redis

class CacheHandler:
    def __init__(self, host):
        self.cacheConn = redis.Redis(host)

    def __del__(self):
        pass

    def getKey(self, key):
        return self.cacheConn.get(key)

    def setKey(self, key, value):
        return self.cacheConn.set(key, value)

    def setKeyExpire(self, key, value, expireSecs):
        self.cacheConn.set(key, value)
        return self.cacheConn.expire(key, expireSecs)

    def getTtlHrs(self, key):
        ttl = self.cacheConn.ttl(key)
        if ttl in [-1, -2]: # [no expire, key does not exist]
            return None
        else:
            return ttl / 60 / 60

    # Warning: consider an alternative approach in production environments with large databases
    #          See https://redis.io/commands/keys
    def delKeys(self, baseKey):
        keysToDel = [x.decode('utf-8') for x in self.cacheConn.keys() if str(baseKey) in str(x)]
        for key in keysToDel:
            self.cacheConn.delete(key)
            print('Deleted key {}'.format(key))