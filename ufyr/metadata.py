from redis import StrictRedis

class MetadataBase(object):
    def __init__(self, **kwargs):
        self.r = StrictRedis(**kwargs)
        
        #self.key_base = 'ufyr:%s'  ##Should probably be subclassed
        self._get_meta_key = lambda x: self.key_base%x
        self._error_key = self._get_meta_key('err:%s')
        self._get_error_key = lambda x: self._error_key%x
        self._lockout_key = self._get_meta_key('lk:%s')
        self._get_lockout_key = lambda x: self._lockout_key%x
    
    @property
    def key_base(self):
        return 'ufyr:%s'

    def set_metadata(self, key, dict_val):
        self.r.hmset(self._get_meta_key(key), dict_val)
        
    def get_metadata(self, key ):
        return self.r.hgetall(self._get_meta_key(key))
    
    def delete_metadata(self, key):
        return self.r.delete(self._get_meta_key(key))
    
    
    def set_lockout(self, key, expire=7200):
        key = self._get_lockout_key(key)
        #raise Exception('FUCK OFF!')
        #import pdb; pdb.set_trace()
        self.r.set(key, 1)
        self.r.expire(key, expire)

    def remove_lockout(self, key):
        self.r.delete(self._get_lockout_key(key))
    
    def is_locked_out(self, key):
        return bool(self.r.keys(self._get_lockout_key(key)))
    
    def set_error(self, key, val=''):
        current_error = self.r.get(self._get_error_key(key)) or ''
        new_error = current_error + '\n' + val
        self.r.set(self._get_error_key(key), new_error)
    
    def get_error(self, key):
        return self.r.get(self._get_error_key(key)) or ''
    
    def clear_error(self, key):
        self.r.delete(self._get_error_key(key))
        
    def monitor(self):
        keys = self.r.keys(self._get_meta_key('*'))
        ret = {}
        for key in keys:
            if ':err:' in key or ':lk:' in key:
                continue
            ret_key = ':'.join(key.split(':')[1:])
            ret[ret_key] = self.r.hgetall(key)
            
        return ret
    
    def monitor_lockout(self):
        keys = self.r.keys(self._get_lockout_key('*'))
        ret = {}
        for key in keys:
            ret_key = key.lstrip(self._get_lockout_key(''))
            ret[ret_key] = self.r.ttl(key)
        
        return ret
    
    def monitor_errors(self):
        keys = self.r.keys(self._get_error_key('*'))
        ret = {}
        for key in keys:
            ret_key = key.lstrip(self._get_error_key(''))
            ret[ret_key] = self.r.get(key)
        
        return ret