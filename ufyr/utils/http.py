#! /usr/bin/python

import json

import requests

class Callback(object):
    def __init__(self, url, method='GET', req_kwargs={}, **kwargs):
        
        assert isinstance(url, (str, unicode))
        assert isinstance(method, (str, unicode))
        assert isinstance(req_kwargs, dict)
        
        
        self.url = url
        self.method = method
        self.req_kwargs = req_kwargs
    
    @classmethod
    def from_json(cls, json_string):
        '''
        Input:  string json_string containing a url key
        
        Returns: a Callback obj instance initalized by the parameters in the json_string.
        '''
        json_obj = json.loads(json_string)
        
        if 'url' not in json_obj:
            raise Exception('"url" not in json')
        
        return cls.__init__(**json_obj)
    
    def to_json(self):
        '''
        Return a JSON serialization of the Callback obj.
        '''
        json_obj = {'url':self.url,
                    'method':self.method,
                    'req_kwargs':self.req_kwargs}
        
        return json.dumps(json_obj)
    
    def execute(self):
        f = getattr(requests, self.method.lower())
        f(self.url, **self.req_kwargs)

    
    