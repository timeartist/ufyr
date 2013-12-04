#! /usr/bin/python

import json

import requests

from ufyr.decorators import retry

class Callback(object):
    def __init__(self, url, method='GET', req_kwargs={}, **kwargs):
        
        assert isinstance(url, (str, unicode))
        assert isinstance(method, (str, unicode))
        assert isinstance(req_kwargs, dict)
        
        
        self.url = url
        self.method = method
        self.req_kwargs = req_kwargs
        
    def __eq__(self, other):
        
        return all((self.url.lower() == other.url.lower(),
                    self.method.lower() == other.method.lower(),
                    self.req_kwargs == other.req_kwargs))
        
    
    @classmethod
    def from_json(cls, json_string):
        '''
        Input:  string json_string containing a url key
        
        Returns: a Callback obj instance initalized by the parameters in the json_string.
        '''
        json_obj = json.loads(json_string)
        
        if 'url' not in json_obj:
            raise Exception('"url" not in json')
        
        return Callback(**json_obj)
    
    def to_json(self):
        '''
        Return a JSON serialization of the Callback obj.
        '''
        json_obj = {'url':self.url,
                    'method':self.method,
                    'req_kwargs':self.req_kwargs}
        
        return json.dumps(json_obj)
    
    def execute_async(self, queue):
        f = getattr(requests, self.method.lower())
        f = retry(f) ##decorate and then enqueue the decorated function
        f.enqueue_call(func=f,
                       kwargs = self.req_kwargs)
    
    @retry
    def execute(self):
        '''
        Execute the callback call that this object represents.
        '''
        f = getattr(requests, self.method.lower())
        return f(self.url, **self.req_kwargs).status_code <= 400

    
    