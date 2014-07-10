import unittest
import json
from os import environ
from random import choice
from subprocess import Popen
from sys import executable
from time import sleep

import requests

from ufyr.utils.http import Callback

class TestHTTPCallback(unittest.TestCase):
    def setUp(self):
        self.app = Popen([executable, 'tests/echo_server.py'], env=environ.copy())
        self.url = 'http://localhost:5000'
        self.method = choice(['GET', 'PUT', 'POST', 'DELETE'])
        self.req_kwargs = {'data':json.dumps({'foo':'bar'}),
                           'params':{'baz':'boo'}}
        
        sleep(5)
        
    
    def test_execute(self):
        '''
        Test an execution of the callback obj
        '''
        
        cb = Callback(self.url, self.method, self.req_kwargs)
        resp = cb.execute()
        
        resp_data = json.loads(resp.text)
        
        self.assertEqual(resp_data.get('method'), self.method)
        self.assertEqual(resp_data.get('args'), self.req_kwargs['params'])
        self.assertEqual(resp_data.get('data'), self.req_kwargs['data'])


    def test_jsonification(self):
        cb = Callback(self.url, self.method, self.req_kwargs)
        cb_json = cb.to_json()
        
        resp = requests.get('http://localhost:5000', data=cb_json)
        
        resp_data = json.loads(resp.text)
        
        new_cb = Callback.from_json(resp_data.get('data', {}))
        
        self.assertEqual(cb, new_cb)
        
        
    def tearDown(self):
        self.app.kill()
        
        

if __name__ == '__main__':
    unittest.main()
        