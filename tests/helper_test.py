import requests
from xetrack import Tracker
import sys


def read_image():
    ret = requests.get(
        'https://images.unsplash.com/photo-1608501078713-8e445a709b39?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=2070&q=80')
    sys.getsizeof(ret.content)


def test_record():
    tracker = Tracker(params={'data_dir': 'mock'})
    tracker.track(func=read_image, params={'tech': 'tech', 'merged': False})
    results = tracker.latest
    assert results['data_dir'] == 'mock'
    assert results['tech'] == 'tech'
    assert not results['merged']
    assert results['bytes_sent']
    assert results['bytes_recv'] > 0.5
