from src.utils.metrics import MetricsHelper
import requests
import sys


def read_image():
    ret = requests.get(
        'https://images.unsplash.com/photo-1608501078713-8e445a709b39?ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D&auto=format&fit=crop&w=2070&q=80')
    sys.getsizeof(ret.content)


def test_record():
    helper = MetricsHelper('mock', 1)
    helper.set_file('file')
    helper.record(func=read_image, tech='tech', merged=False)
    df = helper._get_output()
    results = df.to_dict(orient='records')[0]
    assert results['data_dir'] == 'mock'
    assert results['file_count'] == 1
    assert results['tech'] == 'tech'
    assert not results['merged']
    assert results['filename'] == 'file'
    assert results['step'] == 0
    assert results['bytes_sent']
    assert results['bytes_recv'] > 6e5
    assert results['bytes_sent_1s']
    assert results['bytes_recv_1s'] < results['bytes_recv']
