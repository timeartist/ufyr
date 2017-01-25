'''
A very ok recurstive S3 uploader.

- Start py file with folder you want to start in
- Uploads, Verifies, Doesn't stop til it hits the bottom
- if you kill the process id, it will finish the upload then quit
- It's designed to run on a cron during off hours
'''

import math, os, signal, hashlib
from glob import glob

from filechunkio import FileChunkIO
from boto import connect_s3
from boto.s3.key import Key

from gevent import sleep
from gevent.pool import Pool
from gevent.event import Event

from ufyr.decorators import retry

CHUNK_SIZE = 80000000 #10Mb

pool = Pool(4)
s3 = connect_s3()
bucket = s3.get_bucket('i-haz-a-bucket')
closure = Event()

def signal_handler(*args):
    '''
    Function container to catch SIGTERM and translate
    it to a graceful closure
    '''
    print "SETTING CLOSURE", closure
    closure.set()

def save_pid():
    with open('/tmp/uploader.pid', 'w') as f:
        f.write(str(os.getpid()))

def do_upload(_file):
    '''
    Given a filepath, upload it using the appropriate s3 method
    '''
    if closure.is_set():
        return

    if os.path.isfile(_file):
        if os.stat(_file).st_size <= CHUNK_SIZE:
            upload_file(_file)
        else:
            upload_large_file(_file)

def traverse_file_path(filepath):
    '''
    Given a filepath, yield files in that path.
    Is recursive, will only return files.
    '''
    if closure.is_set():
        raise StopIteration

    files = glob(os.path.join(filepath, '*'))

    for _file in files:
        if os.path.isfile(_file) and bucket.get_key(_file) is None:
            yield _file
        else:
            for __file in traverse_file_path(_file):
                yield __file

@retry(limit=3, interval=(90, 120))
def upload_file(filepath):
    '''
    Simple upload - straight out of the boto docs
    '''
    print 'UPLOAD SMALL FILE', filepath

    md5 = md5_from_file(filepath)
    key = Key(bucket, filepath)
    key.set_contents_from_filename(filepath)

    print 'UPLOAD COMPLETE', filepath

    return '"%s"'%md5 == key.etag

@retry(limit=3, interval=(90, 120))
def upload_large_file(filepath):
    '''
    Big upload - also straight out of the docs
    '''
    print 'UPLOAD LARGE FILE', filepath

    uploader = bucket.initiate_multipart_upload(filepath)
    hashval = b''
    _i = 0
    try:
        file_size = os.stat(filepath).st_size
        chunk_count = int(math.ceil(file_size/CHUNK_SIZE))

        for i in range(chunk_count + 1):
            offset = CHUNK_SIZE * i
            _bytes = min(CHUNK_SIZE, file_size - offset)
            with FileChunkIO(filepath, 'r', offset=offset, bytes=_bytes) as fp:
                hashval += hashlib.md5(fp.read()).digest()
                fp.seek(0)
                print str((float(CHUNK_SIZE) * i / float(file_size))*100) + '% complete\r'
                uploader.upload_part_from_file(fp, part_num=i+1)

            _i += 1

        uploader.complete_upload()
        key = bucket.get_key(uploader.key_name)
        print 'UPLOAD COMPLETE', filepath

        return key.etag == '"%s-%d"'%(hashlib.md5(hashval).hexdigest(), _i)
    except:
        uploader.cancel_upload()
        raise

def md5_from_file(_file):
    return hashlib.md5(_get_file_contents(_file)).hexdigest()

def _get_file_contents(_file):
    with open(_file, 'rb') as f:
        return f.read()

if __name__ == '__main__':
    save_pid()
    signal.signal(signal.SIGTERM, signal_handler)
    pool.map(do_upload, traverse_file_path(argv[1]))
