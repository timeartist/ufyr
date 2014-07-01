from os import stat
from math import ceil
from posixpath import join

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from filechunkio import FileChunkIO

C = S3Connection('AKIAJUHCUN6DO44DGDCQ', '2WAr/4uFDsdDi94wkw0vHT7ya7ahlhTXbFPkw+6e')

CLIP_MEDIA = C.get_bucket('clip-media')
MOVIE_MEDIA = C.get_bucket('adishakti-dev')
POSTROLL = C.get_bucket('adishakti-dev')
YOUTUBE = C.get_bucket('adishakti-dev')
SOURCE = C.get_bucket('adishakti-dev')
STREAMING = C.get_bucket('adishakti-dev')


def _get_file(bucket, s3_path, output_path):
    k = Key(bucket)
    k.key = s3_path
    k.get_contents_to_filename(output_path)

def _put_file(bucket, s3_path, input_path):
    chunk_size = 52428800
    file_size = stat(input_path).st_size
    
    if file_size <= chunk_size:
        k = Key(bucket)
        k.key = s3_path
        k.set_contents_from_filename(input_path)
    else:
        chunk_count = int(ceil(file_size / chunk_size))
        mp = bucket.initiate_multipart_upload(s3_path)
        
        for i in range(chunk_count + 1):
            offset = chunk_size * i
            _bytes = min(chunk_size, file_size - offset)
            with FileChunkIO(input_path, 'r', offset=offset, bytes=_bytes) as fp:
                mp.upload_part_from_file(fp, part_num = i + 1)
                
        mp.complete_upload()
        
def __canonical_path(studio_name, movie_title_slug):
    _studio_name = studio_name.replace(' ', '-').lower()
    return join(_studio_name, movie_title_slug[0], movie_title_slug)

def __clip_media_small_media_path(studio_name, movie_title_slug, filename):
    return join('small_media', __canonical_path(studio_name, movie_title_slug), filename)

def __clip_media_large_media_path(studio_name, movie_title_slug, filename):
    return join('large_media', __canonical_path(studio_name, movie_title_slug), filename)
        
def __clip_media_png_path(clip_id):
    return join('png', '%d.png'%clip_id)

def __clip_media_clip_full_path(clip_id):
    return join('clip_full', '%d.mov'%clip_id)

def __movie_media_posters_path(movie_id):
    return join('posters', 'archive', '%d.jpg'%movie_id)

def __postroll_clip_prev_full_path(clip_id):
    return join('clip_prev_full', '%d.mp4'%clip_id)

def __postroll_thumbs_path(clip_id):
    return join('thumbs', '%d.jpg'%clip_id)

def __source_path(movie_id, media_id):
    return join(movie_id, '%d.mp4')

def __streaming_path(media_id):
    return '%d.mp4'%media_id






    
if __name__ == '__main__':
    _put_file(CLIP_MEDIA, '/Users/afoulger/Desktop/tmp/overlay.png', 'small_media/test/overlay.png')
    #_get_file(CLIP_MEDIA, 'small_media/test/overlay.png', '/tmp/overlay.png')
    #_put_file(CLIP_MEDIA, '/Users/afoulger/Desktop/tmp/10268_tmp.mov', 'large_media/10268.mov')
    #_get_file(CLIP_MEDIA, 'large_media/10268.mov', '/tmp/10268.mov')
    
        
    




