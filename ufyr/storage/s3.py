from os import stat, path, environ
from math import ceil
from posixpath import join

from boto.s3.connection import S3Connection
from boto.s3.key import Key

from filechunkio import FileChunkIO


C = S3Connection(environ['AWS_KEY'], environ['AWS_SECRET'])

CLIP_MEDIA = C.get_bucket('clip-media')
MOVIE_MEDIA = C.get_bucket('movie-media')
POSTROLL = C.get_bucket('postroll')
YOUTUBE = C.get_bucket('movieclips-youtube')
SOURCE = C.get_bucket('movieclips-source')
STREAMING = C.get_bucket('movieclips-streaming')

def save_source_file(filepath, movie_id, media_id):
    _validate_filepath(filepath)
    _put_file(SOURCE, _source_path(movie_id, media_id), filepath)
    
def save_streaming_file(filepath, media_id):
    _validate_filepath(filepath)
    _put_file(STREAMING, _streaming_path(media_id), filepath)
    
def save_postroll_thumb(filepath, clip_id):
    _validate_filepath(filepath)
    _put_file(POSTROLL, _postroll_thumbs_path(clip_id), filepath)
    
def save_derivative_thumbs(filepaths, studio_name, movie_title_slug):
    assert isinstance(filepaths, (list, tuple))
    
    for fp in filepaths:
        _validate_filepath(fp)
        _put_file(CLIP_MEDIA,
                  _clip_media_small_media_path(studio_name, movie_title_slug, path.basename(fp)),
                  fp)
        
def save_png_thumb(filepath, clip_id):
    _validate_filepath(fp)
    _put_file(CLIP_MEDIA, _clip_media_png_path(clip_id), fp)
    
def save_clip_full(filepath, clip_id):
    _validate_filepath(fp)
    _put_file(CLIP_MEDIA, _clip_media_clip_full_path(clip_id), fp)
    
def save_deriviative_clips(filepaths, studio_name, movie_title_slug):
    assert isinstance(filepaths, (list, tuple))
    
    for fp in filepaths:
        _validate_filepath(fp)
        _put_file(CLIP_MEDIA,
                  _clip_media_large_media_path(studio_name, movie_title_slug, path.basename(fp)),
                  fp)

##SUPPORT FUNCTIONS##

def _validate_filepath(fp):
    if not path.isfile(fp):
        raise Exception('%s does not exist'%fp)


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
        

def _clip_media_small_media_path(studio_name, movie_title_slug, filename):
    return join('small_media', __canonical_path(studio_name, movie_title_slug), filename)

def _clip_media_large_media_path(studio_name, movie_title_slug, filename):
    return join('large_media', __canonical_path(studio_name, movie_title_slug), filename)
        
def _clip_media_png_path(clip_id):
    return join('png', '%d.png'%clip_id)

def _clip_media_clip_full_path(clip_id):
    return join('clip_full', '%d.mov'%clip_id)

def _movie_media_posters_path(movie_id):
    return join('posters', 'archive', '%d.jpg'%movie_id)

def _postroll_clip_prev_full_path(clip_id):
    return join('clip_prev_full', '%d.mp4'%clip_id)

def _postroll_thumbs_path(clip_id):
    return join('thumbs', '%d.jpg'%clip_id)

def _source_path(movie_id, media_id):
    return join(movie_id, '%d.mp4')

def _streaming_path(media_id):
    return '%d.mp4'%media_id

##SUPPORT SUPPORT FUNCTIONS##

def __canonical_path(studio_name, movie_title_slug):
    _studio_name = studio_name.replace(' ', '-').lower()
    return join(_studio_name, movie_title_slug[0], movie_title_slug)






    
if __name__ == '__main__':
    _put_file(CLIP_MEDIA, '/Users/afoulger/Desktop/tmp/overlay.png', 'small_media/test/overlay.png')
    #_get_file(CLIP_MEDIA, 'small_media/test/overlay.png', '/tmp/overlay.png')
    #_put_file(CLIP_MEDIA, '/Users/afoulger/Desktop/tmp/10268_tmp.mov', 'large_media/10268.mov')
    #_get_file(CLIP_MEDIA, 'large_media/10268.mov', '/tmp/10268.mov')
    
        
    




