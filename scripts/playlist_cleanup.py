import traceback
from json import dumps
from time import sleep
from random import triangular

import requests
from BeautifulSoup import BeautifulSoup, SoupStrainer

from annotator.browser import yt_browser

from videolab.youtube_tools import YoutubeTools
from videolab.thread_tools import threadify

from zdb.utils import get_session

headers = {'api-key':'righteous~lion-battleaxe+MAC10'}

yt = YoutubeTools()
s = get_session('postgresql://annotator_user:7842-cried-SORRY-arms-HIGH-marry-PROBLEM-plant-HALT-85__________@use1-prod-masterdb0.cgii22lwhlzo.us-east-1.rds.amazonaws.com:6543/zefr')

@threadify(16, is_method=False)
def get_videos_for_playlists(playlist_ids):
    playlists = {}
    for playlist_id in playlist_ids:
        try:
            resp = requests.get('http://staging-lister.zefr.com/playlist/playlist_videos/%s/%s'%\
                                (cid, playlist_id), headers=headers)
            if resp.status_code == 200:
                resp_data = resp.json()
                playlists[playlist_id] = [video['id'] for video in resp_data['videos']]
        except:
            traceback.print_exc()
    
    return playlists
    #cleanup_empty_playlists(playlists.items())
    #return ret

@threadify(16, is_method=False)
def cleanup_empty_playlists(playlists):
    for playlist_id, videos in playlists:
        valid_video_found = False
        for video in videos:
            video_resp = yt.get_v3_video(video)
            if video_resp.get('items'):
                valid_video_found = True
            else:
                resp = requests.get('http://staging-lister.zefr.com/playlist/remove_video/%s/%s/%s'%\
                                    (cid, playlist_id, video), headers=headers)
                print 'Removed', video, resp
        
        if not valid_video_found:
            resp = requests.get('http://staging-lister.zefr.com/playlist/delete_playlist/%s/%s'%\
                                (cid,playlist_id), headers=headers)
            print resp
            print playlist_id, 'Valid Video Not Found', resp
            
@threadify(16, is_method=False)        
def turn_playlists_public(playlists):
    for playlist in playlists:
        resp = requests.get('http://staging-lister.zefr.com/playlist/playlist_data/%s/%s'%playlist,
                            headers=headers)
        #import pdb; pdb.set_trace()
        if resp.status_code == 200:
            resp_data = resp.json()
            if resp_data.get('status') == 'private':
                resp_data['status'] = 'public'
                resp = requests.post('http://staging-lister.zefr.com/playlist/playlist_data',
                                     data=dumps(resp_data), headers=headers)
                
                
def get_channel_playlists_mechanized(username, password):
    
    browser = yt_browser(username=username, password=password)
    browser.login()
    
    page = browser.visit('http://www.youtube.com/view_all_playlists')
    
    tags = BeautifulSoup(page, parseOnlyThese=SoupStrainer('span'))
    
    #print tags
    
    for tag in tags:
        _id = tag.get('id')
        if _id and _id == 'vm-num-videos-shown':
            playlist_count = int(tag.text)/100 + 1
            
    playlist_ids = get_playlists_from_page(page)
    
    for i in xrange(2, playlist_count):
        page = browser.visit('http://www.youtube.com/view_all_playlists?page=%d'%i)
        playlist_ids.extend(get_playlists_from_page(page))
        sleep(triangular(3,5))
    
    return playlist_ids
    
def get_playlists_from_page(page):
    
    playlists = []
    
    tags = BeautifulSoup(page, parseOnlyThese=SoupStrainer('li'))
    
    for tag in tags:
        _id = tag.get('id')
        if _id and 'playlist' in _id:
             playlists.append( _id.split('-', 2).pop())
        
    return playlists
    

if __name__ == '__main__':
    
    cid = yt.get_channel_id('movieclipsactor')
    playlist_ids = get_channel_playlists_mechanized('movieactor@movieclips.com', 'NewTable')
    playlist_videos = get_videos_for_playlists(playlist_ids)
    cleanup_empty_playlists(playlist_videos.items())
    #mcid = yt.get_channel_id('movieclips')
    
    #resp = requests.get('http://staging-lister.zefr.com/playlist/channel_playlists/%s'%cid, headers=headers)
    #playlist_ids = [(cid, pid) for pid in resp.json().keys()]
    ##print playlist_ids
    ##sql = "select youtube_id from movieclips.new_youtube_playlists where lower(channel) = 'movieclipsfilm'; "
    #
    ##rows = s.execute(sql).fetchall()
    ##playlist_ids = [(cid, row[0]) for row in rows]
    ##print playlist_ids
    ##playlist_videos = get_videos_for_playlists(playlist_ids)
    ##print playlist_videos
    #
    ##
    #turn_playlists_public(playlist_ids)
    
    #for playlist_id, videos in playlist_videos.items():
    #    valid_video_found = False
    #    for video in videos:
    #        video_resp = yt.get_v3_video(video)
    #        if video_resp.get('items'):
    #            valid_video_found = True
    #    
    #    if not valid_video_found:
    #        resp = requests.get('http://staging-lister.zefr.com/playlist/delete_playlist/%s/%s'%(cid,playlist_id), headers=headers)
    #        print resp
    #    
    #    print playlist_id, 'Valid Video Found', valid_video_found
                


