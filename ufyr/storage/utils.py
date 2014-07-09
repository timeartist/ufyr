from os import chmod, unlink, stat
from os.path import isfile
from shutil import copy

def move_verify_delete(in_file, out_file):
    '''
    Moves in_file to out_file, verifies that the filesizes are the same and
    then does a chmod 666
    '''
    
    if isfile(in_file) and not isfile(out_file):
        orig_file_size = stat(in_file).st_size
        copy(in_file, out_file)
        new_file_size = stat(out_file).st_size
        
        if new_file_size != orig_file_size:
            raise Exception('File Transfer Error! %s:%d -> %s:%d'%(in_file, orig_file_size, out_file, new_file_size))
        
        unlink(in_file)
        chmod(out_file, 666)