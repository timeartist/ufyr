from os import chmod, unlink, stat, makedirs
from os.path import isfile, split, exists
from shutil import copyfile

def move_verify_delete(in_file, out_file, overwrite=False):
    '''
    Moves in_file to out_file, verifies that the filesizes are the same and
    then does a chmod 666
    '''
    if not exists(split(out_file)[0]):
        makedirs(split(out_file)[0])
            
    if isfile(in_file) and (overwrite or not isfile(out_file)):
        orig_file_size = stat(in_file).st_size
        copyfile(in_file, out_file)
        new_file_size = stat(out_file).st_size
        
        if new_file_size != orig_file_size:
            raise Exception('File Transfer Error! %s:%d -> %s:%d'%(in_file, orig_file_size, out_file, new_file_size))
        
        unlink(in_file)
        #chmod(out_file, 666)
    else:
        
        raise Exception('File Transfer Error! %s EXISTS %s %s EXISTS %s'%(in_file,
                                                                          isfile(in_file),
                                                                          out_file,
                                                                          isfile(out_file)))