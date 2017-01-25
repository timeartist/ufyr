#stdlib
import traceback
from time import sleep

#local
from logger import Logger
from utils.http import Callback


def do(func_list, metadata_class, retry_limit=None, log_name='/var/log/do.log', callback=None, **kwargs):
    '''
    Logging/retry logic wrapper.
    
    Inputs:
        func_list: list of tuples [(func, (args), {kwargs})] - args vs kwargs works on type identification
        metadata_class - instance of metadata.MetadataBase
        retry_limit: int, maximum retries for a given function
        log_name: string name of log file that do should write to
        callback: string callback json from http.Callback.to_json to execute upon func_list completion
    '''
    
    logger = Logger('do', log_name).get_logger()
    md = metadata_class()
    
    ##iterate through the given functions
    for f_data in func_list:
        
        func, args, fkwargs = _get_args_kwargs(f_data)
        
        f_name = '.'.join((func.__module__, func.__name__))
        
        if 'ref_id' in fkwargs:
            fkwargs.update(md.get_metadata(fkwargs['ref_id']))
        elif args:
            fkwargs.update(md.get_metadata(args[0]))
        
        fkwargs.update(kwargs)
        
        logger.info('%s\tBEGIN\targs: %s\tkwargs: %s', f_name, args, fkwargs)
        retry = retry_limit if retry_limit else 1
        
        success = False
        for i in xrange(retry):
            sleep(30*i)
            logger.info('%s\tATTEMPT %s', f_name, i+1)
            
            try:
                result = func(*args, **fkwargs)
                
                logger.info('%s\tRETURNED %s', f_name, result)
                success = True
                break
            except:
                logger.error('%s\tRAISED AN EXCEPTION', f_name)
                logger.error(traceback.format_exc())
                md.set_error(traceback.format_exc())
        
        if not success:
            raise Exception('Maximum retries exceeded')
    
    if callback is not None:
        logger.info("Executing Callback")

        cb = Callback.from_json(callback)
        cb.execute()
        
        logger.info('Callback complete')
        
    logger.info('DONE')
    
    
def _get_args_kwargs(f_data):
    '''
    Given a tuple of a length of 1-3 return a tuple of func, args, kwargs
    
    Inputs:
        f_data - tuple (function, (args) or {kwargs}, {kwargs} or (args)) -- id'd off of type
        
    Returns:
        a tuple of func, args, kwargs
    '''
    func = f_data[0]
    
    if isinstance(func, (str, unicode)):
        func = __get_function(func)
        
    assert callable(func)
    
    arg_chunk = f_data[1:]
    args = ()
    kwargs = {}
    
    if len(arg_chunk) == 0:
        pass
    elif len(arg_chunk) == 1:
        tmp = arg_chunk[0]
        if isinstance(tmp, (list, tuple)):
            args = tmp
        elif isinstance(tmp, dict):
            kwargs.update(tmp)
        else:
            raise Exception('args mapping failure: %s'%str(arg_chunk))
    elif len(arg_chunk) == 2:
        for args_item in arg_chunk:
            if isinstance(args_item, (list, tuple)) and not args:
                args = args_item
            elif isinstance(args_item, dict):
                kwargs.update(args_item)
            else:
                raise Exception('args mapping failure: %s'%str(arg_chunk))
            
    else:
        raise Exception('args mapping failure: %s'%str(arg_chunk))
    
    return func, args, kwargs

def __get_function(name):
    """Return an attribute from a dotted path name (e.g. "path.to.func")."""
    module_name, obj, method = name.rsplit('.', 2)
    module = import_module(module_name)
    var = getattr(module, obj)
    return getattr(var, method)
