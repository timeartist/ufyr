
from time import sleep

from logger import Logger
from utils.http import Callback


def do(func_list, metadata_class, retry_limit=None, log_path='/var/log/do.log', callback=None, **kwargs):
    '''
    Logging/retry logic wrapper.
    
    Inputs:
        func_list: list of tuples [(func, (args), {kwargs})] - args vs kwargs works on type identification
        retry_limit: int, maximum retries for a given function - not to exceed settings.RETRY_THRESHOLD
        log_name: string name of log file that do should write to
        callback: string callback json from Callback.to_json to execute upon func_list completion
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
        retry = retry_limit if retry_limit and retry_limit <= RETRY_THRESHOLD else RETRY_THRESHOLD
        
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