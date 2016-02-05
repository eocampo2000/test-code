'''
Created on May 29, 2012

@author: eocampo
 General routine to handle logger

'''
import os 

# ln  log name
# log log4py handler
# returns the logFile path/name

def getLogHandler(ln,log):
    
    logName   = os.environ['LOGNAME'] if os.environ.has_key('LOGNAME') else ln
    infaLogs  = os.environ['LOG_DIR'] if os.environ.has_key('LOG_DIR') else os.path.dirname(__file__)
    
    if os.environ.has_key('LOG_LEVEL'): log.set_loglevel(os.environ['LOG_LEVEL'] )
        
    logFile = '%s/%s.log' % (infaLogs,logName)
    
    log.add_target(logFile)
    
    #log.remove_target('%s/infa_adm_script.log' % infaLogs)
    
    return logFile
