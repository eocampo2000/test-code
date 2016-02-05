'''
Created on Jan 16, 2012

   For standalone runs.

@author: eocampo


'''
import  common.log4py as log4py
import sys

# lvl = LOG_LEVELS = {"DEBUG"  : LOGLEVEL_DEBUG, 
#                     "VERBOSE": LOGLEVEL_VERBOSE, 
#                     "NORMAL" : LOGLEVEL_NORMAL, 
#                     "NONE"   : LOGLEVEL_NONE, 
#                     "ERROR"  : LOGLEVEL_ERROR } 
# tgt = targets e.g. [sys.stdout, r'C:\ptest\all\logs\ryderinfa.log' ]

def getLogger(lvl = 'NORMAL', tgt = (sys.stdout,)):
    
    log = log4py.Logger().get_instance()
    log.set_time_format("%d.%m.%Y_%H:%M")
    lvl = log4py.LOG_LEVELS.get(lvl,'NORMAL')
    log.set_loglevel(lvl)
  
    for t in tgt:
        log.add_target(t)
 
    return log