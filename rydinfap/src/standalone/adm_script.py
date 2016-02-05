'''
Created on Apr 10, 2012

@author: eocampo
'''
import sys
import os

import mainglob as mg                 # DO not comment
import common.log4py   as log4py 
import utils.fileutils as fu
from common.loghdl import getLogHandler

#Get the Logname
log     =  log4py.Logger().get_instance()


def treadCSV(fn):
    return ['1','2','3'], ['B1','B2','B3']

# dp  dir path 
# dl  delimeter
# suf file suffix
#
def check_dir(dp, dl, suf=''):
    flst = fu.getDirFiles (dp,suf)
    for f in flst:
        fn = '%s/%s' % (dp,f)
        r,b = treadCSV(fn)
        log.info('Processed file %s rows %d bad %d' % (f,len(r),len(b)))
        if len(b) > 0:
            fu.createFile ('%s.bad' % fn, b)
            log.error('Created file %s.bad' % fn)
        #log.info
# dp  dir path 
# fn file name
def check_file(dp,fn):
    pass

#def _getLogHandler(p):
#    global logFile
#    logName   = os.environ['LOGNAME'] if os.environ.has_key('LOGNAME') else p
#    infaLogs  = os.environ['LOG_DIR'] if os.environ.has_key('LOG_DIR') else os.path.dirname(__file__)
#    
#    if os.environ.has_key('LOG_LEVEL'): log.set_loglevel(os.environ['LOG_LEVEL'] )
#        
#    logFile = '%s/%s.log' % (infaLogs,logName)
#    print "LogFile = %s " % logName
#    
#    log.add_target(logFile)
#    log.remove_target('%s/infa_adm_script.log' % infaLogs)


# Use from script for standalone functions/
# Invoke notify from here.
def main(argv):
    
    if len(argv) > 1 :
        logFile = getLogHandler(argv[1])
        
           
    else:
        log.error( "USAGE : <%s> fx [args] Not enough arguments " % argv[0])
        sys.exit(1)
    
        
if __name__ == '__main__':
    rc=main(sys.argv)
    check_dir(r'C:\infa_support\test_files', ',','csv')
    
