'''
Created on Apr 17, 2012

@author: eocampo

SAS Admin Tasks

'''

import sys, os
import time
import shutil

import common.log4py as log4py 
import proc.process  as ps 
import bkupconf      as bc
from common.loghdl import getLogHandler

log     = log4py.Logger().get_instance()
TS      = time.strftime("%Y%m%d_%H%M", time.localtime(time.time()))

bkpDst  = ('%s\%s') % (bc.bkpBase,TS)

def _metaBkup():
    rc,rm = ps.runSync(bc.SASBkupCmd, log)
    return rc

def _copyBkup(src,dst):
    rc = 0
    try:
        shutil.copytree(src, dst)   
    except:
        log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))
        rc = -1
    finally : return rc 

## log levels "DEBUG", "VERBOSE", "NORMAL","NONE", "ERROR"
#def _getLogHandler(p):
#    #global logFile
#    logName   = os.environ['LOGNAME'] if os.environ.has_key('LOGNAME') else p
#    infaLogs  = os.environ['LOG_DIR'] if os.environ.has_key('LOG_DIR') else os.path.dirname(__file__)
#    
#    if os.environ.has_key('LOG_LEVEL'): log.set_loglevel(os.environ['LOG_LEVEL'] )
#        
#    logFile = '%s/%s.log' % (infaLogs,logName)
#    print "LogFile = %s " % logName
#    
#    log.add_target(logFile)
  
# Invoke notify from here.
def _bkupSAS(argv):
    ln = "backupSAS"              # LogName Hardcoded in case not defined in an ENV VAR or in cmd line. 
    print len(argv)
    if len(argv) > 1 : ln = argv[1]
    logFile = getLogHandler(ln,log)  
    print "LogFile is %s " % logFile

    # invoke SAS script
    log.info('Starting Metadata Backup to %s' % bkpDst)
    rc = _metaBkup()
    if rc != 0 : log.error("%s rc=%s" % (rc,bc.SASBkupCmd))
    else       : 
        log.info("%s rc=%s" % (rc,bc.SASBkupCmd))
        rc = _copyBkup(bc.bkpSASDir,bkpDst)
        log.info('_copyBkup rc = %s' % rc )

    return rc

# Use from script for standalone functions/
# Invoke notify from here.
def main(argv):
    rc = _bkupSAS(argv)
    
    
if __name__ == '__main__':
    rc=_bkupSAS(sys.argv)
#    check_dir(r'C:\infa_support\test_files', ',','csv')