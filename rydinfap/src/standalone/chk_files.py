'''
Created on May 20, 2015

@author: eocampo

General utility to do operations in files. 

'''

__version__ = '20150520'

import sys
import os
import socket
import common.log4py   as log4py
import common.simpmail as sm
import utils.fileutils as fu
import utils.strutils  as su

from common.loghdl import getLogHandler

os.environ['LOG_LEVEL'] = 'NORMAL'
os.environ['LOG_DIR']   = r'C:/apps/logs' 
os.environ['LOGNAME']   = 'chk_files'
os.environ['APP_NAME']  = 'chk_files'

hostname = socket.gethostname()
appName  = os.environ['APP_NAME'] 

#Get the Logname
log      =  log4py.Logger().get_instance()
logFile  = getLogHandler('chk_files',log)
infaLogs = 'logs'
RET_WARN = 101

class InfaBaseAppBean:
    pass



# cfgFile = r'C:\apps\config\chk_files.cfg'
ib = InfaBaseAppBean

# Use wildcard in filename
def _checkIncWildFiles(fn):
    
    # Get file
    fnp = fu.getNewestFile(ib.landDir,fn)
    log.debug("Newest File " , fnp)
    if fnp is None : 
        log.error("No files were found for %s/%s "  % (ib.landDir,fn))
        return 1
    
    #Check file last modification time
    tm = fu.chkFileModTime(fnp)
    if (float(tm)  > float(ib.fileAge)) : 
        log.error("file %s age %.1f secs is older than fileAge %s secs" % (fnp,tm,ib.fileAge))
        return 2
    
    log.info ("file %s age  %.1f secs is newer than  fileAge %s secs" % (fnp,tm,ib.fileAge))
    
    fms = su.toInt(ib.fileMinSize)
    log.debug("fms = %s ib.fileMinSize %s" % (fms,ib.fileMinSize))
    if fms is None : return 3
    # Check File Size in bytes, only if fileMinSize > -1
    if fms > -1 :
        fs = fu.getFilseSize(fnp)
        log.debug('size = %s minsize = %s fnp = %s ' % (fs,fms,fnp))
       
        if fs < fms:
            log.error('File fnp %s %s(bytes) < %s fileMinSize' % (fnp,fs,fms))
            return 4
    
    return 0

def getIncWildFiles():
    errFlg = 0
    fnl = ib.fileName.split(',')
    log.info('Source File dir %s' % ib.landDir)
    log.info('filename Size= %d List = %s ' % (len(fnl), fnl))
    for fn in fnl:
        rc = _checkIncWildFiles(fn)
        log.debug("filename %s -- rc = %s" %  (fn,rc))
        if rc != 0 : errFlg+=1 
        
    return errFlg

def main(argv):
    
    
    log.debug("argv ", argv, "len argv ", len(argv))
    
    if len(argv) == 2 :
       
        log.info('Creating logFile %s ' %  logFile)
        cfgFile = argv[1]
        log.info('Loading ConfigFile %s ' %  cfgFile)
       
    else:
        
        log.error( "USAGE : <%s> fx [args] Not enough arguments " % argv[0])
        return 1

    # Loading config files.
    rc = fu.loadConfigFile(cfgFile, ib, log)
    if rc != 0 :
        log.error('Issue opening config File %s rc = %s' % (cfgFile,rc))
        return rc
    log.debug('Loaded %s ' % cfgFile )
    
    # Set Files
    rc = getIncWildFiles()
    if rc != 0 :
        log.error('Issue with source files Files rc = %s' % (rc))
        return rc
        
    
    
def notify(rc):
    # Notify
    if rc != RET_WARN:
        text = 'Please see logFile %s' % logFile
        subj = "SUCCESS running %s on %s " % (appName, hostname) if rc == 0 else "ERROR running %s on %s" % (appName, hostname)
        r, msg = sm.sendNotif(rc, log, subj, text, [logFile, ])
        log.info('Sending Notification\t%s rc = %s' % (msg, r))    
    
    else:
        log.info('Notification Overrided. Not sending message (RET_WARN) rc = %s ' % rc)    # EO To do add notify logic.     
    
if __name__ == '__main__':
    rc=  main(sys.argv)
    notify(rc)
    sys.exit(rc)
