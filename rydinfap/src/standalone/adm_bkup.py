'''
Created on Oct 23, 2012

@author: eocampo

This module is to create FS backups.

'''
__version__ = '20121023'


import os, sys, socket

import utils.fileutils  as fu 
import common.simpmail  as sm
import common.log4py    as log4py 

from common.loghdl   import getLogHandler

log      =  log4py.Logger().get_instance()
hostname = socket.gethostname()

# Remove in UNIX
os.environ['SRC_DIR' ] = r'C:/backup_test/config,C:/backup_test/lck,C:/backup_test/junk,C:/backup_test/ptest,' 
os.environ['DEST_DIR'] = r'C:/backup_test1/all_bkups'   # BASE DIR
os.environ['COMP']     = 'gz' 
# END OF REMOVE

src_dir = ''     # CSV string containing all src_dirs that need backup.
dest_dir= ''     # Dest directory.
comp    = ''

infaEnvVar   = {
                'SRC_DIR'          : 'src_dir'      ,
                'DEST_DIR'         : 'dest_dir'     ,
                'COMP'             : 'comp'
               }  
    
# This function will archive a list of dirs.
#
def bkupDir():
    r = 0
    if dest_dir == ''  or src_dir == '' :
        log.error("DEST_DIR : %s is empty ! or SRC_DIR  : %s is Empty ! " % (dest_dir,src_dir))
        return 1
    
    if fu.isDir(dest_dir) is False:    # If dest dir does not exist, create it.
        rc = fu.createDirs(dest_dir) 
        if rc != 0 :
            log.error("Creating %s dir !" % dest_dir)
        
    dirToTar = src_dir.split(',')
    log.debug('Dir to tar = ', dirToTar)
    if  len(dirToTar) < 1 :
        log.error("Nothing to Tar %s" % dirToTar)
        return 2
        
    for d in dirToTar:
        if d == '' : continue 
        rc = fu.tarFile(d, dest_dir, log, comp)
        if rc == 0 : log.info('Archived %s ' % d)
        else       : 
            log.error('Issue Archiving %s' % d)
            r+=1
    
    return r   
# This methods sets OS environment variables, needed for the MAIN_CFG driver
# Side Effect sets the global sets globally ,infaHome,  infaFiles, ib, etc
def _getEnvVars():
    ret = 0   
    for ev, v in  infaEnvVar.items():
        log.debug("ev =%s v = %s" % (ev,v))
    try:     
        for ev, v in  infaEnvVar.items():
            x = os.environ[ev]
            exec  "%s='%s'" % (v,x) in globals()
            log.debug("%s='%s'" % (v,x))                     
                      
    except:     
            ret = 2
            log.error("ENV var not set %s %s\n" % (sys.exc_type,sys.exc_value))
          
    finally : return ret

# Use from script for standalone functions/
# Invoke notify from here.
def main(argv):

    # Set log
    fn = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    logFile = getLogHandler(fn,log)
    log.info('logFile= %s' % logFile)
    
    rc = _getEnvVars()
    if rc != 0 :
        log.error( "Need to set all env vars:\n %s" %  infaEnvVar.keys())
        sys.exit(rc)
    
    rc = bkupDir()
    log.info('bkupDir rc = %s ' % rc)
    subj = ("SUCCESS running backup on Server %s " % ('host')) if rc == 0 else ("ERROR running backup on Server %s " % ('host'))
    text = "Running backup on host %s. Please See logfile %s" % (hostname,logFile)

    #text += gText

    rc,msg=sm.sendNotif(rc,log,subj,text,[logFile,])
    log.info('logFile= %s' % logFile)
    log.info('Sending Notification\t%s rc = %s' % (msg,rc))
    
  
if __name__ == '__main__':
    rc=main(sys.argv)