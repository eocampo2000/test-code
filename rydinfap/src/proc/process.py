#-------------------------------------------------------------------------------
# Version 1.0  20090807
#
#process.py
#
#
# Creation Date:     2012/01/03
# Modification Date:
# Description: This module contains functions that deal
# with Processes. In newer version 2.4 and above use the 
# coprocess module.
#
# Contain OS specific commands
# EO Added apply_async_pool.
#------------------------------------------------------------------------------
__version__ = '20130212'

import sys
import subprocess  
import os
import signal
import errno

SUCCESS =  0
ERROR   = -1
EXCEP   = -3

import multiprocessing as mp

def runSync(cmd,logger):
    retVal = (ERROR,'Error')
    
    try:
        logger.debug(":: cmd %s" % cmd)
        #-------------- Platform Specific Cmd :Start
        if(sys.platform == 'win32'):p = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        else                       :p = subprocess.Popen(cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE,close_fds=True) 
        #-------------- Platform Specific Cmd :End
       
        rMsg   = p.communicate()[0]
        rCode  = p.wait()
      
        retVal = ( rCode, rMsg )
      
        logger.debug("rv=%s p.rc=%s\np.msg ======= %s ======== " %    (rCode , p.returncode, rMsg))
        if(rCode != SUCCESS):
            logger.error("rv=%s rc=%s\nmsg ========= %s ========= " % (rCode, p.returncode, rMsg))
       
    except:
        logger.debug("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))
        retVal = (EXCEP,rMsg)
    
    finally:
        return retVal
    
# pn process name to find.    
def getProcStat(pn,logger):
    retVal = -1
    
    logger.debug("process name  %s" % pn)
    if(sys.platform == 'win32'): 
            logger.error("This process is not implemented for windows at this time !")
            return retVal 
    else                       :
                 
            for line in os.popen("ps au"):
                flds = line.split()
                process = flds[7]
                logger.debug('user %s pid = %s proc = %s pn = %s' % (flds[0], flds[1],process, pn ))
                if process.find(pn) > 0 : return 1
            return 0     


def isProcRunning(pid,logger):
    if(sys.platform == 'win32'): 
            logger.error("This process is not implemented for windows at this time !")
            return True
    else:
            return  isUXProcRunning(pid,logger)

def isUXProcRunning(pid,logger):
    
    try:
        os.kill(pid, 0)
        return True
    
    except OSError, e:
        if errno.EPERM == e.errno:
            return os.path.isdir("/proc/%d" % (pid))
        
        logger.warn("Process %s Not Running exc_cd = %s" % (pid,e.errno))
        return False
    # Should not get in HERE , if it does try it as a running process.
    except: 
        logger.error("Please check manually pid %s in server ! EXCEP %s %s" % (pid,sys.exc_type,sys.exc_value))
        return True 
# pn process name to find.    
def killProc(pn,logger):
    retVal = -1
    
    logger.debug("process name  %s" % pn)
    if(sys.platform == 'win32'): 
            logger.error("This process is not implemented for windows at this time !")
            return retVal 
    else                       :
                 
            for line in os.popen("ps au"):
                flds = line.split()
                pid = flds[1]
                process = flds[7]
                logger.debug('user %s pid = %s proc = %s pn = %s' % (flds[0], flds[1],process, pn ))
                if process.find(pn) > 0 : 
                    os.kill(int(pid),signal.SIGKILL)
            return 0       
        

result_list = []
def log_result(result):
    name = mp.current_process().name
    print "Name is %s PID = %d " % (name,os.getpid())
    # This is called whenever foo_pool(i) returns a result.
    # result_list is modified only by the main process, not the pool workers.
    result_list.append(result)

# Asynchronous call with callback()
# f     call back function.  
# a      args list of arguments  
# psz   pool size
def apply_async_pool(f,a ,psz, logger):

    pool = mp.Pool(psz)
    
    
    for i in range(psz):
        print "ITER ", i
        pool.apply_async(f, args = a, callback = log_result)
    pool.close()
    pool.join()
    return result_list


    