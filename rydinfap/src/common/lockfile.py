'''
Created on Jun 27, 2012

@author: eocampo
     New Style class
'''
__version__ = '20120923'

# flock.py
import os
import sys
import utils.fileutils as fu
import utils.strutils  as su
import proc.process    as ps

class LockFile(object):
    '''Class to handle creating and removing (pid) lockfiles'''

    def __init__(self, lckFn, log, pid = os.getpid()):
        self.pid   = pid
        self.lckFn = lckFn  
        self.log   = log
        self.lock  = False
        self.log.debug('Initializing: %s for pid %s' % (self.lckFn,self.pid))      
          
    # Try to get a lock. Returns True if lock is acquired, otherwise false.
    def getLock(self):
        
        # Check if a valid process lock exists ! 
        rc = self._chkIfValidLock()  
        
        if rc is True:
            self.log.warn('Cannot Create %s. Process is currently running!!' % self.lckFn)
            return False
 
        rc =  fu.createFile(self.lckFn,str(self.pid)) 
        if rc == 0 : 
            self.log.info('Created lock %s for PID = %s' % (self.lckFn,self.pid))
            self.lock  = True
            return True
        else       : 
            self.log.error('Could not create Lock %s for PID = %s' % (self.lckFn,self.pid))
            return False
    
    # This method checks if a lock is valid.
    def _chkIfValidLock(self):
        
        rc = True
        # Check if there is a lock file.
        if  fu.fileExists(self.lckFn):
            sPid = fu.readFile(self.lckFn)
            pid = su.toInt(sPid)
            self.log.info('Lock File  %s EXISTS. sPid = %s , pid = %s' % (self.lckFn,sPid,pid))
            # Check if file has a valid PID (number)
            if pid is None : 
                rc = fu.delFile(self.lckFn)      
                if rc == 0 : 
                    self.log.info('Removed File  %s' % (self.lckFn))
                else:
                    self.log.error('Could not removed File  %s' % (self.lckFn))
                return False
            
            # If pid is a valid number, check if the process is running ...
            rc = ps.isProcRunning(pid,self.log)
            self.log.debug('isProcRunning returned %s' % (rc))
            return rc
        
        # No lock file exists.
        else: return False    
    
    # Release the lock file
    def relLock(self):
        
        if self.lock:
            try:
                fu.delFile(self.lckFn)
                self.log.info('Released lock: %s for pid %s' % (self.lckFn,self.pid))
                return 0
                   
            except:
                self.log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))
                return 1
        else:
            self.log.warn('No lock to release for: %s for pid %s' % (self.lckFn,self.pid))
            return 0
    # Destructor
    def __del__(self):
        # Do not remove an existing valid lock file !
        rc=self.relLock()
        self.log.debug('Automatic Lock Cleanup  for: %s for pid %s rc = %s' % (self.lckFn,self.pid,rc))
                
        

