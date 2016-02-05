'''
Created on Jul 4, 2012

@author: eocampo

New Class Style.

Mainframe syntax from shell:
export FILE="\'P.HP580D40\'"

Added mget capability. Use python regex expressions.
20130502 Added rename capabilty and rxfile var for regexp. 
20150102 Modified for explicit ftp mode type.
20150112 Added main routine for standalone.
'''
__version__ = '20150112'

import ftplib as ftl
import sys, os, socket
import re

import utils.strutils  as su
import utils.fileutils as fu
import common.log4py   as log4py 
import common.simpmail as sm
from   common.loghdl import getLogHandler

# Environment Variables that need to be set. Key is the ENV and ELE the name of var.
# Below are global variables.
hostname    = socket.gethostname()
FTP_TIMEOUT = socket._GLOBAL_DEFAULT_TIMEOUT
FTP_PORT    = 21
FTP_VERBOSE = 0
FTP_MODE    = 'BIN'

# Explicit actions, for ease of invocation.
ftpAct = { 'get'   : 'ABCDGZ',
           'getn'  : 'ABDGZ',    # Do not change dir          
           'mget'  : 'ABCDEZ',
           #'get-d' : 'ABCDGRZ', For this operation should set env var to true.
           'mput'  : 'ABCDNZ',
           'put'   : 'ABCDPZ',
           'list'  : 'ABCDJZ',
           'listd' : 'ABCDHZ',
           'listp' : 'ABCDIZ',
           'del'   : 'ABCDRZ',
           'test'  : 'ABCDZ',}


# Base class for File Transfers
class _BaseFT(object):
    exitOnError = True   # default.   
    
    def __init__(self, appName, log):      
        
        if appName is None : self.appName = self.__class__.__name__.lower()
        else               : self.appName = appName
        
        if log is not None : 
            self.log = log          # Overwrite the default. 
            self.stdAlone = False
               
        else               : 
            self.log      = log4py.Logger().get_instance() 
            self.logFile  = getLogHandler(appName,self.log)
            self.stdAlone = True                     # Standalone invocations (notify)
            
        self.cwd    = None
        self.rpwd   = None        # Remote Working dir
        self.lpwd   = None        # Local Working  dir
        
        self.dirDetLst  = []      # Detailed File list
        self.dirLst     = []      # place holder for dir list (filename only)
        self.dirPathLst = []      # place holder for dir list
        self.txFiles    = []      # transfer files. List of SUCC transfered files. 
        
        self.cmdStep    = {}      # Empty for base class
        self.infaEnvVar = {}      # Empty for base class  
                        
    # Environment Variables that need to be set. Key is the ENV and ELE the name of var.
    # Below are global variables. Env variables should be set based on env settings.    
    def _getFtpEnvVars(self):
        ret = 0   
        for ev, v in  self.infaEnvVar.items():
            self.log.debug("ev =%s v = %s" % (ev,v))
            
        # Optional Env Vars
        self.ftpVerb    = os.environ['FTP_VERB']         if os.environ.has_key('FTP_VERB')      else FTP_VERBOSE
        self.ftpPort    = os.environ['FTP_PORT']         if os.environ.has_key('FTP_PORT')      else FTP_PORT
        self.ftpTimeout = os.environ['FTP_TIMEOUT']      if os.environ.has_key('FTP_TIMEOUT')   else FTP_TIMEOUT
        self.ftpMode    = os.environ['FTP_MODE'].upper() if os.environ.has_key('FTP_MODE')      else FTP_MODE
    
        delRemFile      = os.environ['DEL_REM_FILE'] if os.environ.has_key('DEL_REM_FILE')  else 'False'
        try    : self.delRemFile = eval(delRemFile.capitalize())
        except : self.delRemFile = False

        self.log.debug("FTP_VERB    = %s" % self.ftpVerb)
        self.log.debug("FTP_PORT    = %s" % self.ftpPort)
        self.log.debug("FTP_TIMEOUT = %s" % self.ftpTimeout)
        self.log.debug("FTP_MODE    = %s" % self.ftpMode)
        self.log.debug("DEL_REM_FILE = %s self.delRemFile %s " % (delRemFile,self.delRemFile))
            
        #Mandatory
        try:     
            for ev, v in  self.infaEnvVar.items():
                x = os.environ[ev]
                exec  "%s='%s'" % (v,x) 
                self.log.debug("%s='%s'" % (v,x))                   
             
        except:
                ret = 2
                self.log.error("ENV var not set %s %s\n" % (sys.exc_type,sys.exc_value))
    
        finally : return ret
        
    def _execFTSteps(self,runStep):
        
        self.log.debug('runStep = %s' % runStep)
        for s in runStep:
            if (not self.cmdStep.has_key(s)):
                self.log.error('Invalid step %s' %s)
                return 1
            
            rv = 1
            try:
                rv = self.cmdStep[s]()
                if rv != 0 and self.exitOnError : 
                    self.log.error('[%s]:%s()\trc\t= %d' % (s,self.cmdStep[s].__name__,rv))
                    return rv
                
                self.log.debug('[%s]:%s()\trc\t= %s' % (s,self.cmdStep[s].__name__,rv))
                self.log.info('[%s]:%s()\trc\t= %d'  % (s,self.cmdStep[s].__name__,rv))
            
            except AttributeError:
                self.log.error( '[%s]:%s()' % (s,self.cmdStep[s].__name__))
                self.log.error( '%s ==> %s ' % (sys.exc_type, sys.exc_value))
                if (self.exitOnError) : return rv
                
            except SyntaxError:
                self.log.error( '[%s]:%s()' % (s,self.cmdStep[s].__name__))
                self.log.error( '%s ==> %s ' % (sys.exc_type, sys.exc_value))
                if (self.exitOnError) : return rv
            
        return rv

    def FtMain(self, seq):
        
        rc = 0  # Success
        
        # should NEVER get this programmatic error !!!!
        if self.cmdStep is None or len(self.cmdStep) == 0 :
            self.log.error("Program Error:self.cmdStep is ",self.cmdStep)
            return 1
               
        runSeq = seq

        rc = self._getFtpEnvVars()
        if rc != 0 :
            self.log.error( "Need to set all env vars:\n %s" %  self.infaEnvVar.keys())
            return rc
           
        if runSeq is not None and len(runSeq) > 0 : 
            rc = self._execFTSteps(runSeq)
            if rc == 0 : self.log.info ('Completed running execSteps rc = %s' % rc)
            else       : self.log.error('execSteps rc = %s' % rc)
        
        # Only for standalone runs, otherwise invoking app should manage notifications.
        if self.stdAlone:
            text = 'Please see logFile %s' % self.logFile
            subj = "SUCCESS running %s on %s " %(self.appName,hostname ) if rc == 0 else "ERROR running %s on %s" % (self.appName,hostname)
            r,msg=sm.sendNotif(rc,self.log,subj,text,[self.logFile,])
            self.log.info('Sending Notification\t%s rc = %s' % (msg,r))    
        
        return rc
    
class Ftp(_BaseFT):
   
    # ftpVerb = 0  turn off
    # ftpVerb = 1  base mode
    # ftpVerb = 2  debug mode
    # appName = Invoking app name. None is for standalone.
    # log = Invoking app log handler. None is for standalone.
    
    def __init__(self, appName=None,log=None):
        #_BaseFT.__init__(self,appName,log)
        super(Ftp,self).__init__(appName,log)
        self.ftp    = ftl.FTP()   # ftplib (ftl)
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.connect           ,
                         'B' : self.login             ,
                         'C' : self.ftpServCwd        ,
                         'D' : self.ftpServPWD        ,
                         'E' : self.ftpMGet           ,
                         'G' : self.ftpGet            ,
                         'P' : self.ftpPut            ,    
                         'N' : self.ftpMput           ,  
                         'R' : self.ftpDelFile        , 
                         'H' : self.ftpServDirDetLst  ,  
                         'I' : self.ftpServDirPathLst ,  
                         'J' : self.ftpServDirLst     ,  
                       # 'G' : self.notifyUsers,
                         'Z' : self.disconnect        ,
                        }
    
        self.infaEnvVar = {
              'REMOTE_HOST': 'self.remHost' ,
              'USER'       : 'self.user'    ,
              'PWD'        : 'self.pwd'     , 
              'REMOTE_DIR' : 'self.remDir'  ,
              'LOC_DIR'    : 'self.locDir'  ,
              'FILE'       : 'self.file'    ,                  #USe to  GET , PUT  (list), 
              'RXFILE'     : 'self.rxfile'  ,                  #Use for MGET, MPUT (string)              
              #'FTP_VERB'   : 'self.ftpVer'  , optional
              # FTP_PORT     : 'self.ftpPort' optional
              # FTP_TIMEOUT  : 'self.ftpTimeout', optional
              }
      
        
    def getTXFiles(self): return self.txFiles
  
    # Sets the Remote Host Connection variables.
    def _setConnParam(self):
        #rc = _BaseFT._getFtpEnvVars(self)   
        rc = self._getFtpEnvVars()   
        if rc != 0 : return rc
        ftpVerb = su.toInt(self.ftpVerb)
        if ftpVerb is None: 
            self.log.error('FTP_VERB %s needs to be an integer. Setting to default %d' % (self.ftpVerb,FTP_VERBOSE))
            self.ftpVerb  = FTP_VERBOSE
        else: 
            self.ftpVerb  = 0 if ( ftpVerb < 0 or ftpVerb > 2 ) else ftpVerb
            self.log.debug('FTP_VERB : %d ' % self.ftpVerb)
            
        ftpPort = su.toInt(self.ftpPort)
        if ftpPort is None: 
            self.log.error('FTP_PORT : %s needs to be an integer.Setting to Default %d ' % (self.ftpPort,FTP_PORT))
            self.ftpPort  = FTP_PORT                
        else:
            self.ftpPort = ftpPort
            self.log.debug('FTP_PORT : %s' % (self.ftpPort))
                               
        ftpTimeout = su.toFloat(self.ftpTimeout)
        if ftpTimeout is None: 
            self.ftpTimeout = FTP_TIMEOUT
            self.log.error('FTP_TIMEOUT : %s needs to be a float .Setting to Default %s' % (self.ftpTimeout, FTP_TIMEOUT))              
        else:
            self.ftpTimeout = ftpTimeout
            self.log.debug('FTP_TIMEOUT : %s' % (self.ftpTimeout))
        return rc        
    ## --------------------------- Connection Methods ------------------------------
    def connect(self):
        rc = 1
        try:            
            rc = self._setConnParam()
            if rc != 0 : return rc 

            self.log.info('Trying to connect to remHost %s' % self.remHost)

            self.ftp.connect(self.remHost, self.ftpPort, self.ftpTimeout)
            self.ftp.set_debuglevel(self.ftpVerb)
          
            self.log.debug('Connection Successful : self.ftp = ',self.ftp )
            rc = 0
        except:
            self.log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))
            self.ftp  = None
            
        finally : return rc
        
    def login(self):
        rc = 1
        self.log.debug('Login to remHost %s user = %s pwd = %s' % (self.remHost,self.user,self.pwd ))
        try:
            self.ftp.login(self.user,self.pwd)
            rc = 0
        except:
            self.log.error('Invalid Login  % remHost %s user = %s pwd = %s' % (self.remHost,self.user,self.pwd))
            self.log.debug("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))          
        
        finally : 
            
            return rc
    ## --------------------------- Navigation/General Methods ------------------------------
    # Try to change to this self.remDir. To check if exist and permissions.
    def ftpServCwd(self):
        rc = 1
        self.log.debug('Trying to change dir on remote remHost to  %s ' % (self.remDir))
        try:
            self.log.debug('Remote Server CWD is ', self.ftp.cwd(self.remDir))
            rc = 0
            
        except:
            self.log.error('Cannot change to dir %s on remote remHost.' % (self.remDir))
            self.log.debug("==EXCEP %s %s" % (sys.exc_type,sys.exc_value))          
        
        finally : return rc
          
    def ftpServPWD(self):
        self.rpwd  = self.ftp.pwd() 
        self.log.debug('Remote Server PWD % s' % (self.rpwd))
        return 0
    
    def ftpSetPasv(self,fg=True): self.ftp.set_pasv(fg)    
    
    def _ftpServDir(self,a):
        
        if self.remDir is not None and a == 'pathLst':
            return self.ftp.nlst(self.remDir)    # ['/home/infa/eotest/ftpt/rwf.sh', '/home/infa/eotest/ftpt/rwf.txt']
        
        if  a == 'lst': 
            return self.ftp.nlst()               #  ['rwf.sh', 'rwf.txt']
    
        return []
    
    #  List detailed  directory contents
    #  dir to list, if None will use CWD
    # '-rw-r--r--    1 1000     200             0 Aug 30 22:35 FBAP.TXT', '-rw-r--r--'
    def ftpServDirDetLst(self):
        if self.remDir is not None:
            self.ftp.dir(self.remDir,self.dirDetLst.append)
        else:
            self.ftp.dir(self.dirDetLst.append) 
            
        self.log.info('Lst',self.dirDetLst) 
        return 0

    # List filenames only on the pwd/cwd
    #  ['rwf.sh', 'rwf.txt']
    def ftpServDirLst(self): 
        rc = 0
        self.dirLst  = self._ftpServDir('lst')
        if len(self.dirLst) < 1:
            self.log.error('No files found on PWD  %s' % self.rpwd)
            rc = 1
        
        self.log.info('Lst',self.dirLst) 
        return rc
    
    # List files with the full path.
    # ['/home/infa/eotest/ftpt/rwf.sh',
    def ftpServDirPathLst(self):
        rc = 0
        self.dirPathLst = self._ftpServDir('pathLst')
        if len(self.dirPathLst) < 1:
            self.log.error('No files found on REMDIR %s' % self.remDir)
            rc = 1      
        
        self.log.info('Lst',self.dirPathLst) 
        return rc
    

    ## --------------------------- Get/Download Methods ------------------------------
    # fn is the filename (note: dir needs to be already set to the where file is !)
    # outf is the file handler.
    # use a lambda to add newlines to the lines read from the server           
    def _getText(self,fn,_outf):
        self.log.debug('Text Mode : get file %s' % (fn ))
        rc =self.ftp.retrlines("RETR " + fn, lambda s, w=_outf.write: w(s+"\n"))
        return rc

    # fetch a binary file
    # fn is the filename 
    # outf is the file handler.
    def _getBinary(self,fn,_outf):
        self.log.debug('Binary Mode : get file %s ' % (fn ))
        rc = self.ftp.retrbinary("RETR " + fn, _outf.write)
        return rc

#    for root, dirs, files in os.walk('path/to/local/dir'): 
#    for fname in files: 
#        full_fname = os.path.join(root, fname) 
#        ftp.storebinary('STOR remote/dir' + fname, open(full_fname, 'rb')) 
                        
    # Not to be invoked directly.       
    def _ftpGetFile(self,fn):
        rc =1 
        outf = None
        self.log.info('Getting File - %s - FROM %s : %s  ' % (fn,self.remHost, self.remDir ))
        self.log.info('                      TO %s : %s  ' % (hostname, self.locDir ))   
        f = '%s/%s' % (self.locDir, fn)
        
        try:

            if ( self.ftpMode == 'TXT' ) :
                outf = open(f,"w")                         # Add lcoal direcrory
                r = self._getText(fn,outf)
                self.log.debug("r = %s" % r) 
                outf.close()
                if self.delRemFile is True:
                    r = self._ftpDelRemFile(fn)
                rc = 0
                
            else:
                outf = open(f,"wb")
                r = self._getBinary(fn,outf)  
                self.log.debug("r = %s" % r) 
                outf.close()
                rc = 0 
                
        except:
            # rm local created file.
            try   : 
                sz = os.path.getsize(f)
                self.log.debug('Removing %s size %d bytes' % (f,sz) )
                outf.close()
                os.unlink(f)
            except: self.log.error('Cannot remove local file %s . ' % (f))
            self.log.error('Cannot get (read) file %s from remHost %s . ' % (fn,self.remHost))
            self.log.debug("==EXCEP %s %s" % (sys.exc_type,sys.exc_value)) 
        
        finally : return rc
    
    # self.file is a string of the form "file1,file2,..."
    # Uses FILE env var.
    def ftpGet(self):
        rc = 0
        fns = self.file.split(',')
        for f in fns:
            r = self._ftpGetFile(f)
            if r == 0 : self.txFiles.append(f)
            else      : rc+=r
        return rc
    # fl is the file list and use regexp      
    
    # MGET Check Reg Exp for Python.
    # If no pattern is found then ret 0
    # Uses RXFILE env var.
    def ftpMGet(self):
        rc = 0
        fns = self._ftpServDir('lst')
        if len(fns) < 1 : return 1
        
        self.log.debug("File List in Remote Host", fns)
        p = re.compile(self.rxfile)
        for f in fns:
            if p.search(f):
                self.log.debug('file %s matched pattern %s' % (f,self.rxfile))
                r = self._ftpGetFile(f)
                if r == 0 : self.txFiles.append(f)
                else      : rc+=r
                
            else: self.log.debug('file %s DID NOT matched pattern %s' % (f,self.rxfile))
                
        return rc
    ## --------------------------- Put/upload Methods ------------------------------

    def _putText(self,fn,_fh):
        self.log.debug('Text Mode : put file %s' % (fn ))
        rc = self.ftp.storlines("STOR " + fn, _fh)
        return  rc
    
    def _putBinary(self,fn,_fh):
        self.log.debug('Binary Mode : put file %s' % (fn ))
        rc = self.ftp.storbinary("STOR " + fn, _fh,1024)
        return  rc

    def _ftpPutFile(self,fn):
        rc = 1
        try:   
            f = '%s/%s' % (self.locDir, fn)
            if not os.path.isfile(f):
                self.log.error('Cannot find file %s on local machine ' % (f))
                return rc
            
            #f = os.path.basename(fn)
            self.log.info('Putting File - %s - FROM %s : %s  ' % (fn, hostname, self.locDir ))
            self.log.info('                      TO %s : %s  ' % (self.remHost, self.remDir )) 
            if ( self.ftpMode == 'TXT' ) :
                outf = open(f)
                r = self._putText(fn,outf)
                self.log.debug("r = %s" % r) 
                outf.close()
                rc = 0
                
            else:
                outf = open(f, "rb")
                r = self._putBinary(fn,outf)  
                self.log.debug("r = %s" % r) 
                outf.close()
                rc = 0
                           
        except:
            self.log.error('Cannot put file %s to remote remHost. ' % (f))
            self.log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value)) 
        
        finally : return rc     
    
    # Use this to put 'ALL FILES that match from a src dir
    def ftpMput(self):
        rc = 0
        fns = fu.getFileName(self.locDir, self.rxfile)
        for f in fns:
            r = self._ftpPutFile(f)
            if r != 0 : rc+=r
        return rc    
    
    def ftpPut(self):
        rc = 0
        fns = self.file.split(',')
        for f in fns:
            r = self._ftpPutFile(f)
            if r != 0 : rc+=r
        return rc    
    

    # d  absolute dir 
    # fn with path
    def _ftpRenFile(self,sfn,tfn):  
        rc = 'Error'
        try:
            
            self.log.debug('Renaming %s to %s' % (sfn,tfn))
            rc = self.ftp.rename(sfn,tfn)
        except:
            self.log.error('Cannot renane file %s to %s. ' % (sfn,tfn))
            self.log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value)) 
        
        finally : return rc             
            
          
    # renFile is a list containing (src,dst) tuples.
    # src is a filename in the PWD and dst has a relative path of PWD
    def ftpRenFile(self, renFiles):
        rc = 0
        if len(renFiles) < 1:
            self.log.error('No files has been specified to rename')
            return 1
        
        pwd = self.ftp.pwd()
        
        
        self.log.debug('rename File list', renFiles)
        for ele in renFiles:
            sfn,dfn = ele
            sf = '%s/%s' % (pwd,sfn)
            tf = '%s/%s' % (pwd,dfn)
            rv = self._ftpRenFile(sf,tf)
            r = su.findStr(rv,'250')
            if r != 0 : rc+=r
            self.log.debug('rv = %s r = %s' % (rv,r))
        return rc   
    
    def _ftpDelRemFile(self,fn):
        rc = 1
        f = '%s/%s' % (self.remDir, fn)       
        try:
            rc = self.ftp.delete(f)
            self.log.info('Deleted file %s from Remote Server %s ' % (self.remHost, self.remDir )) 
            self.log.debug('ftp rc =  %s ' % (rc )) 
            rc = 0
           
        except:
            self.log.error('Cannot delete file %s from remote remHost. ' % (f))
            self.log.error("==EXCEP %s %s" % (sys.exc_type,sys.exc_value)) 
        finally : return rc  
    
    def ftpDelFile(self):
        rc = 0
        fns = self.file.split(',')
        for f in fns:
            r = self._ftpDelRemFile(f)
            if r != 0 : rc+=r
        return rc   
    
    
    def ftpDelRemDir(self):
        pass
    ## -------------------------- Disconnect/ Cleanup methods --------------------------

        return 0
    
    def disconnect(self):
        self.log.debug('Disconnecting from remHost %s' % self.remHost)
        if self.ftp is None :
            self.log.warn('self.ftp is None. cannot Close ') 
        else :
            rc = self.ftp.close()
            self.log.debug('rc = %s' % rc )
        
        return 0
    
    # Invoke this method when running standalone.
    # Environment Variables that need to be set. Key is the ENV and ELE the name of var.
    def ftpgetEnvVars(self):
        rc = _BaseFT._getFtpEnvVars(self)
        #rc = self._getFtpEnvVars()
        #rc = self._getFtpEnvVars()           
        if rc != 0 : return rc        
        
        # Optional Env Vars
        self.ftpVerb    = os.environ['FTP_VERB']     if os.environ.has_key('FTP_VERB')      else FTP_VERBOSE
        self.ftpPort    = os.environ['FTP_PORT']     if os.environ.has_key('FTP_PORT')      else FTP_PORT
        self.ftpTimeout = os.environ['FTP_TIMEOUT']  if os.environ.has_key('FTP_TIMEOUT')   else FTP_TIMEOUT
        self.ftpMode    = os.environ['FTP_MODE'].upper() if os.environ.has_key('FTP_MODE')  else FTP_MODE
        delRemFile      = os.environ['DEL_REM_FILE'] if os.environ.has_key('DEL_REM_FILE')  else 'False'
        try    : self.delRemFile = eval(delRemFile.capitalize())
        except : self.delRemFile = False
          
        self.log.debug("FTP_VERB     = %s" % self.ftpVerb) 
        self.log.debug("FTP_PORT     = %s" % self.ftpPort) 
        self.log.debug("FTP__TIMEOUT = %s" % self.ftpTimeout) 
        self.log.debug("FTP_MODE     = %s" % self.ftpMode)
        self.log.debug("DEL_REM_FILE = %s self.delRemFile %s " % (delRemFile,self.delRemFile))
        return 0
     
        
    # Invoke this method when using the class as a client from a config file.
    # Environment Variables that need to be set. Key is the ENV and ELE the name of var.    
    def _getFtpVar(self,remHost, user,pwd, remDir, locDir, port=FTP_PORT, ftpVerb=FTP_VERBOSE, ftpTimeout = FTP_TIMEOUT):
        self.remHost    = remHost
        self.user       = user
        self.pwd        = pwd
        self.remDir     = remDir
        self.locDir     = locDir
        self.ftpPort    = port
        self.ftpTimeout = ftpTimeout
        self.ftpVerb    = ftpVerb
        
    
    # Destructor
    def __del__(self):
        # Close the connection always
        rc=self.disconnect()
        self.log.debug('Automatic Disconnection from remHost: %s and  user %s rc = %s' % (self.remHost,self.user,rc))
        return rc


class SFtp:
    pass


# Ftp Interfaces.



# main use standalone calls back compatibilty.
def main(Args):
        #print "0 = ", Args[0]," 1 = ", Args[1]," 2 = ", Args[2]
        if len(Args) < 2 :
            print ( "USAGE : <%s> fx -a [%s] : " % (Args[0],sorted(ftpAct.keys())))
            return 1

        if Args[1]  == '-a':
            act = Args[2].strip()
            print "ACT = %s " % act
            if not ftpAct.has_key(act):
                print ( "Invalid -a [%s] . Valid actions %s " % (act,sorted(ftpAct.keys())))
                return 2
            seq = ftpAct[act]

        else:
            seq =Args[1]
            
        # Invoke Ftp
        a = Ftp()
        rc = a.FtMain(seq)
        return rc

def get(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['get']
    rc = a.FtMain(seq)
    return rc

def getn(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['getn']
    rc = a.FtMain(seq)
    return rc

def mget(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['mget']
    rc = a.FtMain(seq)
    return rc

def put(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['put']
    rc = a.FtMain(seq)
    return rc

def listfd(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['listd']
    rc = a.FtMain(seq)
    return rc

def listf(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['list']
    rc = a.FtMain(seq)
    return rc

def listfp(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['listp']
    rc = a.FtMain(seq)
    return rc

def delf(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['del']
    rc = a.FtMain(seq)
    return rc

def testf(appName = None,log = None ):
    a   = Ftp(appName,log)
    seq = ftpAct['test']
    rc = a.FtMain(seq)
    return rc

if __name__ == '__main__':
    rc=  main(sys.argv)

