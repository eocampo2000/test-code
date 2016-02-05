'''
Created on 20111027 
This module process server resources utilization data.
At this time this is for AIX servers
# EO 20120523  Added support for Linux, HP-UX and Windows
     20121001  New Style Class
     20121001  Added ssh for UX (pexpect). Moved out all os command strings to oscmdstr module. 
     20130417  Added helper methods for pxssh.
     
All method pre-defined commands run on the same platform as specified. They cannot run remote. 
However the classes provides the _execRemCmd Method that can be use to run 'any remote command'
At this point this module execute remote cmds from Windows to Unix and from UNIX to UNIX.    
As per C Morlock request parse for the following string as part of the result:  rc=
     
 Changed exception return codes to 10X, since 0 to 4 are OK for SAS !
 20150917 : Added pexpect global Exception in execRemCmd method.
@author: emo0uzx
'''
__version__ = ' 20150917'

import re
import sys
import inspect
import ctypes 

import proc.process   as p
import mainglob       as mg
import utils.strutils as su

#import bean.mainbean     as mbe
#from bean. import RemCredBean

sysp=sys.platform
if sysp != 'win32' : 
    import pxssh
    import pexpect

np = 10    # default number of process.
it = 10    # iterations
tm = 10    # time number of secs


# Base OS CmdDriver Class. Do not instantiate directly !!!
class _CmdDriver(object):

    def __init__(self,log):
        self.logger = log
    
    def execRemCmd(self,c,tgtOs):    
        rc = eval('self._execRem%sCmd(c)' % tgtOs)
        self.logger.debug('self._execRem%sCmd(c) rc = %s' % (tgtOs,rc))
        return rc
    
    def _execRemWinCmd(self,c):
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],sys.platform))
        return 100,100
    
    def _execRemCmd(self,c):
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],sys.platform))
        return 100,100
    
    def pingServer(self, host): 
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],sys.platform))
        return 100,100
   
    def _getPartions(self, host):
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],sys.platform))
        return 100,100
    
    def diskUsage(self, host):
        self.logger.warn('cmd %s Not Implemented for this platform : %s !!!' % (inspect.stack()[1][3],sys.platform))
        return 100,100

    # Get the remote process PID for handshaking and  process result.
    # Remote shell Script should capture the PID and echo it back immediately 
    # on a string PID=pid.

    def verifyPID(self,rMsg):

        self.logger.debug('rMsg is %s' % (rMsg))

        if len(rMsg) < 1 : return -1

        msg = [] ; rc = -2
        for item in rMsg.split('\n'):
            ln = item.strip()
            if ln.startswith('PID=') :
                p   = ln[4:]
                rc = su.toInt(p)           
                self.logger.debug('PID is %s' % rc)
                break          

            if ln != "" : msg.append(item.strip())

        return rc


    # rCode will return 0 if initiates a ssh session successfully.
    # Check the rMsg for errors even if rCode is 0 !       
    # Use negative error codes for error.
    def parseRemUxResp(self,rCode,rMsg):
         
        self.logger.debug('rCode=%s\trMsg is %s' % (rCode,rMsg))
       
        if isinstance(rMsg,int) : return rMsg

        if rCode    != 0 : return rCode
        if len(rMsg) < 1 : return -2
        
        msg = [] ; rc = -1 
        brkFlg = False
        for item in rMsg.split('\n'):
            fg  = item.find('Killed')
            if fg >= 0 : 
                self.logger.error('%s' % item)
                return -3
            
            fg = item.find('Terminated')
            if fg >= 0 :
                self.logger.error('%s' % item)
                return -4

            fg = item.find('not found')
            if fg >= 0 :
                self.logger.error('%s' % item)
                return -5
            
            fg = item.find('cannot execute')
            if fg >= 0 :
                self.logger.error('%s' % item)
                return -6
    
            # By convention we are using rc=, to indicate a return code. Should parse for this string
            # At the very end. 
            fg = item.find('rc=')
            if fg >= 0 :
                self.logger.info('item rc = %s' % item)
                item   = item[3:]
                brkFlg = True

            if item.strip() != "" : 
                self.logger.debug('Stripping <- %s -> ' % item)
                msg.append(item.strip())
                if brkFlg is True: break
                     
        if len(msg) > 0 :
            self.logger.debug('msg is %s' % msg)
            rc = su.toInt(msg[-1])
            if rc is None: return 100 # Partial return STR may had come. 
        
        return  rc
        
    
# Windows handler.
class CmdWinDriver(_CmdDriver):
    
    #  Arguments use for remote Cmd Exec rcb.
    def __init__(self,log,rcb):
        #_CmdDriver.__init__(self,log)
        super(CmdWinDriver,self).__init__(log)
        self.rcb = rcb
    
    # Success Packets: Sent = 2, Received = 2, Lost = 0 (0% loss)
    # Failure Packets: Sent = 2, Received = 0, Lost = 2 (100% loss)  
    def pingServer(self, host):    
        rv = 1
        cmd = "ping %s -n %d" % (host,2)
        self.logger.debug( "cmd = %s " % (cmd))
        rc, msg = p.runSync(cmd, self.logger)
        if (rc == 0):
            rv = su.findStr(msg,'(0% loss)')
        return rv       
    
    def diskUsage(self, host):
        free_bytes = ctypes.c_ulonglong(0)  
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(u'c:\\'), None, None, ctypes.pointer(free_bytes)) 
        print 'dont panic' , free_bytes.value
        return 0
         
    def _execRemUxCmd(self,c):
        cmd=r'%s\plink.exe -ssh %s@%s -pw %s -batch %s' % (mg.binDir,self.rcb.uname,self.rcb.sname,self.rcb.pwd,c) 
        self.logger.debug ("cmd = %s" % cmd)
        rCode,rMsg = p.runSync(cmd,self.logger)
        self.logger.debug('rCode = %s rMsg= %s' % (rCode,rMsg))
        return self._parseRemUxResp(rCode,rMsg)
         
class _CmdUXDriver(_CmdDriver):
        
    #import pxssh    
     
    def __init__(self,log,rcb):
        #_CmdDriver.__init__(self,log)
        super(_CmdUXDriver,self).__init__(log)
        self.rcb = rcb

    # Failure 2 packets transmitted, 0 received, 100% packet loss, time 1000ms    LINUX
    # Success 2 packets transmitted, 2 received,   0% packet loss, time 1000ms    LINUX
    # Failure 4 packets transmitted, 0 packets received, 100% packet loss         HP
    #         4 packets transmitted, 4 packets received, 0% packet loss           HP   
    #         Wrapper fx only, since different UX flavors have different ping cmd !
    
    def _pingServer(self,cmd):    
        rv = 1
        self.logger.debug( "cmd = %s " % (cmd))
        rc, msg = p.runSync(cmd, self.logger)
        if (rc == 0):
            rv = su.findStr(msg,' 0% packet loss')
        return rv            

    def getDiskUsage(self):
        rc,rm = self._execCmd(self.diskUse)
        if rc == 0 and len(rm) > 10: return self._parseDiskUse(rm)
        return 0

    def _getUsedSpace(self,tot,fre):
        retVal = 1.0
        try:
            t = float(tot)
            f = float(fre)
            retVal = (1 - f/t) * 100
   
        except ValueError:
            pass
        #print "getUsedSpace: Error %s %s " % (sys.exc_type,sys.exc_value)       
        #print "getUsedSpace: tot = %s fre = %s " % (tot,fre)
        finally:
            return (retVal)
        
    # Execute Remote commands (pexpect)
    # self.rcb.timeout blocking time, it is in minutes.
    # self.rcb.logtimeout in seconds.
    def execRemCmd(self,c,tgtOs):
        # Check numeric arguments:
        to    = su.toLong(self.rcb.timeout)
        if to is None :
            self.logger.error('timeout %s parameter needs to be an integer.' % ( self.rcb.timeout))
            return 101

        logto = su.toInt(self.rcb.logtimeout)
        if logto is None :
            self.logger.error('logging_timeout %s parameter needs to be an integer.' % ( self.rcb.logtimeout))
            return 102
        
        try:
            if to < 0 : 
                tos = None
                self.logger.debug('timeout = %d Defaulting to None' % (to))
            else      : 
                tos = to * 60
                self.logger.debug('timeout = %d * 60 = %d secs' % (to,tos))

            remProc = pxssh.pxssh(timeout=tos)
            remProc.force_password = True
            remProc.login (self.rcb.rhost, self.rcb.user, self.rcb.pwd, login_timeout=logto)
            
            self.logger.info('remote host=%s user=%s pwd=%s cmd = %s ' % ( self.rcb.rhost, self.rcb.user, 'XXXX', c))
            self.logger.debug('p %s ' % self.rcb.pwd)
            remProc.sendline(c)
            remProc.prompt() # match the prompt
            rMsg =  remProc.before
            self.logger.debug('RC = %s' % rMsg)
            remProc.logout()
            #return self._parseRemUxResp(0,rMsg)
            return rMsg
    
        except NameError, e:
            self.logger.error("name Error, need to run in UX OS", e)
            return 103
        
        except pxssh.ExceptionPxssh, e:
            self.logger.error('pxssh failed on login.. %s' % str(e))
            return 104      
        
        except pexpect.TIMEOUT,e:
            self.logger.error('Timeout %s min := %s' % (self.rcb.timeout,str(e)))
            return 105

        except pexpect.ExceptionPexpect, e:
            self.logger.error('pexpect failed on login.. %s' % str(e))
            return 106

        except OSError:
            self.logger.error('End session to ' + self.rcb.user + '@' + self.rcb.server)
            return 107
    
    
# HP 2 packets transmitted, 2 received, 0% packet loss, time 1000ms
class CmdLinuxDriver(_CmdUXDriver):
    def __init__(self,log,rcb):
        #CmdUXDriver.__init__(self,log)      
        super(CmdLinuxDriver,self).__init__(log,rcb)
        self.diskUse = 'df -h'  

    def pingServer(self,host):  
        return self._pingServer("ping %s -c %d" % (host,2) ) 

    
class CmdHPUXDriver(_CmdUXDriver):
    def __init__(self, log,rcb):
        #CmdUXDriver.__init__(self,log)  
        super(CmdHPUXDriver,self).__init__(log,rcb)
        self.diskUse = 'bdf'
    
    def pingServer(self,host):  
        return self._pingServer("ping %s -n%d" % (host,2) )           
    
class CmdAIXDriver(_CmdUXDriver):
    def __init__(self,log,rcb):
        #CmdUXDriver.__init__(self, sn,un,pwd,log) 
        super(CmdAIXDriver,self).__init__(log,rcb)
        self.diskUse = 'df -g'  

    def pingServer(self,host):  
        return self._pingServer("ping %s -c%d" % (host,2) ) 
    # Done for Unix (UX)  using df -k
    #Filesystem  1024-blocks   Free %Used   Iused %Iused Mounted on
    #/dev/hd4          65536   39292   41%   2061    19%  /


    # Done for Unix (UX)  using df -k
    #Filesystem  1024-blocks   Free %Used   Iused %Iused Mounted on
    #/dev/hd4          65536   39292   41%   2061    19%  /
    def _parseDiskUse(self,msg):
    
        dskList= [] 
    
        lst = msg.splitlines()
        conn = re.compile(r"Unable", re.IGNORECASE)
        resp = re.findall(conn,msg)
        if resp:
            for i in lst:
                dskList.append(i)
    
        else:
            for i in lst:
                li = i.split()
                fs,diskTot,diskFree,mp = li[0],li[1],li[2],li[6]
                usedSpace=self._getUsedSpace(diskTot,diskFree)
                dskList.append([fs,mp,diskTot,usedSpace])
      
        return dskList  
    


    # int np number of process to get. 
    # Returns a dict with the following keys: cpu, mem, run,dsk
    
    def getTopProc(self,np=10):
        d = {}
        rc,rm = self._execCmd( 'topProc' % (np,np,np,np))
        #rc,rm = self._execCmd( topProc )
        #return rm
        self.logger.debug( "rc = %s\nrm = %s" % (rc,rm))
        if rc == 0 and len(rm) > 10:
            lst = rm.split('=')
            i = 0 ; l = len(lst)
            while i < l:
                d[lst[i]] = lst[i+1]
                i+=2        
        return d 
    
    # Informatica proc pmtdm running WF proc
    # returns a list witn pmdtm processes.
    def getPmtdmProc(self):
        lst = []
        rc,rm = self._execCmd( 'pmtdmProc' )
        self.logger.debug( "rc = %s\nrm = %s" % (rc,rm))
        if rc == 0 and len(rm) > 10:
            lst = eval(rm)
        return lst

    # i Iterations
    # Reading part
    # returns a list with readings
    def _parseServUtilRead(self, i, read):
        #print "READ = %s" % read
        lst = []
        tl  = read.split()
        for i in range(i):
            lst.append(tl[i*21:i*21+21])
        return lst
    
    # Server Utilization
    # i number of iterations
    # t time in sec between iter
    # returns a tuple (system Info, list with readings)    
    def getServUtil(self,t,i):
        
        rc,rm = self._execCmd( self.servUtil % (t,i) )
        s = ('Error executing Shell rc = %s ' % rc,[])
        self.logger.debug( "rc = %s\nrm = %s" % (rc,rm))
        if rc == 0 and len(rm) > 200 : 
            id1 = rm.find("kthr")
            id2 = rm.find("se")
            if id1 < 1 or id2 < 1 : 
                return ('Error Parsing id1=%d id2=%d ' %(id1,id2), [])
            else:
                return ( rm[0:id1],                                 # System Info
                         self._parseServUtilRead(i, rm[id2+2:])     # Readings
                        )               
           
        return s
    
        
    def getls(self):
        rc,rm = self._execCmd( ls )
        print "IN GETLS RC = %s type(rm) %s len(rm) = %d " % (rc,type(rm),len(rm))
     
    # This method checks if a process is running.
    # p process name to search for.
    # returns True if running otherwise false    
    def chkProc(self,p):
        c = 'ps -C %s' % p
        rc,rm = self._execCmd(c)
        i = rm.find(p)
        self.logger.debug( "rc = %s\t%d \nrm = %s" % (rc,i,rm))
        if (rc != 0 or i < 0) : return False
        else                  : return True


# Instantiate the appropiate class !
# rcb RemCredBean. Use only for Remote Commands.
def getCmdClass(log,rcb = None):

    if len(sysp) > 4:
        sp = sysp[:5]
        log.debug('sysp = %s sp = %s' % (sysp,sp))
        if   sp == 'linux' : return CmdLinuxDriver(log,rcb)
        elif sp == 'hp-ux' : return CmdHPUXDriver(log,rcb)
        elif sp == 'win32' : return CmdWinDriver(log,rcb)
        else               : return None
