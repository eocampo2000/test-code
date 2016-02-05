'''
Created on Aug 24, 2015

@author: eocampo

Added Temporary ora_db until tnsnames.ora gets updated
'''

__version__ = '20150827'

import os,sys, socket

from sets import Set
import common.log4py     as log4py 
import utils.strutils    as su
import utils.fileutils   as fu
import proc.process      as proc
import cmd.oscmd         as oc
import cmd.dbcmd         as dbc                # DO not comment
import common.simpmail   as sm


from common.loghdl     import getLogHandler

ggs_res = """

Oracle GoldenGate Command Interpreter for Oracle
Version 11.2.1.0.22 18594233 OGGCORE_11.2.1.0.0OGGBP_PLATFORMS_140503.0605_FBO
Linux, x64, 64bit (optimized), Oracle 11g on May  3 2014 10:44:15

Copyright (C) 1995, 2014, Oracle and/or its affiliates. All rights reserved.



GGSCI (rsroldp1.ryder.com) 1>
Program     Status      Group       Lag at Chkpt  Time Since Chkpt

MANAGER     RUNNING
JAGENT      RUNNING
EXTRACT     RUNNING     EXT1        00:00:02      00:00:01
EXTRACT     RUNNING     EXT2        00:00:00      00:00:04
EXTRACT     RUNNING     EXT3        00:00:02      00:00:08
EXTRACT     RUNNING     EXT4        00:00:02      00:00:03
EXTRACT     RUNNING     EXT5        00:00:03      00:00:00
EXTRACT     RUNNING     PMP1        00:00:02      00:00:01
EXTRACT     RUNNING     PMP2        00:00:03      00:00:09
EXTRACT     RUNNING     PMP3        00:00:00      00:00:06
EXTRACT     RUNNING     PMP4        00:00:00      00:00:05
EXTRACT     RUNNING     PMP5        00:00:00      00:00:09
EXTRACT     ABBEND      EXT6        00:00:03      00:00:00


GGSCI (rsroldp1.ryder.com) 2>

"""


# ls =['a','b','c','d']
# d = { k : -1 for k in ls }
exitOnError = True
hostname = socket.gethostname()
# Email GLobal options. 
gSubj   = ''
gText   = ''
gAttach = []

gProgRes = {}     # Program Results
gGrpRes  = {}     # Group Results     key : GroupNAme  : (Status, Lag , chkpt)

#Get the Logname 
log     =  log4py.Logger().get_instance()
cmdInst = oc.getCmdClass(log)
class StStruct: pass
ib      = StStruct() #ibe.InfaCmdBean()
ora_db = '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = rolpdrac-scan)(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME = rolpd)))'

# Environment Variables that need to be set. Key is the ENV and ELE the name of var.
# Below are global variables.
infaEnvVar   = {
                'CONFIG_FILE'  : 'ib.configFile',
}

# For TM is an Oracle Server
def pingOraSrv():
    rc = cmdInst.pingServer(ib.ora_dbserver)
    return rc

# For TM is a Windows Server
def pingMSqlSrv():
    rc = cmdInst.pingServer(ib.ms_dbserver)
    return rc

def tnspingOraDB():
    #ib.db_name = ib.ora_db
    ib.db_name = "'%s'" % ora_db
    cmdDB      = dbc.ORADBCmd(ib,log)
    rv         = cmdDB.pingDB()
    log.debug('rv', rv)
    return rv   

def connOraDB():
    ib.db_user = ib.ora_user; ib.db_pwd = ib.ora_pwd ; # ib.db_name = ib.ora_db
    ib.db_name = ora_db
    cmdDB      =  dbc.ORADBCmd(ib,log)
    rv         = cmdDB.connToDB()
    log.debug('rv', rv)
    return rv   

def connMSQLDB():
    ib.db_user = ib.ms_user; ib.db_pwd = ib.ms_pwd ; ib.db_name = ib.ms_db ; ib.db_server = ib.ms_dbserver
    cmdDB      = dbc.MSSQLDBCmd(ib,log)
    rv         = cmdDB.connToDB()
    log.debug('rv', rv)
    return rv   
# For process status Running Returns 0
def _getProcStat(p):
    if p == 'RUNNING' :  return 0
    return 1

def _getLagTime(tm):
    if ib.GGS_lag < 0 : return 0
    l = su.convToSecs(tm)
    if l < ib.GGS_lag : return 0
    return 1    

def _getChkPtTime(tm):
    if ib.GGS_chkpt < 0 : return 0
    l = su.convToSecs(tm)
    if l < ib.GGS_chkpt : return 0
    return 1    

# EO TODO Add process to dictonary
#         Add group to dicitonary.
# At the end parse the dictionay with chosen or all keys .
# Main parser for the 
def _parseGGSCI(ggStr):
   
    if isinstance(ggStr,int) : return ggStr 
    rc = 0; r = 0 ; i = 0
    lines = ggStr.split('\n')
    for l in lines:
        
        if l.strip() == '' : continue
        i+=i
        if   l.startswith('MANAGER'): 
                wd = l.split()
                if len(wd) != 2 : rc+=1 ; continue 
                r = _getProcStat(wd[1])
                gProgRes[wd[0]] = r
                if r != 0 :
                    log.error('%s Status %s = %d' % (wd[0],wd[1],r))
                    rc+= 1

        elif l.startswith('JAGENT'): 
                wd = l.split()
                if len(wd) != 2 : rc+=1 ; continue 
                r = _getProcStat(wd[1])
                gProgRes[wd[0]] = r
                if r != 0 :
                    log.error('%s Status %s = %d' % (wd[0],wd[1],r))
                    rc+= 1
        # For extract we need to place results in dictionary. 
        # gGrpRes  = {}. key : GroupNAme  : (Status, Lag , chkpt)
        elif l.startswith('EXTRACT') :
                resL = []
                wd = l.split()
                if len(wd) != 5: rc+=1 ; continue 
                
                r = _getProcStat(wd[1])
                resL.append(r)         # Status
                if r != 0 :
                    log.error('%s:%s Status %s = %d' % (wd[0],wd[2],wd[1],r))
                    rc+= 1
                    
                r = _getLagTime(wd[3])    
                resL.append(r)         # Lag
                if r != 0 :
                    log.error('%s Lag at Chkp %s:%s = %d' % (wd[0],wd[2],wd[3],r))
                    rc+= 1

                r = _getChkPtTime(wd[4])
                resL.append(r)         # Chkpt
                if r != 0 :
                    log.error('%s Time Since Chkpt %s:%s = %d' % (wd[0],wd[2],wd[4],r))
                    rc+= 1
                    
                # Add Results to dict.
                gGrpRes[wd[2]] = resL

        # For extract we need to place results in dictionary. 
        # gGrpRes  = {}. key : GroupNAme  : (Status, Lag , chkpt)
        elif l.startswith('REPLICAT') :
                resL = []
                wd = l.split()
                if len(wd) != 5: rc+=1 ; continue 
                
                r = _getProcStat(wd[1])
                resL.append(r)         # Status
                if r != 0 :
                    log.error('%s:%s Status %s = %d' % (wd[0],wd[2],wd[1],r))
                    rc+= 1
                    
                r = _getLagTime(wd[3])    
                resL.append(r)         # Lag
                if r != 0 :
                    log.error('%s Lag at Chkp %s:%s = %d' % (wd[0],wd[2],wd[3],r))
                    rc+= 1

                r = _getChkPtTime(wd[4])
                resL.append(r)         # Chkpt
                if r != 0 :
                    log.error('%s Time Since Chkpt %s:%s = %d' % (wd[0],wd[2],wd[4],r))
                    rc+= 1
                    
                # Add Results to dict.
                gGrpRes[wd[2]] = resL
                
    return rc

# Check Results for all processes.
def _chkAllProc():
    rc = 0
    k = gProgRes.keys()
    log.debug('gProgRes keys', k)
    pl = ib.GGS_prog.split(',')
    lk = Set(pl) - Set(k)
    if len(lk) > 0 :
        log.error('Missing Process=', lk)
        rc = 1
    
    for k in pl:
        d = gProgRes.get(k)
        log.debug('%s = %s' % (k,d))
        if d  != 0 :
            log.error('%s = %s' % (k,d)) 
            rc = 1
            
    return rc

# Check Results for all groups.
def _chkAllGrp():
    rc = 0
    k = gGrpRes.keys()
    log.debug('gGrpRes keys', k)
    if len(k) < 1 :
        log.error('No results found for Groups ' )
        return 1

    gl = ib.GGS_grp.split(',')
    lk = Set(gl) - Set(k)
    if len(lk) > 0 :
        log.error('Missing Process=', lk)
        rc = 1

    # d is a list that contains status,lag,chkpt    
    for k in gl:
        dl = gGrpRes.get(k)
        log.debug('grp = ', k, 'res = ', dl)
        if dl is None: 
            log.error('grp = ', k, 'res = ', dl)
            rc = 1
            continue
        for d in dl :
            if d != 0 :
                log.error('%s = %s' % (k,d))
                rc = 2
        
    return rc


def _gss_infoAll_linux2():
    bfn  = 'chk_all_proc'     # By convention will add .sh termination. MANDATORY
    cmd  = '%s/%s.sh; echo rc=$?' % (ib.rpath,bfn)
    #ib.pwd = 'Ryder#123'
    rcmd = oc.CmdLinuxDriver(log,ib)
    log.debug('rcmd = \n\n%s' % cmd)
    rmsg = rcmd.execRemCmd(cmd, 'Linux')
    return rmsg

# Invoke: 
#E:\GoldenGate\GG>type infall.bat
#                      ggsci < infall.dat
# E:\GoldenGate\GG>type infall.dat
#                      info all

def _gss_infoAll_win32():
    # Invoke gss command. ggsci < infall.datggsci < infall.dat
    bfn = 'chk_all_proc'    # By convention will add .bat termination. MANDATORY
    gss_cmd = '%s/%s.bat' %  (ib.rpath,bfn)

    log.debug('gss_cmd = %s' % gss_cmd)

    rc,rmsg = proc.runSync(gss_cmd,log)
    if rc != 0 :log.error('cmd %s rc = %s -> msg= %s' % (gss_cmd,rc,rmsg))
    return rmsg
    

def gss_infoAll():
    
    rc = 0
    r = _setIBVarsInt() 
    if r != 0 : return rc
    if sys.platform == 'linux2': 
        rmsg = _gss_infoAll_linux2()
    elif sys.platform == 'win32':
        rmsg = _gss_infoAll_win32()
    else :
        log.error('system %s not supported' % sys.platform)
        return 9 

    log.debug('rmsg = \n\n%s' % rmsg)
    #r = _parseGGSCI(ggs_res) 
    r = _parseGGSCI(rmsg) 
    #if r != 0 : return r
    
    r = _chkAllProc()        ; rc+=r
    r = _chkAllGrp()         ; rc+=r
    
    return rc
 
def _setIBVarsInt():
        n = su.toInt(ib.GGS_lag) 
        if n == None : 
            log.error('GGS_LAG_MAX Needs to be an integer. Current setting %s' % ib.GGS_lag)
            return 1
        ib.GGS_lag = n
        
        n = su.toInt(ib.GGS_chkpt)
        if n == None : 
            log.error('GGS_LAG_MAX Needs to be an integer. Current setting %s' % ib.GGS_lag)
            return 1
        ib.GGS_chkpt = n 
        
        return 0
    
# This methods sets OS environment variables, needed for the MAIN_CFG driver
# Side Effect sets the global sets globally.
def _getEnvVars():
    ret = 0   
    for ev, v in  infaEnvVar.items():
        log.debug("ev =%s v = %s" % (ev,v))
    try:     
        for ev, v in  infaEnvVar.items():
            x = os.environ[ev]
            exec  "%s='%s'" % (v,x) in globals()
            log.debug("---%s='%s'" % (v,x))    
     
    except:
            ret = 2
            log.error("ENV var not set %s %s\n" % (sys.exc_type,sys.exc_value))
          
    finally : return ret
    

# Set of diagnostics commands to run
cmdStep = { 'A' :  pingOraSrv   ,
            'B' :  tnspingOraDB ,
            'C' :  connOraDB    ,
            'D' :  pingMSqlSrv  ,
            'E' :  connMSQLDB   ,
            'F' :  gss_infoAll  ,
           }

def execSteps(runStep):
    #diagnose Bean
    
    log.debug('runStep = %s' % runStep)
    for s in runStep:
        if (not cmdStep.has_key(s)):
            log.debug('Invalid step %s' %s)
            if (exitOnError) : return 1
        rv = 1
        try:
            rv = cmdStep[s]()
            if rv != 0 and exitOnError : return rv
            
            log.info('[%s]:%s()\trc\t= %d' % (s,cmdStep[s].__name__,rv))
        
        except AttributeError:
            log.error( '[%s]:%s()' % (s,cmdStep[s].__name__))
            log.error( '%s ==> %s ' % (sys.exc_type, sys.exc_value))
            if (exitOnError) : return rv
            
        except SyntaxError:
            log.error( '[%s]:%s()' % (s,cmdStep[s].__name__))
            log.error( '%s ==> %s ' % (sys.exc_type, sys.exc_value))
            if (exitOnError) : return rv
        
    return rv
            

# Use from script for standalone functions/
# Invoke notify from here.
def main(argv):
    appName = os.path.splitext(os.path.basename(argv[0]))[0]
    if len(argv) != 2 :
        log.error("USAGE : <%s> fx [runSeq] Incorrect Number of arguments (%d)" % (argv[0], len(argv)))
        sys.exit(1)  
    
    runSeq = argv[1]     
    logFile = getLogHandler(appName,log)
    log.info("logfile = ",logFile)
    
    rc = _getEnvVars()
    if rc != 0 :
        log.error( "Need to set all env vars:\n %s" %  infaEnvVar.keys())
        sys.exit(rc)

    log.info('Loading Config File %s' % ib.configFile)
    rc = fu.loadConfigFile(ib.configFile,ib,log)
    if rc != 0 : 
        log.error('Cannot Load %s Exiting the program' % ib.configFile)
        return 1
    
    # Execute program    
    if runSeq is not None and len(runSeq) > 0 : 
        rc = execSteps(runSeq)
        if rc == 0 : log.info ('Completed running execSteps')
        else       : log.error('execSteps rc = %s' % rc)
     
    log.info('Executed cmd = ',argv[1:],' rc = ', rc)
    subj = ("SUCCESS running %s on Server %s Monitoring %s " % (appName, hostname,ib.rhost)) if rc == 0 else ("ERROR running %s on Server %s. Monitoring %s " % (appName, hostname,ib.rhost))
    text = "Running %s host=%s. Monitoring %s . Please See logfile %s" % (appName,hostname, ib.rhost,logFile)
    text += gText
 
    rc1,msg=sm.sendNotif(rc,log,subj,text,[logFile,])
    log.info('logFile= %s' % logFile)
    log.info('Sending Notification\t%s rc = %s' % (msg,rc1))
    return rc

if __name__ == '__main__':
    rc=  main(sys.argv)
    sys.exit(rc)

