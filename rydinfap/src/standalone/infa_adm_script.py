'''
Created on Jan 3, 2012

@author: eocampo

20130530 : Added methods to support pwd encoding. 
20131023 : Added del conn methods. 
20150327 : Addedstart task and restart from task methods.
'''

__version__ = '20150327'

import os,sys, socket
from datastore.dbutil    import DBOracle
from procdata.procxml    import parseSched 
from sets import Set

import mainglob          as mg                 # DO not comment
import common.log4py     as log4py 
import common.simpmail   as sm
import utils.strutils    as su
import utils.fileutils   as fu
import bean.infabean     as ibe
import procdata.procinfa as pi
import procdata.procmain as pm
#import datastore.dbinfa  as dbi

from common.loghdl import getLogHandler

# Should move to another module.
class FxSignatureError(Exception):
    def __init__(self, fx, err):
        self.fx  = fx
        self.err = err
        
# Move to Another Module:
ib          = ibe.InfaCmdBean()
ib.rep_bkup = 'FILE_REP_BKUP'
ib.dom_bkup = 'DOM_REP_BKUP'
infaLogs    = 'logs'


hostname = socket.gethostname()
# Email GLobal options. 
gSubj   = ''
gText   = ''
gAttach = []

#Get the Logname
log     =  log4py.Logger().get_instance()
#

#
# Environment Variables that need to be set. Key is the ENV and ELE the name of var.
# Below are global variables.
infaEnvVar   = {
                'INFA_BIN'         : 'infaBin'       ,
                'INFA_SERVER'      : 'infaServer'    ,
                'INFA_HOME'        : 'infaHome'      ,
                'INFA_DOMAINS_FILE': 'infaDomFile'   ,
                'PMCMD'            : 'mg.pmcmd'      ,
                'PMREP'            : 'mg.pmrep'      ,
                'INFACMD'          : 'mg.infacmd'    ,    
                'INFASETUP'        : 'mg.infasetup'  ,  
                'INFASRV'          : 'mg.infasrv'    ,             
                'INFA_USER'        : 'ib.rep_user'   ,
                 #'INFA_PWD'       : 'ib.rep_pwd'    ,
                'INFA_XPWD'        : 'ib.rep_xpwd'    ,
                'INFA_HOST'         : 'ib.dom_host'   ,
                'INFA_DOM'         : 'ib.dom_name'   ,
                'INFA_NODE'        : 'ib.dom_node'   ,
                'INFA_PORT'        : 'ib.dom_port'   ,
                'DOM_DBHOST'       : 'ib.dom_dbhost' ,
                'DOM_DBSERVICE'    : 'ib.dom_dbservice',
                'DOM_DBPORT'       : 'ib.dom_dbport' ,
                'DOM_DBTYPE'       : 'ib.dom_dbtype' ,
                'DOM_DBUSER'       : 'ib.dom_dbuser' ,
                #'DOM_DBPWD'        : 'ib.dom_dbpwd'  ,
                'DOM_DBXPWD'       : 'ib.dom_dbxpwd'  ,
                'REPO_SERV'        : 'ib.rep_name'   ,
                'INT_SERV'         : 'ib.IS'         ,
                'REP_DB_NAME'      : 'ib.rep_dbname' ,
                'REP_DB_USER'      : 'ib.rep_dbuser' ,
                #'REP_DB_PWD'       : 'ib.rep_dbpwd'  ,
                'REP_DB_XPWD'      : 'ib.rep_dbxpwd'  ,
                'REPO_BKUP'        : 'ib.rep_bkup'   ,
                'DOM_BKUP'         : 'ib.dom_bkup'   ,
                
               }

# This structure have a list of valid commands.
cmd_arg = {'restartinfa' : '_restartInfaSrv'  ,
           'startinfa'   : '_startInfaSrv'    ,
           'stopinfa'    : '_stopInfaSrv'     ,
           'killinfa'    : '_killInfaSrv'     ,
           'sched'       : '_schedWkfl'       ,
           'unsched'     : '_unSchedWkfl'     ,
           'schedall'    : '_schedWkflAll'    ,
           'unschedall'  : '_unSchedWkflAll'  ,
           'getissched'  : '_getISSched'      ,
           'getunsched'  : '_getISUnsched'    ,
           'printsched'  : '_printSched'      ,
           'printvar'    : '_printVar'        ,
           'runwkfw'     : '_runWkflWait'     ,
           'runwkfwtask' : '_runWkflTaskWait' ,
           'runtask'     : '_runTask'         ,           
           'bkuprepo'    : '_bkupRepo'        ,
           'bkupdom'     : '_bkupDom'         ,
           'printenv'    : '_printEnv'        ,
           'showopt'     : '_showOpt'         ,
           'wkfstat'     : '_wkfStatus'       ,
           'delrepcnx'   : '_delRepoCNXS'     ,
          }

# Common method to check FX signature, used across several functions that
# have the same signature,
# IMPORTANT 2nd arg need to be a list. 
def _chkFxSignature(fx,*arg):
    if len(arg) != 2  : raise FxSignatureError(fx ,'Not enough arguments')
    log.debug("arg0 f = %s arg1 = %s " % (arg[0] ,arg[1]))
    
    f = arg[0]
    try: wkfl = eval(arg[1])    # Needs to be a list !  
    except NameError:  
        raise FxSignatureError('fx %s ',"needs 2 args and 2nd arg (LIST)")
    else:
        return (f, wkfl)

# This is a generic version that checks for an args
# an number of arguments.
# In UX env cmd will be 1 arg 
def _chkFxSign(fx,an,*arg):

    if len(arg) != an  :
        log.error("Invalid number of arguments")
        raise FxSignatureError(fx ,'FX args %d USED %d' % (an,len(arg)))  
    
# Scheules ALL Workflows for an environment. Uses repositories.
def _schedWkflAll(ib):
   
    ds = DBOracle('%s/%s@%s' % (ib.rep_dbuser,ib.rep_dbpwd,ib.rep_dbname), log)                            
    ret = ds.connToDB()
    if (ret is None or ret != 0 ) : return -2
    pi.infaSchedAll(ib,log, ds)
    ds.closeDBConn()
    return 0 
 
# Should check wkf status. If lst empty get all wkfl  from Repository.
def _checkWkfStatus(*arg):
    _chkFxSign('_getISUnsched',1,*arg)           # If error raises FxSignature exception. 
    fn = arg[0]
    rc,schLst,unschLst = parseSched(fn)
    if rc != 0 :
        log.error("parseSched rc = %s msg = %s " % (rc,schLst))
        return rc

# Checks IS current schedules and compares it to an existing list/inventories of register schedules.
# Reads xml schedules file.
# fn path/file.xml 
# Note in Unix: 
# Argument getunsched  C:\\infa_support\\schedules\\sched.xml
def _getISUnsched(*arg):
  
    global gText
    
    _chkFxSign('_getISUnsched',1,*arg)                 # If error raises FxSignature exception. 
    fn = arg[0]
    rc,schLst,unschLst = parseSched(fn)                # unschLst : Logically unscheduled on master list 
    if rc != 0 :
        log.error("parseSched rc = %s msg = %s " % (rc,schLst))
        return rc

    rc,isSched = pi.retISSched(ib,log)                 # Current IS Schedules
    if rc != 0 :
        log.error("retISSched rc = %s msg = %s " % (rc,schLst))
        return rc

    unsch = Set(schLst) - Set(isSched)    
    un = len(unsch)

    # Running Workflows will not appear as "Scheduled"
    runs   =  pi.getWorkFlowStatus(ib, log, unsch)    
    tu = len(runs)
    if tu > 0 : 
        gText += 'Running     \t:Total %d\t %s' %  (tu, ','.join(sorted(runs)))
        log.info('Running     \t:Total %d\t %s' %  (tu, runs ))
        
    # Scheduled WKF from master list where sched="1" 
    if un > 0 : 
        gText += '\nScheduled(ML)\t:Total %d\t %s' %  (un, ','.join(sorted(unsch))) 
        log.info('Scheduled(ML)\t:Total %d\t %s' %  (un, unsch))
    
    # Unscheduled WKF from master list where sched="0" 
    un = len(unschLst)
    if un > 0 : 
        gText += '\nUnScheduled(ML)\t:Total %d\t %s' %  (un, ','.join(sorted(unschLst))) 
        log.info('UnScheduled(ML)\t:Total %d\t %s' %  (un, unschLst))
        
    # Ones that are unschedule, need to check if currently running 
    unsched = Set(unsch) - Set(runs)
    tu = len (unsched)
    if tu > 0 : 
        gText +=  '\nUnScheduled\t:Total %d\t %s' %  (tu, ','.join(sorted(unsched))) 
        log.info('UnScheduled\t:Total %d\t %s' %  (tu, unsched) )
    return tu    # Should be 0 if OK. 

# Prints all Sched from Repo and IS
def _printSched(ib):
   
    ds = DBOracle('%s/%s@%s' % (ib.rep_dbuser,ib.rep_dbpwd,ib.rep_dbname), log)                            
    ret = ds.connToDB()
    if (ret is None or ret != 0 ) : return -2
    pi.infaSchedAll(ib,log, ds, False)
    ds.closeDBConn()
    return 0 

# This method will unschedule ALL jobs for the defined environment.,    
# dom Domain environment
# srv Service
def _unSchedWkflAll(ib):
    
    ds = DBOracle('%s/%s@%s' % (ib.rep_dbuser,ib.rep_dbpwd,ib.rep_dbname), log)
                                
    ret = ds.connToDB()
    if (ret is None or ret != 0 ) : return -2
    
    pi.infaUnschedAll(ib,log, ds)
    ds.closeDBConn()
    return 0 

# Get "Actual schedules" from the Integration Service (IS)
def _getISSched(ib):
    rc = pi.getISSched(ib,log)
    if rc >= 0:
        log.info("%d task(s) is/are schedule in %s Service" % (rc,ib.IS))
    else:
        log.error("Error retrieving Schedules for  %s Service" % (rc,ib.IS))
    
    return rc

# Methods need to get * number of parameters
# Invocation in UNIX test_glob.py sched myfold [\'a\',\'b\',\'c\']
# from sh  sched \\~Shared_Global "['wkf_EDW_WKF_STATUS_NOTIFY', 'wkf_Long_Run_Session',]"
# arg  Needs 2 arguments folder and wkfl.
def _schedWkfl(*arg):

    f, wkfl = _chkFxSignature('_schedWkfl',*arg)           # If error raises FxSignature exception.
    for w in wkfl:
        ib.fld=f; ib.wkf=w
        rc = pi.infaSched(ib,log)
        if rc == 0 : log.info('fld = %s wkfl = %s rc = %s' % (f,w,rc))
        else       : log.error('fld = %s wkfl = %s rc = %s' % (f,w,rc))
        rc+=rc
    
    return rc      # Last status code. 

# Methods need to get * number of parameters
# Invocation in UNIX test_glob.py sched myfold [\'a\',\'b\',\'c\']
# arg  Needs 2 arguments folder and wkfl.
def _unSchedWkfl(*arg):

    f, wkfl = _chkFxSignature('_unSchedWkfl',*arg)           # If error raises FxSignature exception.
    for w in wkfl:
        ib.fld=f; ib.wkf=w
        rc = pi.infaUnSched(ib,log)
        if rc == 0 : log.info('fld = %s wkfl = %s rc = %d' % (f,w,rc))
        else       : log.error('fld = %s wkfl = %s rc = %d' % (f,w,rc))
        rc+=rc
    
    return rc      # Last status code. 


# Methods need to get * number of parameters. It should only be used to run isolated 
# Invocation in UNIX test_glob.py sched myfold 
# arg  Needs 2 arguments folder and wkfl  [\'a\',\'b\',\'c\']
def _runWkflWait(*arg):
    rc = 1
    f, wkfl = _chkFxSignature('_runWkflWait',*arg)           # If error raises FxSignature exception.    
    for w in wkfl:
        ib.fld=f; ib.wkf=w
        rc = pi.runWkflWait(ib,log)
        if rc == 0 : log.info('fld = %s wkfl = %s rc = %d' % (f,w,rc))
        else       : 
            log.error('fld = %s wkfl = %s rc = %d' % (f,w,rc))
            return rc

    return rc

# Methods need to get * number of parameters. It should only be used to run isolated 
# Invocation in UNIX test_glob.py sched myfold 
# arg  Needs 3 arguments folder,wkfl and task
def _runWkflTaskWait(*arg):
    _chkFxSign('_runWkflTaskWait',3,*arg) 
    ib.fld = arg[0]; ib.wkf =arg[1]; ib.task=arg[2]
    rc = pi.runWkflFromTaskWait(ib,log)
    if rc == 0 : log.info('fld = %s wkfl = %s Fromtask=%s rc = %d' % (ib.fld,ib.wkf,ib.task,rc))
    else       : log.error('fld = %s wkfl = %s Fromtask=%s rc = %d' % (ib.fld,ib.wkf,ib.task,rc))
    return rc

# Methods need to get * number of parameters. It should only be used to run isolated 
# Invocation in UNIX test_glob.py sched myfold 
# arg  Needs 3 arguments folder,wkfl and task
def _runTask(*arg):
    _chkFxSign('_runTask',3,*arg) 
    ib.fld = arg[0]; ib.wkf =arg[1]; ib.task=arg[2]
    rc = pi.runTaskWait(ib,log)
    if rc == 0 : log.info('fld = %s wkfl = %s task= %s rc = %d' % (ib.fld,ib.wkf,ib.task,rc))
    else       : log.error('fld = %s wkfl = %s task= %s rc = %d' % (ib.fld,ib.wkf,ib.task,rc))
    return rc
        
# This method provides information on running wkfl running/failed status ONLY.
def _wkfStatus(ib):   
    ds = DBOracle('%s/%s@%s' % (ib.rep_dbuser,ib.rep_dbpwd,ib.rep_dbname), log)                            
    ret = ds.connToDB()
    if (ret is None or ret != 0 ) : return -2
    prInf = pm.ProcInfaDB('WkfStatus',ds,log)
    lst = prInf.selectAll()
    if lst is not None and len(lst) > 0:
        for b in lst:
            log.info('%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (b.subj,b.wkfl,b.sess,b.st_time,b.end_time,b.status,b.err_cd,b.err_msg))

    else:
        log.warn('Nothing is currently running or failed today')
    
    ds.closeDBConn()
    return 0 
# This method will schedule jobs, that were unscheduled due to a failure, abort,    
# dom Domain environment
# srv Service
    
def _printEnv(a):
    log.info('========== Starting env variables dump: =========')
    for ev, v in  infaEnvVar.items():
        log.info('%s\t = %s' % (ev,v))
    log.info('========== Completed env variables dump: =========')
    return 0

# Deletes Repository connections from running env.
# Reads txt del.
# fn path/file.xml 
# Note in Unix: 
# Argument delrepcnx  C:\\apps\\ctl\\delconn.txt
def _delRepoCNXS(*arg):
    _chkFxSign('delrepcnx',1,*arg)
    
    ib.cnxs = fu.readFileLst(arg[0])
    log.debug('ib.cnxs (lst) = ', ib.cnxs)
    if len(ib.cnxs) < 1: 
        log.error("Error Reading file %s or Empty file" % (arg[0]))
    else:
        log.info('Will delete %d Connections' % len(ib.cnxs))
        rc = pi.delRepoConn(ib,log)
    
    return rc   
     

def _bkupRepo(ib):
    log.debug('%s' % ib)
    rc = pi.backUpRepo(ib, log)
    return rc      # Last status code. 

def _bkupDom(ib):
    log.debug('%s' % ib)
    rc = pi.backUpDom(ib, log)
    return rc                  # Last status code. 

# Informatica Admin functions.
# Informatica Admin functions.
def _restartInfaSrv(ib):
    ib.fflg = False                   # Force Flag
    rc = pi.restartInfaServ(ib, log)  
    if rc == 0:
        log.info("Started Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    else:
        log.error("Could not start Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    return rc

def _startInfaSrv(ib):
    rc = pi.startInfaServ(ib, log)  
    if rc == 0:
        log.info("Started Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    else:
        log.error("Could not start Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    return rc

def _stopInfaSrv(ib):
    ib.fflg = False                   # Force Flag
    rc = pi.stopInfaServ(ib, log)
    if rc == 0:
        log.info("Shutdown Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    else:
        log.error("Could not Shutdown Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    return rc

def _killInfaSrv(ib):
    ib.fflg = True
    rc = pi.stopInfaServ(ib, log)
    if rc == 0:
        log.info("Shutdown Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    else:
        log.error("Could not Shutdown Infaservices on %s. rc=%s" % (ib.dom_name,rc))
    return rc

def procScript():
    print sys.argv

# Form host:pwd. returns h,pwd
def _dPWX(x,ev):
    st = su.dec64(x)
    if ib.xpwd_dbg is True : log.debug('st = ' , st)
    if len(st) < 2            : 
        log.error('%s rc=%s. 1-Invalid, 2 Corrupted' % (ev,st[0]))
        if ib.xpwd_dbg is True : log.debug('%s : Pwd 1) Invalid was not generated using h:p pattern !\n 2) pwd was tampered string is not complete. Regen' % ev)
        return None
    if hostname != st[0] : 
        log.error('%s check host : %s' % (ev,st[0]))
        if ib.xpwd_dbg is True : log.debug('%s : Current host %s does not match pattern h:p. Wronly generated or copied form another host' % (ev,hostname))
        return None
    return st[1]

def _initEnvVar():  
 
    xpwd_dbg = os.environ['XPWD_DBG'] if os.environ.has_key('XPWD_DBG')  else 'False'
    if xpwd_dbg == 'True' : ib.xpwd_dbg = True
    
    ib.rep_pwd   = _dPWX(ib.rep_xpwd   ,'REP_XPWD')
    ib.rep_dbpwd = _dPWX(ib.rep_dbxpwd ,'REP_DBXPWD')
    ib.dom_dbpwd = _dPWX(ib.dom_dbxpwd ,'DOM_DBXPWD')   
  
    
# DEBUG
#def _getEnvVars():
#          
#        for ev, v in  infaEnvVar.items():log.debug("ev =%s v = %s" % (ev,v))
#         
#        for ev, v in  infaEnvVar.items():
#            x = os.environ[ev]
#            exec  "%s='%s'" % (v,x) in globals()
#            #log.debug("%s='%s'" % (v,x))    
#            print "SETTING <===> %s='%s'" % (v,x)          
#    
#        print "======Completed Loop========="
#        _initEnvVar()  
#        return 1                  
    
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
             
        _initEnvVar()  
                          
    except:
            ret = 2
            log.error("ENV var not set %s %s\n" % (sys.exc_type,sys.exc_value))
          
    finally : return ret
    
#def _execCmd(c):   # One arg
#    rc = eval('%s(ib)' % cmd_arg[c[0]])
#    rc=_schedWkfl(*(c[1:]))
#    print"rc = ", rc
#          
#def _execCmd(c):   # Two or more args
#    rc = 1
#    rc = eval('%s(*(c[1:]))' % cmd_arg[c[0]])    # use varargs. Each method is responsible for checking it signature
#                #rc=_schedWkfl(*(c[1:]))                     # since this invoker is agnostic in terms of specific FX signatures.
#    return rc    

def _showOpt():
    print ("valid parameters \n", sorted(cmd_arg.keys()))               

#def _printEnv(ib):
#    _showEnv(True)

#def _showEnv(i=True):    
def _showEnv(i=False):
    log.info('Set LOG_LEVEL=log4py.LOGLEVEL_DEBUG in order to see environment')
    evars = os.environ.items()
    for k,v in evars:
        log.debug("\t\t%s = %s\n" % (k,v))
        if (i): print "\t\t====== %s = %s" % (k,v)
    
# Should run first to get the log file, from here invoke env variables.
# c is argv[1:]
def _execCmd(c):
    
    rc = 1
    log.debug('Argument is ' , c)
    if cmd_arg.has_key(c[0]):
        try:
            if len(c) > 1:   
                rc = eval('%s(*(c[1:]))' % cmd_arg[c[0]])    # use var args. Each method is responsible for checking its signature
                #rc=_schedWkfl(*(c[1:]))                     # since this invoker is agnostic in terms of specific FX signatures.
                
            else :
                rc = eval('%s(ib)' % cmd_arg[c[0]])
           
        except NameError:
            log.error("Function %s is not defined " % (sys.exc_value))
        except IndexError:
            log.error("Invalid Function Signature %s!!!" % (sys.exc_value))
        except FxSignatureError, X:
            log.error("%s : %s" % (X.fx,X.err))
        except:
            log.error("Function is not implemented %s %s!!!" % (sys.exc_type,sys.exc_value))
    else:
        log.error("command parameter <%s> is NOT defined" % c[0])
        log.info ("valid parameters \n", sorted(cmd_arg.keys()))        
    
    return rc 


#def _execCmd(c):
#    
#    rc = 1
#    
#    if cmd_arg.has_key(c[0]):
#        
#        if len(c) > 1:   
#                rc = eval('%s(*(c[1:]))' % cmd_arg[c[0]])    # use var args. Each method is responsible for checking its signature
#                #rc=_schedWkfl(*(c[1:]))                     # since this invoker is agnostic in terms of specific FX signatures.
#                
#        else :
#                rc = eval('%s(ib)' % cmd_arg[c[0]])
#           
#
#    else:
#        log.error("command parameter <%s> is NOT defined" % c[0])
#        log.info ("valid parameters \n", sorted(cmd_arg.keys()))        
#    
#    return rc 


# DON'T USE for standalone invocations !!!
# Use from application invocation. IE Instruction sequences.
# We do not want to set env variables more than one for an app context.
# Do not invoke sendNotif from here, such be done form the app.
envRun = False
def exeCmd(*argv):
    global envRun 
    
    if len(argv) == 0 :
        log.error( "USAGE : <%s> fx [args] Not enough arguments " % argv[0])
        sys.exit(1)

    print "envRun is %s" % envRun
    if envRun == False:
        getLogHandler(argv[0],log)

    rc = _getEnvVars()
    if rc != 0 :
        log.error( "Need to set all env vars:\n %s" %  infaEnvVar.keys())
        sys.exit(rc)
        envRun = True
    
    rc = _execCmd(argv)
    log.info('Executed cmd = ',argv,' rc = ', rc)

    #print "GLobals = ", globals()
    #print "IB = %s " % ib
    return rc

# Use from script for standalone functions/
# Invoke notify from here.
def main(argv):

    if len(argv) > 1 :
        logFile = getLogHandler(argv[1],log)
        #log.remove_target('%s/infa_adm_script.log' % infaLogs)

    else:
        log.error( "USAGE : <%s> fx [args] Not enough arguments " % argv[0])
        sys.exit(1)

    rc = _getEnvVars()
    _showEnv()
    if rc != 0 :
        log.error( "Need to set all env vars:\n %s" %  infaEnvVar.keys())
        sys.exit(rc)

    rc = _execCmd(argv[1:])   # Pass the command action e.g. 'getunsched'
    log.info('Executed cmd = ',argv[1:],' rc = ', rc)
    subj = ("SUCCESS running %s on Server %s " % (argv[1], ib.dom_node)) if rc == 0 else ("ERROR running %s on Server %s " % (argv[1], ib.dom_node))
    text = "Running %s Domain=%s Integration Service %s. Please See logfile %s" % (argv[1],ib.dom_name, ib.IS,logFile)
    text += gText

    rc1,msg=sm.sendNotif(rc,log,subj,text,[logFile,])
    log.info('logFile= %s' % logFile)
    log.info('Sending Notification\t%s rc = %s' % (msg,rc1))
    return rc

if __name__ == '__main__':
    rc=  main(sys.argv)
    sys.exit(rc)


