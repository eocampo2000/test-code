'''
Created on May 23, 2012
@author: eocampo
Should be used for server diagnostics for several platforms/environments

NOTE: If all variables are not SET in config file for a particular test , you will get an AttributeError Exception.
      -r ABC -e -c
      -r AEFG  -c
Attibutes per test

pingServer      host
connToDB:       dbType,db_server,db_name,db_user,db_pwd
pingDomain:     dom_name, dom_host, dom_port, dom_node
pingIntService: dom_name,host,dom_port,IS 
repConn:        rep_name,dom_host,dom_port,rep_user,rep_pwd

'''
__version__ = '20121005'

usage    =     "Usage: python %prog [options] arg1 arg2 ..."


# Supported DB 
# Oracle
# Netezza
import os
import sys
import inspect
import optparse
import socket

import mainglob        as mg                 # DO not comment
import common.log4py   as log4py 
import common.simpmail as sm
import cmd.infacmd     as ic
import cmd.dbcmd       as dbc                # DO not comment
import utils.strutils  as su
import utils.fileutils as fu
from   cmd.oscmd       import getCmdClass
from common.loghdl     import getLogHandler

infaEnvVar   = {
                'PMCMD'            : 'mg.pmcmd'      ,
                'PMREP'            : 'mg.pmrep'      ,
                'INFACMD'          : 'mg.infacmd'    ,  
                'INFASETUP'        : 'mg.infasetup'  ,
               }  

hostname = socket.gethostname()
#Get the Logname
log     =  log4py.Logger().get_instance()
cmdInst = getCmdClass(log)
gText = ''

# Empty Container
class PlaceHolderBean:
    pass

ib            = PlaceHolderBean()

# Command Line Options (User opt)
userOpt       = []         # Log Command line options
runSeq        = None
config        = False
verbose       = False
exitOnError   = False
show_test     = False
diskTh        = 80        # Default
fileAge       = 15        # Default

def pingServer():    
    rv = cmdInst.pingServer(ib.host)
    log.info('rv = %s' % rv)
    return rv

def pingDB():
    cmdDB = eval('dbc.%sCmd(ib,log)' % ib.dbType)
    rv = cmdDB.pingDB()
    log.info('rv ', rv)
    return rv

def connToDB():
    cmdDB = eval('dbc.%sCmd(ib,log)' % ib.dbType)
    rv = cmdDB.connToDB()
    log.info('rv', rv)
    return rv

def diskUsage():
    rv = cmdInst.diskUsage(ib.host)
    log.info('rv = %s' % rv)
    return rv

def pingInfaDomSvc():
    cmdInfa = ic.InfaCmd(ib,log)
    rc,rm = cmdInfa.pingDomain()
    log.debug('rc=%s rm = %s' % (rc,rm))
    return rc

def pingInfaIntSvc():
    cmdInfa = ic.InfaCmd(ib,log)
    rc,rm = cmdInfa.pingIntService()
    log.debug('rc=%s rm = %s' % (rc,rm))
    return rc

def connInfaRepo():
    rv = -1 
    cmdRepo = ic.PMRep(ib,log)
    rc,rm = cmdRepo.repConn()
    if (rc == 0):
            rv = su.findStr(rm,'connect completed successfully')
    log.debug('rc=%s rm = %s' % (rc,rm))
    return rv

def freeResources():
    log.info('freeResources')
    return 0

# Set of diagnostics commands to run
cmdStep = { 'A' : pingServer    ,
            'B' : pingDB        ,
            'C' : connToDB      ,
            'D' : diskUsage     ,
            'E' : pingInfaDomSvc,
            'F' : pingInfaIntSvc,
            'G' : connInfaRepo  ,         
           }


def showTest():
    print "Test Options"
    keys = sorted(cmdStep.keys())
    for k in keys: print '[%s] --> %s ' % (k,cmdStep[k].__name__)
        

def showSteps(runStep):
    #diagnose Bean
    log.debug('runStep = %s' % runStep)
    for s in runStep:
        if (not cmdStep.has_key(s)):
            log.debug('Invalid step %s' %s)
        else:    
            log.info('[%s]:%s()' % (s,cmdStep[s].__name__))


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
            
#Side Effect set global flags based on Command Line options.
# All valid command line arguments
def parseArg(cmdArgs):
    parser = optparse.OptionParser(usage=usage) 
    
    parser.add_option('-e','--exit',     
                       action="store_true", 
                       help="Stop on Error" ,
                       dest="exitonerr" ,
                       default=False)
    
    parser.add_option('-r','--run', 
                       action="store",      
                       help="Run Test [ABCD...] " ,
                       dest="run" )
        
    parser.add_option('-c','--config',  
                       action="store_true", 
                       help="Show Configuration",
                       dest="config" ,
                       default=False)
    
    parser.add_option('-t','--test',  
                       action="store_true", 
                       help="Show Available Tests",
                       dest="show_test" ,
                       default=False)
    
    (options,args) = parser.parse_args(cmdArgs)  
    
    if(len(cmdArgs) < 2):
        parser.print_help()
        log.error('Invalid Number of Arguments')
        return -1
    
    if (options.config):
        global config
        userOpt.append("config = %s," % options.config)
        config = options.config
   
    if (options.run):
        global runSeq
        userOpt.append("run = %s," % options.run)
        runSeq=options.run
          
    if (options.exitonerr):
        global exitOnError
        userOpt.append("exit on error = %s," % options.exitonerr) 
        exitOnError = True
    
    if (options.show_test):
        global show_test
        userOpt.append("exit on error = %s," % options.show_test) 
        exitOnError = True
        
    parser.destroy()
    
    return 0

def show_config(bn):
    lst = inspect.getmembers (bn)
    for k,d in lst:        
        if k[0:2] == '__' : continue
        print '%s\t=\t%s' % (k,d)

def main(Args):

    # Set log
    fn = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    logFile = getLogHandler(fn,log)
    log.info('logFile= %s' % logFile)
    
   # EO Uncomment in UNIX or set en vars in windows.                         
    rc = _getEnvVars()
    if rc != 0 :
        log.error( "Need to set all env vars:\n %s" %  infaEnvVar.keys())
        return (rc)

    rc = parseArg(sys.argv)
    log.info('userOpt = ' , userOpt)
    if rc != 0 : return(rc)
    if os.environ.has_key('CONFIG_FILE') : 
        fn = os.environ['CONFIG_FILE']
    else:
        log.error("need to set env : 'CONFIG_FILE' " )  
        return 1
    
    log.info('Loading config file:%s' % fn)
    rc = fu.loadConfigFile(fn,ib,log)
    if rc != 0 : 
        log.error('Exiting the program')
        return 1
    
    if (config) : show_config(ib)
    #print "MEMBERS ", inspect.getmembers (ib)
    # Execute program    
    if runSeq is not None and len(runSeq) > 0 : 
        rc = execSteps(runSeq)
        if rc == 0 : log.info ('Completed running execSteps')
        else       : log.error('execSteps rc = %s' % rc)

    text = 'Please see logFile %s' % logFile
    subj = "SUCCESS running %s on %s " %(fn,hostname ) if rc == 0 else "ERROR running %s on %s" % (fn,hostname)
    rc,msg=sm.sendNotif(rc,log,subj,text,[logFile,])
    log.info('Sending Notification\t%s rc = %s' % (msg,rc))
    
    sys.exit(rc)


if __name__ == '__main__':
    #showTest()
    #execSteps('ABCDEFGXZ')
    #execSteps('BC')
    #showSteps('ABCDEFGXZ')
    #sys.argv = ['prog','-e', '-c','-r', 'ABCDEFG']
    
    #print "IB = ", ib
    
    # fn = os.path.splitext(os.path.basename(sys.argv[0]))[0] 
    # logFile = getLogHandler(fn,log)
    # text = 'Please see logFile %s' % logFile
    main(sys.argv)
