'''
Created on Jan 3, 2012

@author: eocampo

Note: This module should be DEPRECATED.
'''
import os,sys
#import proc.process as p
#import subprocess
#import string
#
#import procdata.procserv as ps
from datastore.dbutil    import DBSQLLite,DBOracle
from common.getlogger    import getLogger 
#from datastore.dbinfa    import DBInfaRepo

#log = log4py.Logger().get_instance()
import bean.infabean as ib


import cmd.infacmd as ic
#from   cmd.oscmd           import CmdDriver
#ib   = ibe.InfaCmdBean()
#dom  = ibe.InfaDomBean()

#Environment Variables that need to be set.
infaEnvVar   = {'INFA_HOME'        : 'infaHome'    ,
                'INFA_DOMAINS_FILE': 'infaDomFile' ,
                'INFA_APP_LOGS'    : 'infaLogs'    ,
                'DOMAIN'           : 'infaDom'     ,
                'PORT'             : 'infaPort'    ,
                'REPO_SERV'        : 'infaRepo'    ,
                'INT_SERV'         : 'infaIS'      ,
               }


# This structure have a list of valid commands and number of requested parameters.
cmd_arg = {'start'    : '_startInfaSrv(ib)'      ,
           'stop'     : '_stopInfaSrv(ib)'       ,
           'sched'    : '_schedWkfl(dom,srv)'    ,
           'unsched'  : '_unSchedWkfl(dom,srv)'  ,
           'printenv' : '_printEnv()'            ,
          }

# Set environment variables
os.environ['INFA_DOMAINS_FILE'] = r'C:\Informatica\9.1.0\clients\PowerCenterClient\domains.infa' 

# Globals for particular env.
# Production 
ibp = ib.InfaCmdBean() ; domp = ib.InfaDomBean()
ibp.IS = 'IS_Ryder'; ibp.dom = 'Domain_Prod';  ibp.user = 'eocampo' ; ibp.pwd ='oceanprod' 
domp.dom_name = 'Domain_Prod'; domp.db_name = 'infapd' ; domp.db_uname = 'infa' ; domp.db_pwd = 'Infa4prod'                 


#Test 
ibt = ib.InfaCmdBean() ; domt = ib.InfaDomBean()
ibt.IS = 'IS_Test'; ibt.dom = 'Domain_Test';  ibt.user = 'eocampo' ; ibt.pwd ='oceantest'
domt.dom_name = 'Domain_Test'; domt.db_name = 'infadev' ; domt.db_uname = 'infatest' ; domt.db_pwd = 'infatest'                 

# Development
ibd = ib.InfaCmdBean() ; domd = ib.InfaDomBean()
ibd.IS = 'IS_Development'; ibd.dom_name = 'Domain_Dev';  ibd.user = 'eocampo' ; ibd.pwd ='oceandev'
domd.dom_name = 'Domain_Dev'; domd.db_name = 'infadev' ; domd.db_uname = 'infadev' ; domd.db_pwd = 'infadev' 

#  This method invokes an app.
#  srv integartion service
#  app = { 'fld' : '~eocampo',
#          'wkfs' : ['wfk_one','wfk_two','wfk_three']}


def _startInfaSrv(ib):
    print "IN _startInfaSrv(ib = %s)" % (ib)

def _stopInfaSrv(ib):
    print "IN _stopInfaSrv(ib = %s)" % (ib)    
    
def _runWkfl(srv, app):
    log = getLogger(lvl = 'DEBUG')   
    rc = ic.infaRunApp(srv,log,app)
    print "infaRunApp RET = %s " % rc

# This method will unschedule ALL jobs for the defined environment.,    
# dom Domain environment
# srv Service
def _unSchedWkfl(dom,srv):
    


    if (dom is None or dom == '' or srv is None or srv == '' ) : return -1
    log = getLogger(lvl = 'DEBUG')    
    cfg = DBOracle('%s/%s@%s' % (dom.db_uname,dom.db_pwd,dom.db_name),
                                log)
    ret = cfg.connToDB()
    if (ret is None or ret != 0 ) : return -2
    
    ic.infaUnschedAll(srv,log, cfg)
    cfg.closeDBConn()

# THis method will schedule jobs, that were unscheduled due to a failure, abort,    
# dom Domain environment
# srv Service

def _schedWkfl(dom,srv):
    

    if (dom is None or dom == '' or srv is None or srv == '' ) : return -1
    log = getLogger(lvl = 'DEBUG')      
    cfg = DBOracle('%s/%s@%s' % (dom.db_uname,dom.db_pwd,dom.db_name),
                                log)
    ret = cfg.connToDB()
    if (ret is None or ret != 0 ) : return -2
   
    ic.infaSched(srv,log, cfg)
    cfg.closeDBConn()
    
def _printEnv (d,i):
    print "d = %s  IS = %s " % ( d, i)

def procScript():
    print sys.argv
    
# This methods sets OS environment variables, needed for the MAIN_CFG driver
#Side Effect sets the global var infaHome,  infaFiles
def _getEnvVars():
    ret = 0   
    try:     
        for ev, v in  infaEnvVar.items():
            x = os.environ[ev]
            exec  "%s='%s'" % (v,x) in globals()
          
    except:
        ret = -1
        print "setInfaEnvVars:==EXCEP %s %s\n" % (sys.exc_type,sys.exc_value)
          
    finally : return ret
    
if __name__ == '__main__':

    # Test env variables
#    rc = _getEnvVars()
#    if rc != 0 : sys.exit(1)
#    print "GLobals = ", globals()
    # Production
    #_schedWkfl(domp,ibp) 
    
    # Test  
    _unSchedWkfl(domd,ibd)
    
    # Development
    #app = { 'fld' : '~eocampo',
    #        'wkfs' : ['wfk_one','wfk_two','wfk_three']}
  
    #runWkfl(domd,app)
    #print "TEST " ;     print_env (domt,ibt)