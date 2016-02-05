'''
Created on Nov 5, 2010
@author: emo0uzx

'''

import os.path
import sys

import mainglob as mg
import common.log4py 
import datastore.dbutil as ds
import datastore.dblkup as dblkup

from procdata.procmain import ProcRyderInfa

import procdata.proccfg     as pcfg
import procdata.procsoftinv as psi
import procdata.proccred    as pcrd
import procdata.proclkup    as plkup

CONFIG_FILE = 'rydinfap.xml'
LOG_FILE    = r'logs\rydinfap.log'  

# Application root directory.
appPath     =  os.path.dirname(__file__)
print "APP_PATH %s " % appPath 
#LOG4_NAME   = 'softinv.log'

# Globals read from config file. 
DB_DRIVER = None    # 'SQLLite' or 'Oracle'
CONFIG_DB = None    # 'rydinfap.dbf'  # Use for SQLITE or 'tdbu03/tdbu03@IMMDWD'   # Use for ORACLE
XOR       = 1       # Encrypt by DEFAULT

# Getter/Setters
def get_env_lkup() : return mg.env_lkup
def get_user_lkup(): return mg.user_lkup


# returns array of beans, containing software inventory data.
def _getAllData(tbl):
    pm = ProcRyderInfa(tbl,
                   mg.dbHandler,
                   mg.logger)
    d = pm.selectAll()
    return d

# dp directory path.
# Note : When running stand alone, place log4py in the App root dir. (maindriver dir)
#        When running via web2py , need to place log4py on the web2py root, where service is started, 
#        log4py will create the logifle based on the rootdir, so need to add fileAppender based on App.
def _getLogger(dp,dbg=False):
    
    log = common.log4py.Logger().get_instance("RydInfap")
    
    # If logfile config does not exits let's create our logfile if possible.
    # Also 
    
    print "LogTargets = " , log.get_targets()
    
    if (os.path.exists (os.path.dirname(dp))):
        lf=os.path.join(dp, LOG_FILE)
        print "Creating logfile in %s" % (lf)
        log.add_target(lf)
    
    else:         
        print "File %S not found. Redirecting logs to stdout"               
        log.set_target(sys.stdout) 
                                      
# EO TODO : Remove this line. For developing purposes ONLY !   
#    log.set_formatstring("%T %L\t%F - %M")
    if dbg : log.set_loglevel(common.log4py.LOGLEVEL_DEBUG)

    return log
     
# Need to run this function as it will populate the data.
# This function will exit execution if fatal errors are found.
# Otherwise returns 1.
# _pid Product ID
def getSoftInvData():
    # Result Sets and Lookups Dictionaries. GLobal to the module as they contain stat data.
    
    mg.vendor_rs     = _getAllData('Vendor')
    mg.prod_rs       = _getAllData('Prod')
    mg.env_rs        = _getAllData('Env')
    mg.vend_cont_rs  = _getAllData('VendCont')
    mg.soft_ver_rs   = _getAllData('SoftVer')
    mg.soft_eol_rs   = _getAllData('SoftEOL')
    
    # Process for rendering.
    mg.env_pid       = psi.setEnvProdId(mg.prod_rs , mg.env_rs)
    mg.vend_pid      = psi.setVendorProdId(mg.prod_rs , mg.vendor_rs)
    mg.vend_cont_pid = psi.setVendContProdId(mg.vendor_rs, mg.vend_cont_rs)
    mg.soft_ver_pid  = psi.setSoftVerProdId(mg)
    mg.soft_eol_pid  = psi.setSoftEOLProdId(mg)
    
def getInfaEnvData():
    mg.infa_dom_rs = _getAllData('InfaDom')
    mg.infa_pc_rs  = _getAllData('InfaPC')
    print " ============> LEN = %d  %s " % (len(mg.infa_pc_rs), mg.infa_pc_rs)
    # Note this field needs to run during initialization.

def getCredData():
    ds = _getAllData('ServCred')
    mg.serv_cred_rs = pcrd.getCredDecryptData(ds,XOR)
    ds = _getAllData('DBCred')
    mg.db_cred_rs   = pcrd.getCredDecryptData(ds,XOR)
    ds = _getAllData('InfaDomCred')
    mg.dom_cred_rs  = pcrd.getCredDecryptData(ds,XOR)
    ds = _getAllData('InfaRepoCred')
    mg.repo_cred_rs = pcrd.getCredDecryptData(ds,XOR)

def getLkupData():
    mg.env_lkup   = plkup.buildInfaLkupObj(mg.dbHandler, mg.logger,dblkup.infaLkupEnv)
    mg.user_lkup  = plkup.buildInfaLkupObj(mg.dbHandler, mg.logger,dblkup.infaLkupUser)
    mg.serv_lkup  = dict (plkup.buildLkupObj(mg.dbHandler, mg.logger,'ServSel'))

def _setDefaults():
    getSoftInvData()
    getInfaEnvData()
    getCredData()
    getLkupData()

# This method gets configuration info into a dictionary.
# SIDE EFFECT : This method sets global variables from XML config file.
def _getConfig():
    fn ='%s\%s' % ( appPath,CONFIG_FILE)
    mg.logger.info("Opening config file %s" % fn)
    rc = pcfg.parseConfigXML(fn)
    if rc is None : return None
    
    for v,d in rc.items() : 
        mg.logger.debug("%s = %s " % (v,d))
        exec  "%s='%s'" % (v,d) in globals()
    
    mg.XOR = XOR         # Encrypt by default. If found in config will set it to that value
    
    return 0

#This method gets a valid DS handler. Needed specially fro SQLLite in a multiuser env, since the handler needs to be used by the thread that creates. 
#For other RDBMS it could use the original handler.
def _getDBHandler():
    
    if   ( DB_DRIVER == 'SQLLITE' ):
        cs = "%s\databases\%s" % (appPath,CONFIG_DB)
        mg.logger.info ("CS = %s\databases\%s" % (appPath,CONFIG_DB))
        return ds.DBSQLLite(cs,mg.logger) 
    elif ( DB_DRIVER == 'ORACLE') :
        cs = CONFIG_DB
        return ds.DBOracle(cs, mg.logger) 
    else :
        mg.logger.error("Non Suitable DB_Driver %s"  % (DB_DRIVER))
        return -1

# Main Application Startup point.
def startup(app=None):  
    global appPath
    #cmd.appPath =  os.getcwd()  EO TODO final set make sure you get the right dir !
    cs =''
    
    if app is not None:
        appPath = app  # Use to give a defined path for debug
        print " overriding appPath with app ==> %s " % appPath
    
    mg.logger  = _getLogger(appPath,True)       # logFile
    if (mg.logger is None): print('Cannot Create Log File. Check Config %s/log4py.conf' % appPath)
    mg.logger.info ("Logger targets ", mg.logger.get_targets())   
    
    cfg = _getConfig()
    if cfg is None:
        mg.logger.error("Configuration Error Exiting the program !!!")
        return -1
   
    mg.dbHandler = _getDBHandler()
    res = mg.dbHandler.connToDB()
    if ( res != 0): 
        mg.logger.error("Cannot connect to DB %s Driver %s "  % (cs,DB_DRIVER))
        return res 
    
    # Set mg globals
    mg.imgDir  = "%s\images\\" % appPath
    mg.binDir   = bindir
    mg.softdepo = softdepo     
    mg.docdepo  = softdepo     
    
    if res == 0 :
        print("Invoking _set_defaults DB %s Driver %s "  % (cs,DB_DRIVER))
        mg.logger.debug("Invoking _set_defaults DB %s Driver %s "  % (cs,DB_DRIVER))
        _setDefaults()
    
    #closeDBHandler()
    mg.logger.info("\nStarting ==> appPath", appPath)
    mg.logger.info(mg.printGlob())    
    return res   

def closeDBHandler(): mg.dbHandler.closeDBConn()

    
# EO Test only 



if __name__ == '__main__':
    #r = startup("C:\\")
    #print "startup RC = %s " % r
    #startup('C:\infaapp\softinv')

    startup()
    print "XOR = %s" % XOR
    sys.exit()
    print "mg.soft_ver_rs %s " %   mg.soft_ver_rs 
    print "mg.soft_eol_rs %s " %   mg.soft_eol_rs
    print " mg.soft_ver_pid  = %s " %  mg.soft_ver_pid 
    print " mg.soft_eol_pid  = %s " %  mg.soft_eol_pid
    mg.printGlob()
    mg.printSoftInv()
    print " mg.infa_dom_rs  " , mg.infa_dom_rs 
    for d in mg.infa_dom_rs :
        print d
     
#    print "Server Cred " , mg.serv_cred_rs
    print " mg.serv_cred_rs  " , mg.serv_cred_rs
    for sc in mg.serv_cred_rs :
        print sc
    #mg.printInfaMain()
    #print "mg.env_lkup", mg.env_lkup
    #print "mg.user_lkup", mg.user_lkup
    print "mg.serv_lkup" , mg.serv_lkup
    closeDBHandler()
