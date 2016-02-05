'''
Created on Jan 4, 2012

@author: eocampo

This module does high level infa commands. All Logic pertinent to informatica should be placed in this module.
20131023 Added delRepoConn method.
20150327 Added runWkflFromTaskWait and startTask
'''
__version__ = '20150327'

import cmd.infacmd      as pi
import mainmsg          as mm
import common.infacodes as ic
import proc.process     as prc

from utils.strutils     import findStr
from datastore.dbinfa   import DBInfaRepo
from sets import Set

# This function will create a list from incoming string. 
# list will be of the form f.w
# m format is 
def _setWflAtt(m):
    wfa = []
    
    i=0
    fld = " "; wkf = ""
    x=m.split() 
       
    for e in x:
        
        if e == 'Folder:':
            fld = x[i + 1]
        if e == 'Workflow:':
            wkf = x[i + 1]
            wfa.append( ("%s.%s") % (fld[1:-1], wkf[1:-1]))
        i+=1

    return wfa

# Get currently scheduled wfs for the Int Service, using PMCmd command.
def retISSched(ib,log):
    pmc = pi.PMCmd(ib,log)
    rc,rm = pmc.getSchedWflIS()
    log.debug("\n==========rc=%s ==============\n\n " % ( rc))
    if rc == 0 :
        isSched = _setWflAtt(rm) 
        return rc, isSched
    
    return rc,[]


# 2 Get the currently scheduled count wfs for the Int Service. 
# If error returns -1 , otherwise the schedule count.
def getISSched(ib,log):

    rc, isSched = retISSched(ib,log)
    if rc == 0 :
        for s in isSched: print s
        return len(isSched)
    else :
        log.error("rc = %s" % rc)
        return -1

        
# Function schedules Workflows in an IS
# ib     InfaBean
# log    logger
# flg    flag to exec the cmd if True will execute , if false will print only
# rsConn Connection to Repo DB
# All parameters are assumed not to be null

def infaSchedAll(ib,log, ds, flg=True):
    repSched = []
    pmc = pi.PMCmd(ib,log)
    
    # 1- Get all Schedules from repository into a list. (f1.w1,f2.w2, ...fn.wn )
    rs = ds.runQry(DBInfaRepo.selSchedQry)
    r  = len(rs)

    if rs is not None and len(rs) > 0:
        for f,w in rs:   
            repSched.append('%s.%s' % (f,w))
            
    # 2 Get the currently scheduled wfs for each Int Service Connected to Repository
    rc,rm = pmc.getSchedWflIS()
    log.debug("\n==========rc=%s ==============\n\n " % ( rc))
    if rc == 0 :
        isSched = _setWflAtt(rm)
    
    else :
        log.error("Error when runing getSchedWflIS = %s" % rc)
        return rc
     
    #3 Subtract repSched - isSched  to get the unscheduled objects.
    unsched = Set(repSched) - Set(isSched)
    
    log.debug("\n\n======================= Repo Schedules          ==> %d ====================\n\n" % len(repSched))
    log.debug("repSched " , repSched)
    log.debug("\n\n======================= Int Services Schedules  ==> %d ====================" % len(isSched))
    log.debug("isSched "  , isSched)
    log.debug("\n\n======================= Rep - IS Schecules      ==> %d ====================" % len(unsched))
    log.debug("unSched "  , unsched)
    log.debug("\n\n===========================================================================")
    
    # Invoke infa command to schedule the unsched wfls
    # ['Financial.wkf_fin_lbr_chr_out',]
    # f = a[0:x-1]
    # BE aware that the result of scheduling might not be 0 all the time, so continue anyhow.
    if (flg):        
        for a in unsched:
            x=a.find('.')        
            log.debug("a=%s f=%s w=%s " % (a, a[0:x],a[x+1:]))
            #r = pmc.schedWFl(a[0:x],a[x+1:])                     # EO Uncomment to run
            r=0
            if r == 0 : log.info("Scheduling %s " % a)        
        return r
    
    else:
        print("\n\n======================= Repo Schedules          ==> %d ====================\n\n" % len(repSched))
        print("repSched " , repSched)
        print("\n\n======================= Int Services Schedules  ==> %d ====================" % len(isSched))
        print("isSched "  , isSched)
        print("\n\n======================= Rep - IS Schecules      ==> %d ====================" % len(unsched))
        print("unSched "  , unsched)
        print("\n\n===========================================================================")
        
        return 0

# wrapper to sched function
# f folder
# w wkfl
def infaSched(ib,log):
    pmc = pi.PMCmd(ib,log)
    rc,rm = pmc.schedWFl(ib.fld,ib.wkf)
    log.debug("f=%s w=%s" % (ib.fld,ib.wkf))
    log.debug("rm = %s" % rm)
    if rc == 0 : log.info("Scheduling f=%s w=%s " % (ib.fld,ib.wkf)) 
    return rc

# wrapper to sched function
# f folder
# w wkfl
def infaUnSched(ib,log):
    pmc   = pi.PMCmd(ib,log)
    rc,rm = pmc.unSchedWFl(ib.fld,ib.wkf)
    log.debug("f=%s w=%s" % (ib.fld,ib.wkf))
    log.debug("rm = %s" % rm)
    if rc == 0 : log.info("UnScheduling f=%s w=%s " % (ib.fld,ib.wkf)) 
    return rc
# Function unschedules Workflows in an IS
# ib     InfaBean
# log    logger
# ds     rsConn Connection to Repo DB
# All parameters are assumed not to be null

def infaUnschedAll(ib,log, ds):
    
    pmc = pi.PMCmd(ib,log)
    rs = ds.runQry(DBInfaRepo.selRepWflQry)
    r  = len(rs)
    if rs is not None and len(rs) > 0:
        for f,w in rs:  
            rc,rm = pmc.unSchedWFl('"%s"' % f, '"%s"' % w)
            if rc == 0 :
                log.info("Unscheduled f = %s w = %s " % (f,w))
    return r


# Get workflows status.
# lst list of folders/workflows, that are  
# e.g. folder.wkfl (~Shared_Global.wkf_InfaStat_Notify)
# Need to get teh following fields:
#    Workflow run status: [Running]
#    Workflow run status: [Succeeded]
#    Workflow run type: [Schedule]
#
# Returns a list of wkf's currently running...

def getWorkFlowStatus(ib,log,lst):
    runStat = 'Workflow run status: \[Running\]'
    #runStat = 'Workflow run status: \[Succeeded\]'
    rst = []
    f='';w=''
    #x='Workflow run status:'
    pms = pi.PMCmd(ib,log)
    for e in lst:
        f,s,w = e.partition('.')
        if s != '.' or w == '':
            log.error("Invalid entry %s" % e)
            continue
            
        rc,rm =  pms.getWflDetails(f,w)
        log.debug("pms.getWflDetails = %s.%s\n%s" % (f,w,rm))
            
        if rc == 0 : 
            rv = findStr(rm,runStat)
            if rv == 0 : rst.append('%s.%s' % (f,w)) 
                               
        else:
            log.error("pms.getWflDetails f=%s w =%s msg = %s" % (f,w,rm))
            continue
    return rst    
    

def runTaskWait(ib,log):
    pmc   = pi.PMCmd(ib,log)
    rc,rm = pmc.startTaskWait(ib.fld,ib.wkf,ib.task) 
    log.debug("f=%s w=%s t=%s" % (ib.fld,ib.wkf,ib.task))
    log.debug("rm = %s" % rm)
    return rc

def runWkflWait(ib,log):
    pmc   = pi.PMCmd(ib,log)
    rc,rm = pmc.startWkflWait(ib.fld,ib.wkf) 
    log.debug("f=%s w=%s" % (ib.fld,ib.wkf))
    log.debug("rm = %s" % rm)
    return rc
    
def runWkflFromTaskWait(ib,log):
    pmc   = pi.PMCmd(ib,log)
    rc,rm = pmc.startWkflFromTaskWait(ib.fld,ib.wkf,ib.task) 
    log.debug("f=%s w=%s t=%s" % (ib.fld,ib.wkf,ib.task))
    log.debug("rm = %s" % rm)
    return rc

def runWkflParamWait(ib,log):
    pmc   = pi.PMCmd(ib,log)
    rc,rm = pmc.startWkflParamWait(ib.param,ib.fld,ib.wkf) 
    log.debug("p=%s f=%s w=%s" % (ib.param,ib.fld,ib.wkf))
    log.debug("rm = %s" % rm)
    return rc

# Function runs Workflows in an IS
# ib     InfaBean
# log    logger
# app    application folder dict with keys:
#        folder (subject Area)
#        wfl    (list of wkf to run in a particular folder area. 
# All parameters are assumed not to be null

def infaRunApp(ib,log,app):
    pmc  = pi.PMCmd(ib,log)
    fld  = app.get('fld')
    wkfs = app.get('wkfs')
    
    if (fld is None or wkfs is None) : return  mm.PMCMD_INV_APP_ARGS 
    if (fld is '' or len(wkfs) < 1)  : return  mm.PMCMD_INV_SIZE_ARGS
    log.info("Starting application %s IB = %s " % (fld,ib)) 
    for w in wkfs:
        rc,rm = pmc.startWkflWait(fld,w)
        if (ib.eFlg is True and rc != 0 ) :
            log.error("\tw=%s\trc=%s Exit Flag =%s\nrm=%s " % (w,rc,ib.eFlg,rm) )
            log.error("Infacode %s" % ic.infCode)
            return rc
        
        log.info("\tw=%s\trc=%s\trm=%S" % (w,rc,rm) )    
    
    # if (rc != 0 ) : log.info("Infacode %s" % )
    return rc


# Repository Operations
def backUpRepo(ib,log):
    pmr   = pi.PMRep(ib,log)
    rc,rm = pmr.repConn()
    if rc == 0:
        rc,rm =  pmr.repBkup(ib.rep_bkup)
        if rc != 0:
            log.error('Could not bkup repository %s \n-- %s ' % (ib.rep_name,rm))
    else:
        log.error('Could not connect to repository %s \n-- %s' % (ib.rep_name,rm))
    
    return rc    

# Delete Connections in a given Repository. 
# ib.cnxs is a list of connections to be deleted.
# By contract invoker method will invoke only if list > 0
def delRepoConn(ib,log):
    pmr   = pi.PMRep(ib,log)
    rc,rm = pmr.repConn()
    
    r = 0
    if rc == 0:
        for cn in ib.cnxs:
            rc,rm =  pmr.repDelConn(cn)
            if rc != 0:
                log.error('Could not delete connection %s on repository %s \n-- %s ' % (cn,ib.rep_name,rm))
                r += 1
            else:
                log.info('Deleted connection %s on repository %s \n-- %s ' % (cn,ib.rep_name,rm)) 
    
    else:
        log.error('Could not connect to repository %s \n-- %s' % (ib.rep_name,rm))
    
    return r   

# Domain Operations

# Repository Operations
def backUpDom(ib,log):
    ics   = pi.InfaSetup(ib,log)
    rc,rm = ics.domBkup(ib.dom_bkup)
    if rc != 0:
        log.error('Could not bkup domain %s \n-- %s ' % (ib.dom_name,rm))  
    return rc   


# This method will stop Informatica services.
# ib     InfaBean. Need to probe processes
# dom  Target Domain/Environment. 
# fflg force flag False will not restart services if any pmcmd process is running.
#                 True will restart service.
                   
def stopInfaServ(dom,log):
    
    if dom.fflg is False: 
        rc = prc.getProcStat('pmdtm',log)
        if rc == 0:
            ics = pi.InfaSrv(dom,log)
            rc,rm = ics.stopServices()
            log.info('Stopping Services rc = %s' % rc)
            log.debug("rc= %s rm =%s " % (rc,rm) )        

        elif rc > 0  : 
            log.info('Could not Stop Services there are pmdtm process running')
            
    else:    
        ics = pi.InfaSrv(dom,log)
        rc,rm = ics.stopServices()
        log.info('Stopping Services rc = %s' % rc)
        log.debug("rc= %s rm =%s " % (rc,rm) )
   
    return rc
         
def startInfaServ(dom,log):
    
    ics   = pi.InfaSrv(dom,log)
    rc,rm = ics.startServices()
    if rc != 0:
        log.error('Starting Services for domain %s \n--rc = %s ' % (dom.dom_name,rm))  
    return rc   

def restartInfaServ(dom,log):
    rc = stopInfaServ(dom,log)
    if rc == 0 :
        rc = startInfaServ(dom)
        
    return rc    
        
if __name__ == '__main__':
    
    pass
    #test_infa_unsched()