'''
Created on Jan 25, 2012

@author: eocampo

This module has to do with notification only.
'''
from sets import Set

from datastore.dbutil    import DBSQLLite,DBOracle,DBPYODBC,getIDX
import datastore.dbinfa  as dbi
import datastore.dbnotif as dbn
import bean.notifbean    as nb
import bean.infabean     as ib

from common.getlogger    import getLogger 

# This method gets Notification Information, currently from MS SQL server.
# cnSrt Connect String to repository holding notification Information.
# record form :
#    r = ('BeerStore', datetime.datetime(2012, 1, 23, 13, 51, 44, 433000), 'wkf_TGT_TBS_RPRCS', 4) lb = TaskRefBean:
#    busTskNm = BeerStore
#    taskDate = 2012-01-23 13:51:44.433000
#    jobName  = wkf_TGT_TBS_RPRCS
#    sev      = 4

def getTblTaskXRef(connStr,log):
    rs = None
    db = DBPYODBC(connStr,'')
    re = db.connToDB()
    if (re != 0 ) : return rs 
 
    tr = []
    rs = db.runQry(dbn.Notif.selTaskRefQry)
    for r in rs:
        b = nb.TaskRefBean() 
        b.setTaskRefBean(r,getIDX(dbn.Notif.selTaskRefIdx))
        log.debug("task = %s tskBean = %s" % (r,b))
        tr.append(b)
        
    db.closeDBConn() 
           
    return tr

# This method gets information from the Infa Repository.
def getFldWklInfaRepo(connStr,log):
    rs = None
    db = DBOracle(connStr,'')
    re = db.connToDB()
    if (re != 0 ) : return rs 
 
    tr = []
    rs = db.runQry(dbi.DBInfaRepo.selRepWflQry)
    for r in rs:
        b = ib.InfaWFBean() 
        b.setInfaWFBean(r,getIDX(dbi.DBInfaRepo.selRepWflIdx))
        log.debug("r = %s lb = %s" % (r,b))
        tr.append(b)
    
    db.closeDBConn()
                     
    return tr

# Auxiliary method to get subjects 
def _getSubj(lst,repWflow):
    
    r = []
    
    for w in lst:
        for b in repWflow:
            if w == b.wkfl : 
                r.append((b.subj,b.wkfl))
                continue
    return r
    
#  Description : THis method
# 1) Get Workflows from Notification DB. 
# 2) Get Workflows from repository DB
# 3) Compares which Workflows are missing from the Notif DB.
#    conNot Connection Str to Notification DB
#    conRep Connection Str to Repository DB
#    log  log handler
#  returns a list with SUBJ and WKL names not present in notification.

def getNotifInfo(connNot,connRep,log):
    
    nw = []; rw = []     
    
    notifWflow = getTblTaskXRef(connNot,log)    
    for b in notifWflow:
        nw.append(b.jobName)
          
    repWflow   =  getFldWklInfaRepo(connRep,log)
    for b in repWflow:
        rw.append(b.wkfl)
    
    nonReg = Set(rw) - Set(nw)
    lst = list(nonReg)
    lst.sort()
 
    r =  _getSubj(lst,repWflow)
    return sorted(r)


if __name__ == '__main__':
    
    #log = getLogger(lvl = 'DEBUG') 
    log = getLogger()
    # MS SQLn PROD
    connNot = 'DRIVER={SQL Server};SERVER=MAINTSERVER3;DATABASE=RepairHistory;UID=edwuser;PWD=WiWLiC'
#    r = getTblTaskXRef(connNot,'')
#    print "getTblTaskXRef(connNot,'') " , r
    
    # ORACLE PROD
    connRep = '%s/%s@%s' % ('infa','Infa4prod' ,'infapd')
#    r = getFldWklInfaRepo(connStr,'')
#    print "getFldWklInfaRepo(connStr,'') ", r

    nr=getNotifInfo(connNot,connRep,log)
    print "Len = %d" % len(nr)
    print "NR = ", nr

    