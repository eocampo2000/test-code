'''
Created on Feb 22, 2011

@author: emo0uzx
To work with Server Admin data.
'''
import string

#import applications.softinv.modules.mainglob as mg
#import applications.softinv.modules.mainmsg  as mm
#import applications.softinv.modules.procdata.proclkup    as plkup

#from applications.softinv.modules.utils.fileutils   import readCSV 
#from applications.softinv.modules.utils.crypt       import encrypt,decrypt
#from applications.softinv.modules.procdata.procmain import ProcServer,ProcSoftInv

import mainglob          as mg
import mainmsg           as mm
import procdata.proclkup as plkup
from utils.crypt  import  encrypt,decrypt

from utils.fileutils   import readCSV 


from procdata.procmain import ProcServer,ProcRyderInfa

# EO TODO: Need to work on ins/update for Infa Components or load them using internal id's.
#          Issue is if you have PROD/DR pair as their INfa componencs names will be the same.
#          In the meantime data could be repair as follow :
#                        update INFA_DOM_CRED
#                        SET PWD = ( select  PWD from  INFA_DOM_CRED where ID = 5  )    -- 5 DR    DOM_IM_P
#                        where ID = 4                                                   -- 6 PROD  DOM_IM_P  use for loads.


#===============================================================================
# Standalone auxiliary  methods.
#===============================================================================

# Need to decrypt password field for display.
# ers encrypted result set
# x encryption flag
def getCredDecryptData(ers,x):

    if x == 1 :                # Encryption is on     
        i=0
        for b in ers:      
            if b.pwd is not None and len(b.pwd) > 0 : 
                d = decrypt(b.pwd,string.capitalize(b.sname))
                ers[i].pwd = d
            i+=1    
    return ers

#===============================================================================
# Web Related methods.
#===============================================================================
# Credentials
#===============================================================================

def _insDBCred(tn,ds):
    db = ProcServer(tn, mg.dbHandler, mg.logger)
    d = db.insCred(tn, ds)
    return d

def _updDBCred(tn,ds):
    db = ProcServer(tn, mg.dbHandler, mg.logger)
    d = db.updCred(tn, ds)
    return d
     
def _delDBCred(tn,ds):
    db = ProcServer(tn, mg.dbHandler, mg.logger)
    d = db.delCred(tn, ds)
    return d
# tbl table to update. 
# sn  server name (key) 
# act action (add, edit, del) --> (ins,upd,del)
# val list of values to populate. ('ID','UNAME','PWD','DESCR')
# BE aware that val dt are str !
# Note: When using SQLLITE need to create a new connection. For process based RDBMS there is no need.
def webProcCred(tbl,act,val):

    if isinstance(val[0], str) and val[0].isdigit(): sid = int(val[0])
        
    elif isinstance(val[0], int)                   : sid = val[0]
       
    else: return mm.INV_ID_DTYPE 

    sn = _getName(tbl,sid)
    if len(sn) < 2 : return mm.SNAME_NOT_FOUND

    if act == 'del'  : bindVar = ('ID','UNAME')
    else             :
        puc = val[2]  
        if mg.XOR == 1:
            p = encrypt(val[2] , string.capitalize(sn)); val[2] = p
        mg.logger.info( "SN = ", sn, "Encrypting ", val[2] , " val = ", val)
        bindVar = ('ID','UNAME','PWD','DESCR')
        
    ds = dict(zip( bindVar,val))
    rc = eval('_%sDBCred(tbl,ds)' % act )
    if rc == 1: 
        val[0] = sid
        if act != 'del' : val[2] = puc
        r=eval('_%sMemCred(tbl,val)' % act)        # Don't care on result for now.
        mg.logger.debug( " _%sMemCred " , act , " tbl = " , tbl, "vals = ", val )
        mg.logger.info( " _%sMemCred rc = %s " % (act, r))   
    return rc

# Generic fx that will invoke the appropriate _getXXXName, based on table name 
# t DB table name
# id id to pass to appropriate fx.
def _getName(t,id):
    print "get NAme t=%s id=%s" % (t,id) 
    print procCred 
    
    if procCred.has_key(t) : return procCred[t][0](id)
    return ''

# returns server name based on ID
def _getServName(id):
    for s in mg.serv_cred_rs:
        if s.serv_id == id : return s.sname
    return ''   

# returns db instance name based on ID
def _getDBName(id):
    for d in mg.db_cred_rs:
        if d.dbrepo_id == id : return d.sname
    return ''   

# returns infa domain name based on ID
def _getDomName(id):
    for d in mg.dom_cred_rs:
        if d.dom_id == id : return d.sname
    return ''   

# returns infa repo name based on ID
def _getRepoName(id):
    for r in mg.repo_cred_rs:
        if r.repo_id == id : return r.sname
    return ''   

####################################################################
# Use for Loading operations. Not for reporting.
# This functions get ID's , name to enc/dec
# returns a dict where k = serv_name and v = serv_id  
####################################################################
# Use for Credentials only. Will Create Dict.
# tbl is for bean base name.


# This methods are invoked ONCE during load.
def _getServID():
    s = {}
    ds = plkup.buildLkupObj(mg.dbHandler, mg.logger,'ServSel')
    for b in ds:
        if (b[1] is not None) :  s[string.capitalize(b[1])] = b[0]
    return s

def _getDBID():
    s = {}
    ds = plkup.buildLkupObj(mg.dbHandler, mg.logger,'DBServSel')
    for b in ds:
        if (b[1] is not None) :  s[string.capitalize(b[1])] = b[0]
    return s

def _getInfaDomID():
    s = {}
    ds = plkup.buildLkupObj(mg.dbHandler, mg.logger,'DomSel')
    for b in ds:
        if (b[1] is not None) :  s[string.capitalize(b[1])] = b[0]
    return s

def _getInfaRepoID():
    s = {}
    ds = plkup.buildLkupObj(mg.dbHandler, mg.logger,'RepoSel')
    for b in ds:
        if (b[1] is not None) :  s[string.capitalize(b[1])] = b[0]
    return s
# Module global define:
procCred = {
 'SERVER_CRED'    : ( _getServName , 'MemServCred' , ),
 'DB_CRED'        : ( _getDBName   , 'MemDBCred'   , ),
 'INFA_DOM_CRED'  : ( _getDomName  , 'MemDomCred'  , ),
 'INFA_REPO_CRED' : ( _getRepoName , 'MemRepoCred' , ),
 }

# Generic fx that will invoke the appropriate _actXXXName, based on table arg name and will update Mem DS 
# t DB table name
# val (list )  ('ID','UNAME','PWD','DESCR')
def _getAllData(tbl):
    pm = ProcRyderInfa(tbl,
                   mg.dbHandler,
                   mg.logger)
    d = pm.selectAll()
    return d

def _insMemCred(t,val): 
    print "In INSERT MEM CRED t = ,", t, " val = ", val
    if procCred.has_key(t) : return eval('_ins%s(val)' % procCred[t][1])
    return -1

# val list ('ID','UNAME','PWD','DESCR')
def _insMemServCred(val):
    ds = _getAllData('ServCred')
    mg.serv_cred_rs = getCredDecryptData(ds,mg.XOR)   
    
def _insMemDBCred(val)  : 
    ds = _getAllData('DBCred')
    mg.db_cred_rs   = getCredDecryptData(ds,mg.XOR)
    
def _insMemDomCred(val) : 
    ds = _getAllData('InfaDomCred')
    mg.dom_cred_rs  = getCredDecryptData(ds,mg.XOR)
    
def _insMemRepoCred(val): 
    ds = _getAllData('InfaRepoCred')
    mg.repo_cred_rs = getCredDecryptData(ds,mg.XOR)
    
# Generic fx that will invoke the appropriate _actXXXName, based on table arg name and will update Mem DS 
# t DB table name
# val (list )  ('ID','UNAME','PWD','DESCR')
def _updMemCred(t,val): 
    if procCred.has_key(t) : return eval('_upd%s(val)' % procCred[t][1])
    return -1

# val list ('ID','UNAME','PWD','DESCR')
def _updMemServCred(val):
    i = 0
    for b in mg.serv_cred_rs :
        if b.serv_id == val[0] and b.uname == val[1]:
            mg.serv_cred_rs[i].pwd   = val[2]
            return 1
        i+=1
    return -1

def _updMemDBCred(val)  :
    i = 0
    for b in mg.db_cred_rs :
        if b.dbrepo_id == val[0] and b.uname == val[1]:
            mg.db_cred_rs[i].pwd   = val[2]
            return 1
        i+=1
    return -1
    
def _updMemDomCred(val) :
    i = 0
    for b in mg.dom_cred_rs :
        if b.dom_id == val[0] and b.uname == val[1]:
            mg.dom_cred_rs[i].pwd   = val[2]
            return 1
        i+=1
    return -1
    
def _updMemRepoCred(val): 
    i = 0
    for b in mg.repo_cred_rs :
        if b.repo_id == val[0] and b.uname == val[1]:
            mg.repo_cred_rs[i].pwd   = val[2]
            return 1
        i+=1
    return -1

# Generic fx that will invoke the appropriate _actXXXName, based on table arg name and will update Mem DS 
# t DB table name
# val (list )  ('ID','UNAME','PWD','DESCR')
def _delMemCred(t,val): 
    if procCred.has_key(t) : return eval('_del%s(val)' % procCred[t][1])
    return -1

# val list ('ID','UNAME','PWD','DESCR')
def _delMemServCred(val):
    i = 0
    for b in mg.serv_cred_rs :
        if b.serv_id == val[0] and b.uname == val[1]:
            del mg.serv_cred_rs[i]
            return 1
        i+=1
    return -1

def _delMemDBCred(val)  :
    i = 0
    for b in mg.db_cred_rs :
        if b.dbrepo_id == val[0] and b.uname == val[1]:
            del mg.db_cred_rs[i]
            return 1
        i+=1
    return -1
    
def _delMemDomCred(val) :
    i = 0
    for b in mg.dom_cred_rs :
        if b.dom_id == val[0] and b.uname == val[1]:
            del mg.dom_cred_rs[i]
            return 1
        i+=1
    return -1
    
def _delMemRepoCred(val): 
    i = 0
    for b in mg.repo_cred_rs :
        if b.repo_id == val[0] and b.uname == val[1]:
            del mg.repo_cred_rs[i]
            return 1
        i+=1
    return -1


#def _updMemCred(tbl,val): 
#    if   tbl == 'SERVER_CRED'   : rc = _updMemServCred(val)
#    elif tbl == 'DB_CRED'       : rc = _updMemDBCred(val)
#    elif tbl == 'INFA_DOM_CRED' : rc = _updMemDomCred(val)
#    elif tbl == 'INFA_REPO_CRED': rc = _updMemRepoCred(val)
#    else                        : rc = -1
#    return rc
#  
    
# For Loads filename, method to get dict, table to load
tbl_load = {'server'    :(_getServID     ,'SERVER_CRED',)    ,
            'db'        :(_getDBID       ,'DB_CRED',)        ,
            'infa_dom'  :(_getInfaDomID  ,'INFA_DOM_CRED',)  ,
            'infa_repo' :(_getInfaRepoID ,'INFA_REPO_CRED',) ,
            }

####################################################################

#This function loads server credentials from a file.
# ln  loadname
# file has 4 fields : server Name, username, pwd, Descr.
# fn to load, 
# lt load table, could be server, db, infa_dom, infa_repo (key to DB table)
# rs is valid result set
# bad are rows that do not match the sep criteria.
# This method is invoked by web interface.
# returns 
# int number of record to be process by DB (exclude the bad), 
# List of bad rec (read form file) and not proc by DB
# List of rej rejected records from DB.
# Records on CSV file No rec proc DB + bad
# Loaded records will be No rec proc DB - rej

def loadCredFile(fn, lt, uflg = False):
    import os.path
  
    if not os.path.exists(fn) :
        mg.logger.error('File %s does not exits ' % fn)
        return (mm.FILE_NOT_EXIST,[],[])
    
    if not os.path.isfile(fn) :
        mg.logger.error('%s is not a file' % fn)
        return (mm.NOT_FILE_OBJ, [], [])
    
    mg.logger.info( "Loading  = %s" % (fn))
    rs,bad = readCSV(fn,4)                      # rs + bad + records read
    rej=loadBulkCred(rs,lt,uflg)
    return len(rs), bad, rej
    
# lt load table, could be server, db, infa_dom, infa_repo
# rs    = array of lists : results  [['1', "'user1'", "'password1'", "''"], ['2', "'user2'", "'password2'", "'Comment 2'"], .. ]
# uflg  = update flag.
# returns an array with rejected rows.
def loadBulkCred(rs,lt,uflg):
    rej = []
    if rs is not None and  len(rs) > 0:
        tbl   = tbl_load[lt][1]       
        cred  = tbl_load[lt][0]()     #cred = _getServID()
        mg.logger.info( "lt = %s table %s uflg = %s " % (lt,tbl,uflg))
        
        for r in rs:
            sn = string.capitalize(r[0].strip(' \t\n\r'))
            if cred.has_key(sn):
                r[0] = cred.get(sn)                    # Need the server ID.
                u = r[1].strip(' \t\n\r');  r[1] = u
                p = r[2].strip(' \t\n\r');  r[2] = p
                if mg.XOR == 1:
                    e = encrypt(r[2],sn); r[2] = e  
                
                b =  ('ID','UNAME','PWD','DESCR')
                d = dict(zip(b,r))
                print "running 'db.insCred(%s,d) .. " % tbl
                rc = _insDBCred(tbl,d)
                #print "RC INSCRED = ", rc
                # Try to Insert else Update
                if (rc == mm.DB_GRAL_ERR or rc == mm.DB_CONSTR_ERR) and uflg == True:   # Row exists in DB, therefore try to UPDATE
                    mg.logger.info( "Row for %s %s user %s exist." %  (tbl,r[0],r[1] ))
                    rc = _updDBCred(tbl,d)
                    mg.logger.info( "Updated Row for sn %s user %s" % (sn,r[1] ))  
                    if rc != 1 :
                        mg.logger.info( "UPD failed rc = %s %s %s user %s exist." % (rc,tbl,r[0],r[1] ))
                
                elif rc == 1 :
                    mg.logger.info( "Inserted Row for sn %s user %s" % (sn,r[1] ))    
                else:
                    mg.logger.error( "Error rc = %s  %s %s user %s exist." % (rc,tbl,r[0],r[1] )) 
                    rej.append(r)   
                print "db rc = ", rc
#            p    = encrypt(r[2])
#            r[2] = p
            else:
                mg.logger.error("%s %s is not defined. Please define" % (tbl,sn))
            print r
           
    else:
        mg.logger.error("Nothing to load. No data or invalid format on file")

    return rej

#===============================================================================
# Web Related methods.
#===============================================================================
# Operations Credentials status.
#===============================================================================

def _updCredStat(tn,ds):
    db = ProcServer(tn, mg.dbHandler, mg.logger)
    d = db.updCredStat(tn, ds)  
    return d

# val[0] = serv_id
# val[1] = uname
# val[2] = status

def _updMemCredStat(t,val): 
    if procCred.has_key(t) : return eval('_upd%sStat(val)' % procCred[t][1])
    return -1

# val list ('ID','UNAME','STATUS',)
def _updMemServCredStat(val):
    i = 0
    for b in mg.serv_cred_rs :
        if b.serv_id == val[0] and b.uname == val[1]:
            mg.serv_cred_rs[i].status = val[2]
            return 1
        i+=1
    return -1

def _updMemDBCredStat(val)  :
    i = 0
    for b in mg.db_cred_rs :
        if b.dbrepo_id == val[0] and b.uname == val[1]:
            mg.db_cred_rs[i].status   = val[2]
            return 1
        i+=1
    return -1
    
def _updMemDomCredStat(val) :
    i = 0
    for b in mg.dom_cred_rs :
        if b.dom_id == val[0] and b.uname == val[1]:
            mg.dom_cred_rs[i].status   = val[2]
            return 1
        i+=1
    return -1
    
def _updMemRepoCredStat(val): 
    i = 0
    for b in mg.repo_cred_rs :
        if b.repo_id == val[0] and b.uname == val[1]:
            mg.repo_cred_rs[i].status   = val[2]
            return 1
        i+=1
    return -1

# tbl table to update.
# val : sid    - Server ID 
#       uname  -
#       status - 0 disable, 1 enable
#       t :  'server',db','infa_dom', 'infa_repo' 
# Returns 1 when update succesfully in mem & storage.

def webUpdCredStat(t,val):
    mg.logger.debug( "t = " , t, "vals = ", val )
    if isinstance(val[0], str) and val[0].isdigit(): sid = int(val[0])
        
    elif isinstance(val[0], int)                   : sid = val[0]
       
    else: return mm.INV_ID_DTYPE 
    
    # Get DB Table Name 
    if tbl_load.has_key(t): tbl = tbl_load[t][1] 
    else                  : return mm.INV_TBL_KEY
     
    if procCred.has_key(t) : return eval('_upd%sStat(val)' % procCred[t][1])
    bindVar = ('STATUS','ID','UNAME')
    ds = dict(zip( bindVar,val)) 
    rc = _updCredStat(tbl,ds)
    if rc == 1: 
        val[0] = sid
        rc = _updMemCredStat(tbl,val) 
        mg.logger.debug( " tbl = " , tbl, "  vals = ", val )
        mg.logger.info(  " rc = %s " % ( rc))   
    return rc    

