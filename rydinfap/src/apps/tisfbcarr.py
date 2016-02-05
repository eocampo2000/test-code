'''
Created on May 29, 2015

@author: eocampo

Program: This program  multiplexes one row to several based on DB field pronums, that contains delimeted list of ProNums. One row will be inserted in the
Target table based on each ProNum.
1) Ignore blank or empty pro-nums.
2) Truncate if PRON_NO length is greater than column width to COL_LEN.
3) Use odbc or native based on number of rows.

EO 20150825 Added NZ_BIN env var.
   20150826 Queued and process request individually.
'''

__version__ = '20150826'

import sys

import utils.strutils  as su
import utils.fileutils as fu 
import proc.process    as proc

from apps.infbaseapp   import _InfaBaseApp

# Mandatory to define self.cmdStep

cur_dayr   = su.getTodayDtStr('%Y%m%d')
COL_LEN = 50

class TisFBCarrImp(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(TisFBCarrImp,self).__init__()
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 

        
        self.ts        =  su.getTimeSTamp()

        self.delCSVFileFlg  = False

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock        ,                        
                         'B' : self.delCSVFile     ,
                         'C' : self.procCarrPrompt ,
                         'D' : self.delCarrPrompt  ,
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = {
                 'INFA_SHARE'       : 'self.ib.shareDir'   ,  
                 'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                 'INFA_APP_LCK'     : 'self.ib.lckDir'     ,   
                 'INFA_APP_DAT'     : 'self.ib.dataDir'    ,   
                 'NZ_BIN'           : 'self.ib.nzbin'      ,
               }
                
        
    def _setDataDir(self) : pass
        
    def delCSVFile(self):    
        self.delCSVFileFlg = True
        return 0
   
    # rs =  [(4L, ), (18L, ), (19L, ), (20L, ), (21L, )]
    def _getSeqIDtoProc(self):
        sql = "select seqid from tis_fb_carr_pmpt_imprt where proc_flag=0 order by seqid "
        rs   = self._getNZDS(sql)
        self.log.debug('rs ' , rs)
        return rs

    # Exclusive conn for the select.    
    def _getCarrPrompt(self,seqid) : 
         
        sql = """ SELECT seqid, user_id, pro_numbers, date_time FROM tis_fb_carr_pmpt_imprt
                  WHERE seqid = %s AND proc_flag = 0 """ % seqid
        rs   = self.dbh.runQry(sql)
        self.log.debug('rs ' , rs)
        return rs

    # Syntax nzload -host sscptdnetezza2a -u edwetl -pw invetl0530 -db DW_STG_DEV -t tis_fb_carr_pmpt_imprt_dtl -delim ',' -df 37.csv
    #        -lf tis_fb_carr_pmpt_imprt_dtl_37.nzlog -bf tis_fb_carr_pmpt_imprt_dtl_37.nzbad
    def _insNZLoad(self,val,seqid):
        ldata = []
        for s,u,p,d in val :
            ldata.append("%s,%s,%s,%s\n" % (s,u,p,d ))
        data = ''.join(ldata)

        # Create CSV file
        fn = '%s/%s.csv' % (self.ib.dataDir,seqid)
        fl = '%s/tis_fb_carr_pmpt_imprt_dtl_%s.%s.nzlog' % (self.ib.dataDir,seqid,self.ib.db)
        bf = '%s/tis_fb_carr_pmpt_imprt_dtl_%s.%s.nzbad' % (self.ib.dataDir,seqid,self.ib.db)
        rc = fu.createFile(fn, data)
        self.log.debug('File Creation %s/%s.csv rc = %s' % (d,seqid,rc))
        if rc != 0 :
            self.log.error('File Creation %s/%s.csv rc = %s' % (d,seqid,rc))
            return rc

        # Invoke nzload command
        nz_cmd = "%s/nzload -host %s -u %s -pw %s -db %s -t tis_fb_carr_pmpt_imprt_dtl -delim '%s' -df %s -lf %s -bf %s" % (self.ib.nzbin,
                                                                                                           self.ib.dbserver,
                                                                                                           self.ib.user,
                                                                                                           self.ib.pwd,
                                                                                                           self.ib.db, 
                                                                                                           self.ib.delim, fn,fl,bf)      

        self.log.debug('nz_cmd = %s' % nz_cmd) 

        rc,rmsg = proc.runSync(nz_cmd,self.log)
        if rc != 0 :
            self.log.error('cmd %s rc = %s -> msg= %s' % (nz_cmd,rc,rmsg))
            return rc

        # Delete csv file (based on option)
        if self.delCSVFileFlg is True:
            r = fu.delFile(fn)
            self.log.debug('Deleting %s rc = %s ' % (fn,r))
            if r != 0 : self.log.error('Cannot Delete %s ' % fn )

        return rc
    
    def _insODBC(self,val):
        sql = " INSERT INTO tis_fb_carr_pmpt_imprt_dtl (seqid,user_id,pro_number,date_time) VALUES (?, ?, ?, ?) " 
        
        rc = self.dbh.exeManyQry(sql,val)
        self.log.debug('rc = %s' % rc) 
        return rc
    
    # 1 row of data to be multiplex. 
    # Length already checked.
    # Inserts everything in one transaction.
    # Updates the correspondigbn Parent table.
    def _insCarrPromptDet(self, rs):   
        seqid = rs[0]; uid = rs[1]; procNums = rs[2] ; dt = rs[3]       
        self.log.debug('seqid=%s uid=%s procNum=%s dt=%s' % (seqid,uid ,procNums ,dt))
        val = []
        procNumLst =  procNums.split(self.ib.delim)
        for pn in procNumLst:
            pns = pn.strip()
            if su.isBlank(pns) : continue
            if len(pns) > COL_LEN :
                p = pns[0:COL_LEN]
                self.log.warn('Truncate string %s to %s COL_LEN= %d ' % (pns,p, COL_LEN))
                pns = p
            val.append([seqid,uid,pns,dt])
       
        lim = su.toIntPos(self.ib.odbc_lim)
        lval = len(val)
        self.log.debug('seqid=%s lim=%s lval=%s' % (seqid,lim,lval))
        if lval < 1 : 
            self.log.error('Nothing to load ')
            return 2

        if ( lval < lim ):
            rc = self._insODBC(val)
        else:
            rc = self._insNZLoad(val,seqid)
            
        if rc != 0 :
            self.log.error("Issue inserting child records for seqid = %s" % seqid)
            return rc

        # Update Parent table proc_flag
        sql = "update tis_fb_carr_pmpt_imprt set proc_flag = 1  where seqid = ? "
        val = []
        val.append(seqid)
        rc = self.dbh.exeQry(sql,val)
        self.log.debug('updating parent table seqid=%s rc = %s ' % (seqid,rc))
        if rc < 0 :   # This mehod provide the number of update rows
            self.log.error('updating parent table seqid = %s rc = %s ' % (seqid,rc))
            return rc
         
        return 0
        
    def _procCarrPromptSeqIDs(self,sqid):
        errFlg = 0
        self.log.debug('deqid to process = ', sqid)   
        for s in sqid:
            self.log.info('Processing seqid = %s' % s[0])
            rs = self._getCarrPrompt(s[0])
            if rs is None      : 
                self.log.error('rc is Null when querying for seqid %s' % s[0])
                errFlg+=1
                continue
            
            rc = self._procCarrPrompt(rs)
            if rc != 0 : 
                self.log.error('Processing seqid %s' % s[0])
                errFlg+=1
                continue
            
            #rc = self._procCarrPrompt(rs)
    
        return rc 
        
    # rs complete result set with 1 or more rows.
    def _procCarrPrompt(self,rs):
        errFlg = 0
        self.log.debug('len rs = ', len(rs), 'rs = ', rs)   
        for r in rs:
            if len(r) != 4 :
                self.log.error('Error on record ', r , 'Len of all records needs to be 4 not ', len(r))
                errFlg+=1
                continue

            rc = self._insCarrPromptDet(r)
            if rc != 0 : errFlg+=1
        return errFlg

    def procCarrPrompt(self):
        # Get seqids to be processed.
        sqid = self._getSeqIDtoProc()
        if sqid is None      : 
            self.log.error('rc is Null when querying : tis_fb_carr_pmpt_imprt')
            return 1 
        
        if len(sqid) == 0 : 
            self.log.debug('No records to process.')
            return 101 # RET_WARN
              
        rc = self._connToDB('NZODBC')
        if rc != 0 :
            self.log.error('Error Connecting to DB = %s' % rc)
            return rc
        
        rc = self._procCarrPromptSeqIDs(sqid)
        
        self._disFromDB()

        return rc
            
    def delCarrPrompt(self):
        val = []
        rc = self._connToDB('NZODBC')
        if rc != 0 :
            self.log.error('Error Connecting to DB = %s' % rc)
            return rc
        
        self.log.info('Deleting records older than %s' % self.ib.numdays)
        val.append(su.toInt(self.ib.numdays))

        sql = "delete from tis_fb_carr_pmpt_imprt where proc_flag = 1 and date_time < current_date - ?"        
        rc = self.dbh.exeQry(sql,val)
        self.log.debug('qry %s  rc= %s ' % (sql,rc))
        if rc < 0 :
            self.log.error('qry = %s rc = %s' % (sql,rc))
            return rc 
        
      
        #print "del records from tis_fb_carr_pmpt_imprt  rc ", rc
             
        sql = "delete from tis_fb_carr_pmpt_imprt_dtl where seqid > 0 and date_time < current_date - ?"
        rc = self.dbh.exeQry(sql,val)
        self.log.debug('qry %s  rc= %s ' % (sql,rc))

        if rc < 0 :
            self.log.error('qry = %s rc = %s' % (sql,rc))
            return rc 
        
        return 0
           
def main(Args):
    a = TisFBCarrImp()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
