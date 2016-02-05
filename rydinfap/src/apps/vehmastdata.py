'''
Created on Apr 3, 2015

@author: eocampo

To run Vehicle Master Data infa processes. 
20160127  Added 3 new workflows sourcing from NRV. 
20160203  Added trigger file, additional wkf and ftp logic
 
'''
__version__ = '20160203'

import sys

from apps.infbaseapp import _InfaBaseApp
import procdata.procinfa as pi 
import utils.filetransf  as ft
import utils.fileutils   as fu
import utils.strutils    as su

# mandatory to define self.cmdStep

class VehMasterData(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehMasterData,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        self.trigFiles  = []      # Incoming Trigger File.
        
        self.RowCnt     = -1
        self.ib.fileName = r"'P.PO230D15.VPODATA(0)'"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. 
         
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock       ,
                         'B' : self.getTrigFiles  ,
                         'C' : self.wStgSAMCurr   ,
                         'D' : self.wStgSAMRefCDC ,
                         'E' : self.wStgSAMCDC    ,
                         'F' : self.wVMDStgRef    ,
                         'G' : self.wVMDStg       ,
                         'H' : self.wStgSAMPrev   ,
                         'I' : self.wVMDNRVTab    ,
                         'J' : self.wVMDNRVVinVal ,
                         'K' : self.wLoadETLCTRLAudit,
                         'L' : self.txDataFile    ,  
                         'M' : self.txTrigFile    ,                        
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = {
                'PMCMD'            : 'mg.pmcmd'           ,
                'INFA_USER'        : 'self.ib.rep_user'   ,
                'INFA_XPWD'        : 'self.ib.rep_xpwd'   ,
                'DOMAIN'           : 'self.ib.dom_name'   ,
                'INT_SERV'         : 'self.ib.IS'         , 
                'INFA_SHARE'       : 'self.ib.shareDir'   ,  
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,   
               }
 
    def _setDataDir(self) : return 0
    
    
    def wStgSAMCurr(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_STG_SAM_CURR_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wStgSAMRefCDC(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_STG_SAM_REF_CDC_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wStgSAMCDC(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_STG_SAM_CDC_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wVMDStgRef(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_VMD_STG_REF_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wVMDStg(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_VMD_STG_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wStgSAMPrev(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_STG_SAM_PREV_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    
    def wVMDNRVTab(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_VMD_NRV_Tables'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wVMDNRVVinVal(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_VMD_NRV_VIN_Validation'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wLoadETLCTRLAudit(self):
        self.ib.fld = 'VMD'
        self.ib.wkf = 'wkf_Load_etl_ctrl_audit'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
        
    def txDataFile(self):  
        rc= ft.put(self.appName,self.log)
        return rc 

    def txTrigFile(self):
        envVars = {'FILE':'FILE1', }
        rc = self.setEnvVars(envVars)    
        if rc != 0 : return rc
        
        rc= ft.put(self.appName,self.log)
        return rc 

def main(Args):
        a = VehMasterData()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
    sys.exit(rc)