'''
Created on Sep 10, 2015

@author: eocampo
'''

__version__ = '20150930'

import sys
import os

from datetime import datetime
import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import datastore.dbapp   as da
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep

cur_dayr   = su.getTodayDtStr('%Y%m%d')

class VehEightWkday(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehEightWkday,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []    # Will need to re-initialize
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile    =  ('cpm_xref_mfr_to_prefix.csv','part_id_except.csv','tire_cmpl_area.csv','vmrs_tmc_cd33.csv') # File that Informatica expects. Alphabetical.
        self.ib.fileName = "CPM_XREF_MFR_TO_PREFIX.csv,PART_ID_EXCEPT.csv,TIRE_CMPL_AREA.csv,VMRS_TMC_CD33.csv"           # SourceFile Name as String, List
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock                     ,
                         'B' : self.isWorkDayWarn               ,
                         'C' : self.getFtpFiles                 , # Sets self.incFileSet
                         'D' : self.getIncSetFiles              ,
                         'E' : self.copyFilesWorkDir            ,
                         'F' : self.archFilesTS                 ,
                         'G' : self.wkfRepTireAreaCompMthly     ,
                         'H' : self.wkfRepTireCostActualMthly   ,
                         'I' : self.wkfRepTireBrkDownMthly      ,
                         'J' : self.wkfRepRCRCTireIncMthly      ,
                         'K' : self.wkfRepTireScoreCardMthly    ,
                         'L' : self.wkfRepTireScoreCardLocMthly ,   
                         'M' : self.wkfAsstPartDescMthly        ,
                         'N' : self.wkfAsstPartSummMthly        ,
                         'O' : self.wkfRepShpVsTmShtExcpMthly   ,
                         'P' : self.wkfAsstPartSummRankMthly    ,
                         'Q' : self.wkfVendVmrsPartCodeDesc     ,      
                         'R' : self.wkfAsstCPMtopXrefBldk       ,      
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
                'INFA_APP_CTL'     : 'self.ib.ctlDir'     ,         
               }
        
        # FTP is expecting the following env variables, which should not be in a config file.
        # First Set of files from Mainframe. 
        # 
        os.environ['RXFILE'   ] =  ('None')    # 
        os.environ['FILE'     ] =  ('CPM_XREF_MFR_TO_PREFIX.csv,PART_ID_EXCEPT.csv,TIRE_CMPL_AREA.csv,VMRS_TMC_CD33.csv')
 
    def getFtpFiles(self):
        return ft.get('VehEightWkday',self.log)  

  # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)

   
    # TIRE_CMPL_AREA.csv
    def wkfRepTireAreaCompMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_tire_area_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wkfRepTireCostActualMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_tire_cost_actuals_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfRepTireBrkDownMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_tire_breakdown_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfRepRCRCTireIncMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_rcrc_tire_invoice_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfRepTireScoreCardMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_tire_scorecard_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wkfRepTireScoreCardLocMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_tire_scorecard_location_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstPartDescMthly(self):      
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_part_description_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    #PART_ID_EXCEPT.csv
    def wkfAsstPartSummMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_part_summary_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfRepShpVsTmShtExcpMthly(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_shopot_vs_timesheet_exception'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstPartSummRankMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_part_summary_rank_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    #VMRS_TMC_OB_CD33.csv, VMRS_TMC_CD33.csv
    def wkfVendVmrsPartCodeDesc(self):
        self.ib.fld = 'Vendor'
        self.ib.wkf = 'wkf_vmrs_tmc_33_parts_code_desc'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    #CPM_XREF_MFR_TO_PREFIX.csv
    def wkfAsstCPMtopXrefBld(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_cpm_top200_xref_build'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

def main(Args):
    a = VehEightWkday()
    rc = a.main(Args)
    return rc     
    
if __name__ == '__main__':    
    rc=  main(sys.argv)
    sys.exit(rc)
