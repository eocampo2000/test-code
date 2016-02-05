'''
Created on June 18, 2015

@author: eocampo

Vehicle DB2 Conversion 5th workday 

Following is the list of wkf that need to be ran in sequential order:

    wkf_part_dimension_monthly
    wkf_rfp_parts_us_can_monthly
    wkf_rfp_compliance_monthly
    wkf_jobr_compliance_monthly
    wkf_ser_compliance_monthly
    wkf_prt_compliance_monthly
    
    self.wPartDimMthly,
    self.wRFPPartUsCanMthly,
    self.wRFPComplMthly,
    self.wJobrComplMthly,
    self.wSerComplMthly,
    self.wPrtComplMthly,

    FTP FILES:
    wPartDimMthly        RFP_Part_Price.csv,RFP_Part_Xref.csv,MFG_Parts_Match.CSV
    RFPPartUsCanMthly    RFP_Part_Price.csv,RFP_Part_Xref.csv,MFG_Parts_Match.CSV,RFP_Part_Price_CAN.csv,RFP_Part_Xref_CAN.csv,MFG_Parts_Match_CAN.CSV,
                         RFP_CMPL_PART_EXCLUDE.csv
    ComplMthly           RFP_ADJ.csv,RFP_NAVISTAR.txt
    
    
    20150909 Added wkf_rfp_comp_us_can_monthly.
'''

__version__ = '20150909'

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

class VehFifthWkday(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehFifthWkday,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []    # Will need to re-initialize
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile    = ('mfg_parts_match.csv','mfg_parts_match_can.csv','rfp_adj.csv','rfp_cmpl_part_exclude.csv','rfp_navistar.txt','rfp_part_price.csv','rfp_part_price_can.csv','rfp_part_xref.csv','rfp_part_xref_can.csv')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = "MFG_Parts_Match.CSV,MFG_Parts_Match_CAN.CSV,RFP_ADJ.csv,RFP_CMPL_PART_EXCLUDE.csv,RFP_NAVISTAR.txt,RFP_Part_Price.csv,RFP_Part_Price_CAN.csv,RFP_Part_Xref.csv,RFP_Part_Xref_CAN.csv"  # SourceFile Name as String, List
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock                ,
                         'B' : self.isWorkDayWarn          ,
                         'C' : self.getFtpFiles            , # Sets self.incFileSet
                         'D' : self.getIncSetFiles         ,
                         'E' : self.copyFilesWorkDir       ,
                         'F' : self.archFilesTS            ,
                         'G' : self.wPartDimMthly          ,
                         'H' : self.wRFPPartUsCanMthly     ,
                         'I' : self.wRFPComplMthly         ,
                         'J' : self.wJobrComplMthly        ,
                         'K' : self.wSerComplMthly         ,
                         'L' : self.wPrtComplMthly         ,  
                         'M' : self.wkfRFPCompUSCanMthly   ,                         
                         'N' : self.wkfAsstRFPCompCanMthly  ,
                         'O' : self.wkfAsstRFPCompMthly     ,
                         'P' : self.wkfAsstPurchCompMthly   ,
                         'Q' : self.wkfAsstRankResMthly     ,
                         'R' : self.wkfAsstPartPriceMonMthly,                         
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
        os.environ['FILE'     ] =  ('MFG_Parts_Match.CSV,MFG_Parts_Match_CAN.CSV,RFP_ADJ.csv,RFP_CMPL_PART_EXCLUDE.csv,RFP_NAVISTAR.txt,RFP_Part_Price.csv,RFP_Part_Price_CAN.csv,RFP_Part_Xref.csv,RFP_Part_Xref_CAN.csv')
 
    def getFtpFiles(self):
        return ft.get('VehFithtWkday',self.log)  

  # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)

    #RFP_Part_Price.csv, RFP_Part_Xref.csv,MFG_Parts_Match.CSV
    def wPartDimMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_part_dimension_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    #RFP_Part_Price.csv, RFP_Part_Xref.csv, MFG_Parts_Match.CSV, RFP_Part_Price_CAN.csv, RFP_Part_Xref_CAN.csv, MFG_Parts_Match_CAN.CSV, RFP_CMPL_PART_EXCLUDE.csv    
    def wRFPPartUsCanMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_rfp_parts_us_can_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

   
    #RFP_ADJ.csv, RFP_NAVISTAR.txt    
    def wRFPComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_rfp_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    #RFP_ADJ.csv 
    def wJobrComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_jobr_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wSerComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_ser_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wPrtComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_prt_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc


    def wkfRFPCompUSCanMthly(self):   
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_rfp_comp_us_can_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    
    def wkfAsstRFPCompCanMthly(self):      
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_rfp_comp_can_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstRFPCompMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_rfp_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstPurchCompMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_purchasing_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstRankResMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_ranking_research_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstPartPriceMonMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_part_price_monitor_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

def main(Args):
    a = VehFifthWkday()
    rc = a.main(Args)
    return rc     
    
if __name__ == '__main__':    
    rc=  main(sys.argv)
    sys.exit(rc)
