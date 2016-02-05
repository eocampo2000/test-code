'''
Created on June 18, 2015

@author: eocampo

Vehicle DB2 Conversion 5th workday 

Folloing is the list of wkf that need to be ran in sequential order:

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

'''

__version__ = '20150618'

import sys
import os

#import time
from datetime import datetime
import procjobs.procsched as psc
import utils.fileutils   as fu
import utils.strutils    as su
import utils.filetransf  as ft
import datastore.dbapp   as da
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 

RUN_PER_DAY = 1  # Daily runs.
DP_LEN      = len('YYYYMMDD')  
   
   
# Schedules
#SCH_FREQ = 'WDay'
SCH_FREQ = 'Cust'
sch = ('Tue','Wed','Thu','Fri','Sat')

cur_dayr   = su.getTodayDtStr('%Y%m%d')

class VehFifthWkday(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehFifthWkday,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile    = ('PO225D15.dat','PO875D30.dat')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.PO225D15.UMPK(0)','P.PO875D30.WHOUSE.CURR.STD'"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock                 ,
                         'B' : self.isWorkDayWarn           , 
                         'C' : self.getFtpPartDimMthly      ,      
                         'D' : self.wPartDimMthly           ,      
                         'E' : self.getFtpRFPPartUsCanMthly ,      
                         'F' : self.wRFPPartUsCanMthly      ,    
                         'G' : self.getFtpComplMthly        ,      
                         'H' : self.wRFPComplMthly          ,    
                         'I' : self.wJobrComplMthly         ,      
                         'J' : self.wSerComplMthly          ,      
                         'K' : self.wPrtComplMthly          ,    
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
        # Sets a flag, to check for next run.
    


    #FTP RFP_Part_Price.csv, RFP_Part_Xref.csv,MFG_Parts_Match.CSV
    def getFtpPartDimMthly(self):
        os.environ['FILE'     ] =  ('RFP_Part_Price.csv,RFP_Part_Xref.csv,MFG_Parts_Match.CSV') 
        
        return ft.get('PartDimMthly',self.log)
    
    def wPartDimMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_part_dimension_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
        
    #RFP_Part_Price.csv, RFP_Part_Xref.csv, MFG_Parts_Match.CSV, RFP_Part_Price_CAN.csv, RFP_Part_Xref_CAN.csv, MFG_Parts_Match_CAN.CSV, RFP_CMPL_PART_EXCLUDE.csv    
    def getFtpRFPPartUsCanMthly(self):
        os.environ['FILE'     ] =  ('RFP_Part_Price.csv,RFP_Part_Xref.csv,MFG_Parts_Match.CSV,RFP_Part_Price_CAN.csv,RFP_Part_Xref_CAN.csv,MFG_Parts_Match_CAN.CSV,RFP_CMPL_PART_EXCLUDE.csv')
    
        return ft.get('RFPPartUsCanMthly',self.log)    

    def wRFPPartUsCanMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_rfp_parts_us_can_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wRFPComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_rfp_compliance_monthly'
        rc = 0
        #rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wJobrComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_jobr_compliance_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wSerComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_ser_compliance_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wPrtComplMthly(self):
        self.ib.fld = 'Vehicle'
        self.ib.wkf = 'wkf_prt_compliance_monthly'
        rc = 0
        #rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wkfAsstRFPCompCanMthly(self):      
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_rfp_comp_can_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstRFPCompMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_rfp_compliance_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstPurchCompMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_purchasing_compliance_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstRankResMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_ppc_ranking_research_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wkfAsstPartPriceMonMthly(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_part_price_monitor_monthly'
        #rc = pi.runWkflWait(self.ib,self.log)
        rc = 0
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
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()     
    
    rc=  main(sys.argv)
    sys.exit(rc)
    