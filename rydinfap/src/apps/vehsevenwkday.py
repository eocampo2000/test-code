'''
Created on Sep 2, 2015

@author: eocampo
'''
__version__ = '20150902'

import sys
import os
#import time

import utils.strutils    as su
import utils.filetransf  as ft
import procdata.procinfa as pi 

from apps.infbaseapp       import _InfaBaseApp


class VehSevenWkday(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(VehSevenWkday,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        
        self.RowCnt     = -1
        self.srcFile     = ('zepprsi_eps133d0.dat','compliance_ven.csv','or_compliant_loc.csv','or_compliant_vendors.csv ','or_excluded_vendors.csv','or_master_account_list.txt','tire_exceptions.csv')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'ZEPPRSI.EPS133D0.EXTRACT',COMPLIANCE_VEN.CSV,OR_Compliant_Loc.csv,OR_Compliant_Vendors.csv ,OR_Excluded_Vendors.CSV,OR_MASTER_ACCOUNT_LIST.TXT,tire_exceptions.CSV"
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()

        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock                   ,
                         'B' : self.isWorkDayWarn             ,
                         'C' : self.getFtpFiles               ,  
                         'D' : self.getFtpFiles2              ,  
                         'E' : self.getIncSetFiles            ,
                         'F' : self.copyFilesWorkDir          ,
                         'G' : self.archFilesTS               ,    
                         'H' : self.wkfEmpOverLaborMthly      ,    
                         'I' : self.wkfRepShopOTTimeSheetMthly,    
                         'J' : self.wkfRepTireRebillMthly     ,    
                         'K' : self.wkfAssetShopSupSalesMthly ,    
                         'L' : self.wkfRepOutRepOrderMthly    ,    
                         'M' : self.wkfRepBatNewUsedMthly     ,    
                         'N' : self.wkfAssetFuelAnalyisMthly  ,    
                         'O' : self.wkfAssetFuelTicketMthly   ,    
                         'P' : self.wkfRepOutRepCompMthly     ,    
                         'Q' : self.wkfVendShopSuppCompMthly  ,    
                         'R' : self.wkfAssetShopSuppCompMthly ,    
                         'S' : self.wkfAssetTrack800SuppMthly ,                          
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
    
    def getFtpFiles(self):
        os.environ['FILE'     ] =  (r"\'ZEPPRSI.EPS133D0.EXTRACT\'")   
        
        return ft.getn('VehSevenWkday1',self.log)
    
    def getFtpFiles2(self):
        os.environ['FILE'     ] =  ('COMPLIANCE_VEN.CSV,OR_Compliant_Loc.csv,OR_Compliant_Vendors.csv ,OR_Excluded_Vendors.CSV,OR_MASTER_ACCOUNT_LIST.TXT,tire_exceptions.CSV')
        envVars = {'REMOTE_HOST':'REMOTE_HOST1',    
                   'USER'       :'USER1',    
                   'PWD'        :'PWD1',        
                   'REMOTE_DIR' :'REMOTE_DIR1',    
                   'FTP_MODE'   :'FTP_MODE1',   
                  }
        
        rc = self.setEnvVars(envVars)    
        if rc != 0 : return rc
        
        return ft.get('VehSevenWkday2',self.log)    

    # Wrapper Method
    def copyFilesWorkDir(self):
        for i in range (len(self.srcFile)):
            self.incFiles.append('%s' % self.incFileSet[i][0])
         
        return self.cpSrcToTgtFiles()
    
    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
    
    def wkfEmpOverLaborMthly(self):    
        self.ib.fld = 'Employee'  
        self.ib.wkf = 'wkf_overtime_labor_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfRepShopOTTimeSheetMthly(self):    
        self.ib.fld = 'Repair'      
        self.ib.wkf = 'wkf_shopot_vs_timesheet_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfRepTireRebillMthly(self):    
        self.ib.fld = 'Repair'      
        self.ib.wkf = 'wkf_tire_rebill_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfAssetShopSupSalesMthly(self):    
        self.ib.fld = 'Asset'      
        self.ib.wkf = 'wkf_shop_supply_sales_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfRepOutRepOrderMthly(self):    
        self.ib.fld = 'Repair'      
        self.ib.wkf = 'wkf_outside_repair_order_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfRepBatNewUsedMthly(self):    
        self.ib.fld = 'Repair'    
        self.ib.wkf = 'wkf_battery_new_vs_used_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfAssetFuelAnalyisMthly(self):    
        self.ib.fld = 'Asset'      
        self.ib.wkf = 'wkf_fuel_analysis_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfAssetFuelTicketMthly(self):    
        self.ib.fld = 'Asset'      
        self.ib.wkf = 'wkf_fuel_ticket_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfRepOutRepCompMthly(self):    
        self.ib.fld = 'Repair'      
        self.ib.wkf = 'wkf_outside_repair_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfVendShopSuppCompMthly(self):    
        self.ib.fld = 'Vendor'      
        self.ib.wkf = 'wkf_ven_shop_supply_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfAssetShopSuppCompMthly(self):    
        self.ib.fld = 'Asset'      
        self.ib.wkf = 'wkf_ast_shop_supply_compliance_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    

    def wkfAssetTrack800SuppMthly(self):    
        self.ib.fld = 'Asset'      
        self.ib.wkf = 'wkf_track_800_shop_supplies_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc    
    
       
            
def main(Args):
    a = VehSevenWkday()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)

