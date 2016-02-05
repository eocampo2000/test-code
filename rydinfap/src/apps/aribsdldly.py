'''
Created on Oct 5, 2015

@author: eocampo

'''

__version__ = '20151005'

import sys
#import time
from datetime import datetime
import procjobs.procsched as psc
import utils.fileutils   as fu
import utils.strutils    as su
import procdata.procinfa as pi 

from apps.infbaseapp  import _InfaBaseApp
# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 

RUN_PER_DAY = 1     # Daily runs.
DP_LEN      = len('YYYYMMDD')  


SCH_FREQ = 'Dly'
cur_dayr   = su.getTodayDtStr('%Y%m%d')
   
class SdlAribaDly(_InfaBaseApp):  
    
    exitOnError = True

    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(SdlAribaDly,self).__init__()
        self.landDir    = 'SrcFiles/rdw_cp ariba'
        self.incFiles   = []                  # Incoming Files. Contains full path name.                 
        self.procFiles  = []                  # Files to process, that were moved to the working dir .(ideally same than incFiles). 
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles)
       
        # Mandatory Files.
        self.ib.srcFile    = ('InvoiceDetailExport.csv','InvoiceHeaderExport.csv','InvoiceSplitAccountingExport.csv','PurchaseOrderDetailExport.csv',
                              'PurchaseOrderExport.csv','PurchaseOrderSplitExport.csv','ReceiptAssetExport.csv','ReceiptDetailExport.csv',
                              'ReceiptHeaderExport.csv','Supplier_Export.csv','SupplierLocation_Export.csv')  # Sorted Files that Informatica expects
     
        # Optional Files if not found create a 0 byte file.
        self.ib.srcOptFile = ('Address_Load.csv','CommodityCodesExport.csv','PaymentBankExport.csv','PaymentDetailExport.csv',
                              'PaymentHeaderExport.csv','PaymentSplitAccountingExport.csv')

        self.ib.chkFiles   = []                 # To check Incoming file set.
        self.FILE_SET_LEN = len(self.ib.srcFile)
        
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock        ,
                         'B' : self.getMandFiles   ,  # self.ib.srcFile 
                         'C' : self.getOptFiles    ,
                         'D' : self.cpFileToWorkDir,  # Populates self.workFiles 
                         'E' : self.archFilesTS    ,  # Move files to archive
                         'F' : self.chkNextRunFlg  ,  # To verify if no cycle is missed. 
                         'G' : self.rWkfFlowFlg    ,  # To execute wkfl.
                         'H' : self.procIncFiles   , 
                         'I' : self.remWorkFiles   ,  # Remove Input Files from Working directory.
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
    
    def chkNextRunFlg(self) : 
        self.checkNextRunFlg  = True
        return 0
    
    def rWkfFlowFlg(self) : 
        self.runWkfFlowFlg    = True
        return 0                    

    # This method Checks all the mandatory files as per requirements. 
    # If no error will assign self.incFiles for further 
    def getMandFiles(self):                 
        for f in self.ib.srcFile:   self.ib.chkFiles.append('%s/%s/%s' % (self.ib.shareDir,self.landDir,f))
        self.log.debug('SrcFiles = ', self.ib.srcFile , 'CheckFiles = ' , self.ib.chkFiles)
        rc = self.chkFiles()
        if rc == 0 : self.incFiles = self.ib.chkFiles 
        return rc    
    
    # This method will create empty files if optional file not present in landing directory.
    def getOptFiles(self): 
        rc = 0
        for f in self.ib.srcOptFile: 
            fnp = '%s/%s/%s' % (self.ib.shareDir,self.landDir,f)
            self.incFiles.append(fnp)           # Need to be in the incset to be moved to working dir.
            if fu.fileExists(fnp): 
                self.log.debug('fnp exists %s' % fnp)
            
            else : 
                r = fu.crtEmpFile(fnp)
                if r == True : self.log.info ('Touching File %s' % fnp)
                else          : 
                    self.log.Error ('Touching File %s' % fnp)
                    rc+=1
     
        return rc    


    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
    
    def wflSdlAribaDly(self):
        self.ib.fld     = 'sdl'
        self.ib.wkf     = 'wkf_SDL_ARIBA_DLY'
        
        rc = pi.runWkflWait(self.ib,self.log) 
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def procIncFiles(self):
     
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)
        self.log.debug('self.checkNextRunFlg is %s. Loading Control File %s ' %  (self.checkNextRunFlg,ctlFile))
        if self.checkNextRunFlg is True:
                
            # Get Previous Run Info. File should contain one line only :  YYYYMMDD_R from storage.
            prev_dayr = self._getCtlFile()      
            if prev_dayr is None or prev_dayr.strip() == '':
                self.log.error("Could not find control file or No Data")
                return -1

            rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log)
            if rc != 0 : 
                self.log.error("self._chkNextRun rc = %s" % rc)
                return rc  
            
        self.log.debug('self.runWkfFlowFlg is %s' %  self.runWkfFlowFlg)
        if self.runWkfFlowFlg == True:
            rc = self.wflSdlAribaDly()
            if rc != 0 : return rc 

            # End to End Loading Succeeded. Update the control file only after wkf ran.
            rc = fu.updFile(ctlFile,cur_dayr)
            if rc == 0 : self.log.info('Updated Cur Load Date %s, Control File %s' % (cur_dayr,ctlFile))
            else       :
                self.log.error('Could not Update Load Date %s, Control File rc = %s' % (cur_dayr,ctlFile,rc))
                return rc
        return rc   
    
        
def main(Args):
    a = SdlAribaDly()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
    
