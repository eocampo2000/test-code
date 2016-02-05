'''
Created on Jun 22, 2012

@author: eocampo
20130429 New Class Style
20140911 Change isWorkDay to isWorkDayWarn
'''
__version__ = '20140911'

import sys
#import mainglob         as mg   
#import utils.fileutils   as fu

#from datastore.dbutil  import NZODBC, getDSConnStr
import datastore.dbapp   as da
import utils.fileutils   as fu
import procdata.procinfa as pi 

from pegbillmonth import PegBillMonth
# mandatory to define self.cmdStep

class PegBillMonthDean(PegBillMonth):  
    exitOnError = True
    
    def __init__(self):
        #PegBillMonth.__init__(self)
        super(PegBillMonth,self).__init__()
        self.landDir    = 'SrcFiles/Repair'
        self.trigFiles  = []                  # Incoming Files
        self.procFiles  = []                  # Files to process
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles)
        self.badFiles   = []                  # Files with Errors. (trailer) 
        self.pBillTot   = ''                  # To Store Peg Bill s Count Totals.
        
        self.ib.srcFile = 'pegbillmonthdean.txt'  # File that Informatica expects
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock                  ,
                         'B' : self.isWorkDayWarn            ,
                         'C' : self.wPegPO                   ,
                         'D' : self.getTrigFiles             ,
                         'E' : self.mvTrigFileToArchDir      ,
                         'F' : self.wMonthPEGVehDeanFoodLoad ,
                         'G' : self.wMonthBillLoad           ,
                         'H' : self.wMonthToFisDeanFood      ,
                         'N' : self.notifyUsers              ,
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
    
              
    def wMonthPEGVehDeanFoodLoad(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Monthly_PEG_Vehicle_DeanFoods_Load' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
            
    def wMonthToFisDeanFood(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Monthly_to_FIS_DeanFoods_Load' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))   
        return rc

def main(Args):
        a = PegBillMonthDean()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()         # Remove in UX 
    rc=  main(sys.argv)