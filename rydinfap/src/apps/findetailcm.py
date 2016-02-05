'''
Created on Aug 6, 2013

@author: eocampo

To run new product lines (Contract Manager Project)

USe this program for testing only. In prod you need to run the findetail.
'''

import sys

import utils.strutils    as su
from findetail  import FinDetail


__version__ = '20130806'

RUN_PER_DAY = 1     # Daily runs.
DP_LEN      = len('YYYYMMDD')  

SCH_FREQ = 'Cust'
sch = ('Mon','Tue','Wed','Thu','Fri','Sat')

cur_dayr   = su.getTodayDtStr('%Y%m%d')

class FinDetailCM(FinDetail):  
       
    exitOnError = True

    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(FinDetailCM,self).__init__()
        self.landDir    = 'SrcFiles/finance_cm'
        self.incFiles   = []                  # Incoming Files. Contains full path name.
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles). 
        self.badFiles   = []                  # Files with Errors. (trailer) 
        self.incFileSet = []                  # Holds set of files. Populated using getIncSetFiles. List with all pertinent files.
        
        self.ib.srcFile = ('Ap_sce5100.txt', 'Ap_scg4000.txt', 'Ap_scg4100.txt', 'Ap_scudrldn.txt', 'Ap_scusrctl.txt')  # Sorted Files that Informatica expects
     
        self.srcCount   = {  'Ap_sce5100.txt' : -1,         # Use to verify row counts from incoming files.
                             'Ap_scg4000.txt' : -1,
                             'Ap_scg4100.txt' : -1,
                             'Ap_scudrldn.txt': -1,
                             'Ap_scusrctl.txt': -1,
                          }

        self.ctlTbl    = {   'Ap_sce5100.txt' : 'SCE5100',   # Use to verify row counts from populated tables.
                             'Ap_scg4000.txt' : 'SCG4000',
                             'Ap_scg4100.txt' : 'SCG4100',
                             'Ap_scudrldn.txt': 'SCUDRLDN',
                             'Ap_scusrctl.txt': '',
                          }

        self.FILE_SET_LEN = len(self.ib.srcFile)
        
        self.checkNextRunFlg  = False
        self.runWkfFlowFlg    = False
        
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getIncSetFiles  ,   # Sets self.incFileSet
                         'C' : self.chkNextRunFlg    ,  # To verify if no cycle is missed. 
                         'D' : self.rWkfFlowFlg      ,  # To execute wkfl.
                         'F' : self.procIncFiles     ,
                         'Z' : self.postCheck        ,
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

def main(Args):
    a = FinDetailCM()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
    