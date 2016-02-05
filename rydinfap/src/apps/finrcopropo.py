'''
Created on Mar 4, 2015

@author: eocampo

Code will check for MS SQL predecessors before invoking Informatica wkf.
Note as per conversation with Debashis, this process does not need to control previous run.
Predecessors in MS SQL are :

ConsolidatedSHPOM  
ConsolidatedSMWIP
ConsolidatedSIPOD
ConsolidatedSHPOD  
ConsolidatedSIPOM
ConsolidatedSIAPD

'''

__version__ = '20150304'

import sys

import utils.fileutils    as fu
import utils.strutils     as su
import procdata.procinfa  as pi 
import datastore.dbapp    as ds

from apps.infbaseapp  import _InfaBaseApp

# Mandatory to define self.cmdStep

class FinRCOpenROPO(_InfaBaseApp):  
    
    
    exitOnError = True

    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(FinRCOpenROPO,self).__init__()
        self.landDir    = 'SrcFiles/finance'
        self.incFiles   = []                  # Incoming Files. Contains full path name.
        self.workFiles  = []                  # Files that were moved to the working dir (ideally same than incFiles). 
        self.badFiles   = []                  # Files with Errors. (trailer) 
        self.incFileSet = []                  # Holds set of files. Populated using getIncSetFiles. List with all pertinent files.
        
        self.checkNextRunFlg  = False
        
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.checkDBPred     ,
                         'C' : self.wFinRCOpenROPO  ,
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
    
    # Predecessor methods
    def getSHPOMPred(self): return self._getROPOPred('ConsolidatedSHPOM')
    def getSMWIPPred(self): return self._getROPOPred('ConsolidatedSMWIP')
    def getSIPODPred(self): return self._getROPOPred('ConsolidatedSIPOD')
    def getSHPODPred(self): return self._getROPOPred('ConsolidatedSHPOD')
    def getSIPOMPred(self): return self._getROPOPred('ConsolidatedSIPOM')
    def getSIAPDPred(self): return self._getROPOPred('ConsolidatedSIAPD')
    
    def _getROPOPred(self,tbn):
    
        qryStr = ds.selFinRCOpenROPOQry % tbn
        rs = self._chkMssqlRdy(qryStr)
        self.log.debug('rs = ' , rs)
        
        # rs = [(u'ConsolidatedSHPOD   ', '20150310', 6240165)]
        if rs is None or len(rs) < 1 or len(rs[0]) != 3   : return -1
        tbn = rs[0][0]; dt = rs[0][1] ; rowc = rs[0][2] ;
        self.log.info('tbn = %s dt = %s rowc = %s ' % (tbn,dt,rowc))
        
        # Check if date current date 
        todm  = su.getTodayDtStr('%Y%m%d')
        self.log.debug('dt = %s todm = %s' % (dt,todm))
        if  dt != todm : return 1
        
        # Check if number of rows for today are > 0 
        irowc = su.toInt(rowc)
        self.log.debug('irowc = %s' % (irowc))
        if  irowc is None or irowc < 1   : return 2
    
        return 0
    
    def checkDBPred(self):
        pl = ('getSHPOMPred()','getSMWIPPred()','getSIPODPred()','getSHPODPred()','getSIPOMPred()','getSIAPDPred()',)
        return self.chkPred(pl)      
    
    def wFinRCOpenROPO(self):
        self.ib.fld     = 'Financial'
        self.ib.wkf     = 'wkf_FIN_RC_Open_RO_PO'
        self.log.info('UNCOMMENT ->  pi.runWkflWait, self.ib = %s ' % self.ib)
        rc=0 # TODO Remove
        #rc = pi.runWkflWait(self.ib,self.log) 
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
            
def main(Args):
    a = FinRCOpenROPO()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
    
