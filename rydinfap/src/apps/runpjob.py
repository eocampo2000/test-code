'''
Created on Oct 3, 2013

@author: eocampo
'''

__version__ = '20131024'

import sys
import time
from apps.infbaseapp import _InfaBaseApp
import procdata.procinfa as pi 
import utils.strutils    as su

# mandatory to define self.cmdStep
class RunPJob(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(RunPJob,self).__init__()

        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.isWorkDayWarn       ,
                         'C' : self.chkPredComplete     ,
                         'D' : self.wfactInvChg         ,
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
    
    def _setDataDir(self): pass
        
    # This method check if Predecessor conditions are met:
    # isSourcingReady = 1 AND SYSDATE -1 Month = MAX(MonthDate)
    def _chkSrcReady(self):
        qryStr = """ select MonthDate, CAST(isSourcingReady AS int) 
                     from dbo.DataReady
                     WHERE MonthDate = (select MAX(MonthDate) from dbo.DataReady)"""     
        
        self.ib.user = self.ib.ms_user; self.ib.pwd = self.ib.ms_pwd;  self.ib.dbserver = self.ib.ms_dbserver ; self.ib.db = self.ib.ms_db
        
        rs = self._getMSSQLNatDS(qryStr)
        self.log.debug('qryStr = ',  qryStr , ' rs ', rs)  
        
        if len(rs) != 1 : return -1
        
        isRdy = rs[0][1]
        rdtm  = su.getDtStr(rs[0][0],'%m%Y')
        todm  = su.getTodayDtStr('%m%Y')
        nmth  = su.getMonthPlusStr(1,rdtm,'%m%Y')
        self.log.debug("isRdy = %s rdtm=%s todm=%s nmth=%s" % (isRdy,rdtm,todm,nmth))
        if isRdy == 1 and  nmth == todm : # and sysdate - 1 = dt 
            self.log.info ("Ready to Proceed : rdtm = %s isRdy = %s " % (rdtm,isRdy))
            return 0
        
        return 1
            
    def chkPredComplete(self):

        rc = self._chkSrcReady()
        if rc == 0 :  return rc
            
        i = 1        
        wi = int(self.ib.waitPredIter)        
        while (i <= wi):            
        
            self.log.info('Iteration %d : Next Iteration in %s sec' % (i,self.ib.waitPred))
            time.sleep(float(self.ib.waitPred))
            rc = self._chkSrcReady()
            if rc == 0 :  return rc            
            i+=1
        self.log.error('Source is not Ready to Proceed run %d iterations ' % wi)
        return 1
     
    def wfactInvChg(self):
        self.ib.fld = 'CINV_Invoice_Data'
        self.ib.wkf = 'wkf_cinv_fact_invoice_charges'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    

        
def main(Args):
        a = RunPJob()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)
