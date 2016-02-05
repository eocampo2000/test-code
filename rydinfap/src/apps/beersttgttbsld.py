'''
Created on Nov 4, 2013

@author: eocampo
This application will invoke wkf_TGT_TBS_Load and will send an email to users if we have undefined vehicle numbers.

'''

__version__ = '20131105'

import sys
from apps.infbaseapp import _InfaBaseApp   
import datastore.dbapp as da
import procdata.procinfa as pi 

class BeerStTgtTBSLoad(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(BeerStTgtTBSLoad,self).__init__()
        self.uVLst     = []     # User Vehicle List
        self.dSrcLoad  = ''
        self.totMisMat = -1
             
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock           ,
                         'B' : self.wkfTgtTBSLdLst    ,                            
                         'N' : self.notifyUsers       ,
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
 
    def _setDataDir(self) : return  0
         
    def wkfTgtTBSLdLst (self):
        self.ib.fld = 'BeerStore'
        self.ib.wkf = 'wkf_TGT_TBS_Load' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    # Check for a list of mismatched vehicles.
    # returns 0 if there were mismatches. 2 otherwise    
    def _getUnMatchVehList(self):  
        
        rc = 0
        rs = self._getNZDS(da.beerStUnmatchVehSql)
        self.log.debug("rs = ", rs)
        if rs is None : return 5                     # Error with Connection.
        self.totMisMat = len(rs)
        self.log.info('Found %d mismatch(es)' % self.totMisMat)
        if  self.totMisMat > 0 : 
                 
            for custNo,rvehNo in rs:
                self.uVLst.append([ ('%s' % custNo),('%s' % rvehNo),])     

            self.log.debug(' self.uVLst = \n', self.uVLst)          
        
        else               : 
            self.log.warn('No results at this time. No Mismatch found.')
            rc = 2 # . Should not email as they are no results.
       
        return rc
    
    # This method will build email subj/text based on results.
    # SE sets  self.dSrcLoad 
    def _procEmail(self):

        rc = self._getUnMatchVehList()
        if rc == 0 :
            self.dSrcLoad += '\nCustomer No\tRyder Vehicle No\n'
            for custNo,rvehNo in self.uVLst:
                self.dSrcLoad += '%s\t\t%s\n' % (custNo,rvehNo)    
        self.log.debug('self.dSrcLoad = \n',self.dSrcLoad)                             
        return rc
    
    def notifyUsers(self):
        self.files = []
        rc = 1
        
        try: 
            m = -1 ; p = -1
            m = eval(self.ib.mailOnErr) ; p = eval(self.ib.pgOnErr)
        except:
            self.log.error('Error with configuration value(s) self.ib.mailOnErr = %s self.ib.pgOnErr = %s' % (m,p) )
        
        finally:  
            self.ib.mailOnErr = m
            self.ib.pgOnErr   = p
               
        self.rc = self._procEmail()
        if self.rc == 0 :
            self.subj = 'Missing Vehicles in the BeerStore Customer File'
            self.text = self.dSrcLoad
            rc = self._notifyAppUsers()
            rc = 0
        
        elif self.rc == 2:
            self.log.info('No mismatches were found')
            rc = 0
        
        else:  
            self.log.error('Issue with DB rc = %s' % (self.rc))
            rc =  self.rc   
        
        return rc
          
def main(Args):
        a = BeerStTgtTBSLoad()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()         # Remove in UX 
    rc=  main(sys.argv)