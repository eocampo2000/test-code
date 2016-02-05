'''
Created on Jul 11, 2012

@author: eocampo
'''
'''
Created on Jun 22, 2012

@author: eocampo
New Class Style
EO Change to MS SQL Native
EO Added Qry for Updates as per Jim's request.
EO 20140821 Changed CPM from MSSQL to NZ
EO 20150302 Removed notifyUsers boolean casting, to add in parent class.
'''

__version__ = '20150302'

import sys
from collections import Counter

from apps.infbaseapp import _InfaBaseApp   
import datastore.dbapp as da
import procdata.procinfa as pi 

# mandatory to define self.cmdStep

class CPMPartPrice(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(CPMPartPrice,self).__init__()
        self.dSrcLoad  = ''
        self.drunID    = []   # Run Id's 
        self.dupdEdID  = []   # Update Editions.
        self.drunIdCnt = {}
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock            ,
                         'B' : self.wCPMPartPriceList  ,                            
                         'N' : self.notifyUsers        ,
                        # 'Z' : self.chkPreq         ,
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
     

    def wCPMPartPriceList(self):
        self.ib.fld = 'Asset'
        self.ib.wkf = 'wkf_CPM_Part_Price_List' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
        
    def _getRunIdCnt(self):  
               
        rc = 0
        dupID = []
        self.ib.user = self.ib.ms_user; self.ib.pwd = self.ib.ms_pwd;  self.ib.dbserver = self.ib.ms_dbserver ; self.ib.db = self.ib.ms_db
        rs = self._getMSSQLNatDS(da.selCPMPartListCnt)
        
        if rs is None : return 5                     # Error with Connection.
        
        if len(rs) > 0 : 
            #self.drunID.append([('RunID',),('Load_SRC',),('TotalRows',)])
            for rId,ldSrc,cnt in rs:
                self.drunID.append([ ('%s' % rId),('%s' % ldSrc), ('%s'% cnt)])     
                dupID.append('%d' % rId)

            if len(dupID) > 1 :
                self.log.debug('dupID ', dupID)
                runIdCnt = Counter(dupID)
                self.log.debug( "type runIdCnt " , type(runIdCnt), " val", runIdCnt)
                for k,v in runIdCnt.items():
                    if v > 1 : 
                        self.drunIdCnt[k] = v           
                        rc = 1           
        else               : 
            self.log.warn('tblEDWVendorDetails: No results at this time. Qry returned 0')
            rc = 2 # . Should not email as they are no results.
       
        return rc
    
    def _getUpdEdition(self):
        
        rc = 0
        rs = self._getNZDS(da.selCMPUpdEdition)

        if rs is None : return 5  
        
        if len(rs) > 0 : 
        
            #self.dupdEdID.append([('vwEdition',),('Country_cd',),('Count',)])               # Contains header. !
            for vEd,ctrCd,cnt in rs:
                self.dupdEdID.append([ ('%s' % vEd),('%s' % ctrCd), ('%s'% cnt)])  
    
        else               : 
            self.log.warn('tblEDWCPM:No results at this time. Qry returned 0')
            rc = 2 # . Should not email as they are no results.
        
        return rc
    
    # This method will build email subj/text based on results.
    # SE sets  self.dSrcLoad 
    def _procEmail(self):
        rc = 0
        
        r = self._getUpdEdition()
        if r == 0 :
            self.dSrcLoad += '\ntblEDWCPM\nvwEdit\tCountry_cd\tCount\n'
            for vEd,ctrCd,cnt in self.dupdEdID:
                self.dSrcLoad += '%s\t%s\t\t%s\n' % (vEd,ctrCd, cnt) 

        else : rc = r
        
        r = self._getRunIdCnt()
        if r == 0 :
            self.dSrcLoad += '\ntblEDWVendorDetails\nRunID\t\tLoad_SRC\tTotalRows\n'
            for rId,ldSrc,cnt in self.drunID:
                self.dSrcLoad += '%s\t%s\t%s\n' % (rId,ldSrc,cnt)                
               
        elif r == 5: rc = 5
                
        return rc
    
    def notifyUsers(self):
        self.files = []
        rc = 1
               
        self.rc = self._procEmail()
        if self.rc == 0 :
            self.subj = 'Asset.CPM_Part_Price_list : Job had completed successfully'
            self.text = self.dSrcLoad
            rc = self._notifyAppUsers('plain')
            rc = 0
        
        elif self.rc == 2:
            self.subj = 'Asset.CPM_Part_Price_list : No rows found'
            self.text = 'No rows found on table'
            rc = 0
        
        elif self.rc == 1:

            self.subj = "Asset.CPM_Part_Price_list : Duplicates Run IDs where found %s " %  str(self.drunIdCnt.keys())
            self.text = self.dSrcLoad
            rc = self._notifyAppUsers('plain')
        
        self.log.debug('SUBJ %s \t MSG %s' % (self.subj, self.text))    
        
        return rc
          
def main(Args):
        a = CPMPartPrice()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()         # Remove in UX 
    rc=  main(sys.argv)