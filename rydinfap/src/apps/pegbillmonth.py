'''
Created on Jun 22, 2012

@author: eocampo

20130429 New Style class
20131114 Added None check for rs
20140911 Change isWorkDay to isWorkDayWarn
'''
__version__ = '20140911'

import sys

from apps.infbaseapp import _InfaBaseApp
from render.renderHTML import CommonHTML
import datastore.dbapp   as da
import procdata.procinfa as pi 

# mandatory to define self.cmdStep

class PegBillMonth(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(PegBillMonth,self).__init__()
        self.landDir    = 'SrcFiles/Repair'
        self.trigFiles  = []                  # Incoming Files
 
        self.pBillTot   = []     
        #self.ib.srcFile = 'pegbillmonth.txt'  # File that Informatica expects
        # For App Notification
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.isWorkDayWarn       ,
                         'C' : self.wPegPO              ,
                         'D' : self.getTrigFiles        ,
                         'E' : self.mvTrigFileToArchDir ,
                         'F' : self.wMonthPEGVehLoad    ,
                         'G' : self.wMonthBillLoad      ,   
                         'H' : self.wMonthToFis         ,
                         'N' : self.notifyUsers         ,
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
 
    
    # Process Result Counts for Verification
    # By Contract rprCmt > 0. 
    # Returns a list for rendering.
    def _procTotDS(self,rprCnt):
        myData = []
        tot_cost =0.0; tot_rec = 0

        myData.append([('Source',),('TotalCost',),('TotalRecords',)])
        for src,tcost,trec in rprCnt:
            #tot_cost = tot_cost+ float(tcost); tot_rec = tot_rec + float(trec)
            tot_cost+= float(tcost); tot_rec+= int(trec)
            myData.append([ ('%s' % src),('%s' % tcost), ('%s'% trec)])
        myData.append([('~TOTAL~'  ), ('%s' % tot_cost), ('%s' % tot_rec)])
   
        return myData
        #return ''.join(myData) 
 
    def _getHTML(self,data):
        
        self.log.debug('Incoming data ', data)
        o = CommonHTML(' ')
        html = []
        html.append(o.bldHtmlHeader() )
        html.append(o.bldHTMLTab(data))
        html.append('<P>regards, <P>EDW Team')
        html.append(o.bldEndTagHtml() )  
        return ''.join(html)
    
    def wPegPO(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_PO'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 :
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else :
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wMonthPEGVehLoad(self):
        self.ib.fld   = 'Repair'
        self.ib.wkf   = 'wkf_RPR_PEG_Billing_Monthly_PEG_Vehicle_Load'  
        self.ib.param = '$PMRootDir/Parameters/wkf_RPR_PEG_Vehicle_Load_workday_8.parm'
        rc = pi.runWkflParamWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def wMonthBillLoad(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Monthly_Load' 
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
       
    def wMonthToFis(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Monthly_to_FIS_PEG_Load'  
        self.ib.param = '$PMRootDir/Parameters/wkf_RPR_PEG_Monthly_to_FIS_Load_workday_8.parm'
        rc = pi.runWkflParamWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))   
        return rc
    
    def _getPegBillTot(self):  
        rprTot = self._getNZDS(da.selPegBillMthTotQry)

        if rprTot is not None and len(rprTot) > 0 : 
            data = self._procTotDS(rprTot)
            self.pBillTot = self._getHTML(data)
            self.log.debug('self.pBillTot = \n',self.pBillTot)
        else               : 
            self.log.error('Querying RO returned 0')
            return 1
        return 0
    
    def notifyUsers(self):
        self.files = []
        rc = 1
        print " =========== self.ib.pgOnErr = %s " % self.ib.pgOnErr
        try: 
            m = -1 ; p = -1
            m = eval(self.ib.mailOnErr) ; p = eval(self.ib.pgOnErr)
        except:
            self.log.error('Error with configuration value(s) self.ib.mailOnErr = %s self.ib.pgOnErr = %s' % (m,p) )
        
        finally:  
            self.ib.mailOnErr = m
            self.ib.pgOnErr   = p
               
        self.rc = self._getPegBillTot()
        if self.rc == 0 :
            self.subj = 'Peg Bill Monthly Job (%s workday) had completed successfully' %  self.ib.workday 
            self.text = self.pBillTot
            rc = self._notifyAppUsers('html')  
        else:
            self.subj = 'Error running Peg Bill job'
            self.text = 'Please see script logfile for details.'
            
        return rc
          
def main(Args):
        a = PegBillMonth()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()        
    rc=  main(sys.argv)