'''
Created on Apr 29, 2013

@author: eocampo
20130429 New Class Style
20131114 Added None check for rs
20140911 Change isWorkDay to isWorkDayWarn
'''
__version__ = '20140911'

import sys

import datastore.dbapp   as da
import utils.fileutils   as fu
import procdata.procinfa as pi 

from pegbillmonth import PegBillMonth
# mandatory to define self.cmdStep

class PegBillTotAccrual(PegBillMonth):  
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
        self.cmdStep = { 'A' : self.getLock             ,
                         'B' : self.isWorkDayWarn       ,
                         'C' : self.wPegPO              ,
                         'D' : self.getTrigFiles        ,
                         'E' : self.mvTrigFileToArchDir ,
                         'F' : self.wMonthPEGVehLoad    ,
                         'G' : self.wMonthBillLoad      ,   
                         'H' : self.sendTotForApproval  ,
                         'I' : self.waitForApproval     ,
                         'J' : self.wMonthToFis         ,
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
    
    # Process Result Into CSV file , to send attachment.
    # By Contract rprDet. 
    def _procDetDS(self,rprDet):
        myData = []
        hdr = "peg_id|audit_dt|audit_tm|cost_period|lessee_no|customer_no|customer_name|veh_no|odom|" \
              "dom_loc|dom_loc_name|iss_loc|iss_loc_name|source|po_no|ro_no|repair_dt|ro_type|rpr_reason|" \
              "rpr_rsn_desc|exclusion_type|line_item|gl_acct_no|part_ata_desc|part_system|part_assembly|" \
              "part_component|part_no|part_desc|ata_desc|system|assembly|component|action|action_desc|posn|" \
              "part_qty|part_amt|labor_hours|labor_amt|overhead_cost|outside_cost|other_cost|oil_cost|" \
              "tire_cost|total_cost|proc_dt|proc_time|proc_fg|etl_load_dt\n"
        
        myData.append(hdr)
        i=0
        for rs in rprDet:
            i+=1
            myData.append('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|' %  rs[0:49])
            myData.append('%s\n' % rs[49])
            
        self.log.info('Total Records %d' % i)
        return  ''.join(myData) 
    
    
    def sendTotForApproval(self):
        pegBill = 'C:\\apps\\pegbillDet.csv'
        print "In send counts(*)"
        rc = self._getPegBillTot()
        if rc != 0 : return rc
        
        rprDet = self._getNZDS(da.selPegBillMthDeanDetQry)
        if rprDet is not None and len(rprDet) > 0 : detDS  = self._procDetDS(rprDet)
        else               : 
            self.log.error('Querying RO returned 0')
            return 1
       
        rv = fu.createFile(pegBill,  detDS)
        if rv != 0 : self.log.error('Could not create file %s' % pegBill)
        else       : self.log.info( 'Created file %s' % pegBill)    
      
        return 0
    
    def waitForApproval(self):
        self.ib.fileName = 'deanfoodapprov'
        self.log.info('Waiting for approval. Approval File  %s' % (self.ib.fileName))
        rc = self.checkApprovFile()
        return rc
           
    def wMonthPEGVehLoad(self):
        self.ib.fld   = 'Repair'
        self.ib.wkf   = 'wkf_RPR_PEG_Billing_Monthly_PEG_Vehicle_Load'  
        self.ib.param = '$PMRootDir/Parameters/wkf_RPR_PEG_Vehicle_Load_workday_3.parm'
        rc = pi.runWkflParamWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    def wMonthToFis(self):
        self.ib.fld = 'Repair'
        self.ib.wkf = 'wkf_RPR_PEG_Billing_Monthly_to_FIS_PEG_Load'  
        self.ib.param = '$PMRootDir/Parameters/wkf_RPR_PEG_Monthly_to_FIS_Load_workday_3.parm'
        rc = pi.runWkflParamWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))   
        return rc    

def main(Args):
        a = PegBillTotAccrual()
        rc = a.main(Args)
        return rc 

#ABCDEFGH
if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()         # Remove in UX 
    rc=  main(sys.argv)