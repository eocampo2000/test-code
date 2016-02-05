'''
Created on Aug 20, 2014

@author: eocampo

This program will load location dimension data. Should ran 5th workday at 1:00pm . MF predecessor completes on the 3rd wkday. 
MF DOES not provide hand shaking mechanism.
EO 20150903 Added workflow wkf_employee_dimension_monthly as per Srinath email on Tue 9/1/2015 3:35 PM
              

'''
__version__ = '20150907'

import sys
import os
import utils.fileutils    as fu
import utils.strutils     as su
import procdata.procinfa  as pi 
import procjobs.procsched as psc
import utils.filetransf   as ft

from apps.infbaseapp       import _InfaBaseApp

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
RUN_PER_DAY = 1  # Daily runs.
DP_LEN      = len('YYYYMM')  
   
# Schedules
SCH_FREQ = 'Mthly'
sch = ()
cur_dayr   = su.getTodayDtStr('%Y%m')

class LocationDim(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):
        super(LocationDim,self).__init__()
        self.landDir    = 'SrcFiles/employee'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        self.trigFiles  = []    # Incoming Trigger File.
        
        self.srcFile     = ('hp400jnm_sap_dwext.dat',)   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.HP400JNM.SAP.DW.EXTRACT'"
        
        self.fileDate   = ''          
        self.FILE_SET_LEN = 1   
        
        self.ts        =  su.getTimeSTamp()
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock            ,
                         'B' : self.isWorkDayWarn      ,
                         'C' : self.chkNextRunFlg      ,
                         'D' : self.rWkfLocCbuMthly    ,  
                         'E' : self.rWkfLocGeoMthly    ,
                         'F' : self.rWkfLocAreaMthly   ,
                         'G' : self.rWkfLocRfpMthly    ,
                         'H' : self.rWkfLocAdminMthly  ,
                         'I' : self.rWkfLocWalkerMthly ,
                         'J' : self.rWkfLocTeamMthly   ,
                         'K' : self.rWkfEmpDimMthly    ,
                         'L' : self.procLocationDim    ,  
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
        
        # Flags to run wkl
        self.loc_cbu_mthly_flg    = False
        self.loc_geo_mthly_flg    = False
        self.loc_area_mthly_flg   = False
        self.loc_rfp_mthly_flg    = False
        self.loc_admin_mthly_flg  = False
        self.loc_walker_mthly_flg = False
        self.loc_team_mthly_flg   = False
        self.emp_dim_mthly_flg    = False
        
        # FTP is expecting the following env variables, which should not be in a config file.
        #UX cp Cortera_Trx_Extract_20130429.txt "'P.PO230D15.VPODATA(0)'"
        os.environ['FILE'     ] =  (r"\'P.HP400JNM.SAP.DW.EXTRACT\'")   
        os.environ['RXFILE'   ] =  ('None')    # 
        
    # Setter Methods to run wkfs
    def rWkfLocCbuMthly(self)   : self.loc_cbu_mthly_flg    = True; return 0
    def rWkfLocGeoMthly(self)   : self.loc_geo_mthly_flg    = True; return 0
    def rWkfLocAreaMthly(self)  : self.loc_area_mthly_flg   = True; return 0
    def rWkfLocRfpMthly(self)   : self.loc_rfp_mthly_flg    = True; return 0
    def rWkfLocAdminMthly(self) : self.loc_admin_mthly_flg  = True; return 0
    def rWkfLocWalkerMthly(self): self.loc_walker_mthly_flg = True; return 0
    def rWkfLocTeamMthly(self)  : self.loc_team_mthly_flg   = True; return 0
    def rWkfEmpDimMthly(self)   : self.emp_dim_mthly_flg    = True; return 0
            
    def getFtpFiles(self):
        return ft.getn('Employee Dim',self.log)

    def _wkf_loc_cbu_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_cbu_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def _wkf_loc_geo_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_geo_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def _wkf_loc_area_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_area_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def _wkf_loc_rfp_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_rfp_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def _wkf_loc_admin_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_admin_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def _wkf_loc_walker_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_walker_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc

    def _wkf_loc_team_mthly(self):
        self.ib.fld = 'Location'
        self.ib.wkf = 'wkf_location_team_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc
       
    def _wkf_emp_dim_mthly(self):
        self.ib.fld = 'Employee'
        self.ib.wkf = 'wkf_employee_dimension_monthly'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))  
        return rc     
            
    def procLocationDim(self):
        ctlFile = '%s/%s.ctl' % (self.ib.ctlDir,self.appName)                
        self.log.debug('self.checkNextRunFlg is %s' %  self.checkNextRunFlg)
        prev_dayr = self._getCtlFile()
        
        if self.checkNextRunFlg is True:
            
            if prev_dayr is None or prev_dayr.strip() == '': 
                self.log.error("Could not find control file or No Data")
                return -1
            
            rc = psc.getNextRunDate(prev_dayr, cur_dayr, SCH_FREQ, self.log,sch)
            if rc != 0 : 
                self.log.error("self._chkNextRun rc = %s" % rc)
                return rc
 
        # Run workflows
        if self.loc_cbu_mthly_flg: 
            if self._wkf_loc_cbu_mthly()    != 0 : return 1
        if self.loc_geo_mthly_flg  :
            if self._wkf_loc_geo_mthly()    != 0 : return 1
        if self.loc_area_mthly_flg  :    
            if self._wkf_loc_area_mthly()   != 0 : return 1
        if self.loc_rfp_mthly_flg :    
            if self._wkf_loc_rfp_mthly()    != 0 : return 1
        if self.loc_admin_mthly_flg :    
            if self._wkf_loc_admin_mthly()  != 0 : return 1
        if self.loc_walker_mthly_flg:    
            if self._wkf_loc_walker_mthly() != 0 : return 1
        if self.loc_team_mthly_flg  :    
            if self._wkf_loc_team_mthly()   != 0 : return 1    
        if self.emp_dim_mthly_flg   :    
            if self.getFtpFiles()          != 0 : return 1
            if self.getIncFiles()      != 0 : return 1
            if self.cpSrcToTgtFiles()     != 0 : return 1
            rc = self.archFiles()
            self.log.info("archFilesTS() rc = %s" % rc )
            if self._wkf_emp_dim_mthly()   != 0 : return 1
     
        # Loading Staging Succeeded. Update the control file.
        rc = fu.updFile(ctlFile,cur_dayr)               
        if rc == 0 :
            if self.checkNextRunFlg: self.log.info('Updated Cur Load Date from %s to  %s , Control File %s'     % (prev_dayr,cur_dayr, ctlFile))
            else                   : self.log.info('Overwriting Cur Load Date from %s to  %s , Control File %s' % (prev_dayr,cur_dayr, ctlFile))
        else       : 
            self.log.error('Could not Update Load Date %s, Control File %s rc = %s' % (cur_dayr,ctlFile,rc))
        
        return rc 
        
def main(Args):
    a = LocationDim()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)

