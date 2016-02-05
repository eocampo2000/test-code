'''
Created on Feb 17, 2013

@author: eocampo
MRC ETL process

'''
__version__ = '20130301'

import sys, os

from apps.infbaseapp    import _InfaBaseApp
from common.loghdl import getLogHandler
import utils.fileutils      as fu
import utils.strutils       as su
import procjobs.procjobxml  as pxml
import procjobs.procjobload as pjl
import common.simpmail      as sm
import bean.jobbean         as jb

mpid = os.getpid()
# mandatory to define self.cmdStep
# EO TODO if the program runs a set of files, row count will only be verified on the latest load date.
#         need to add the capability to check in very loaded file !
class MRCIncLoad(_InfaBaseApp):  
    
    exitOnError = True   # default.
    grpbl       = -1
    logDir      = ''
    forceRec    = False
    wkday       = 0
    
    def __init__(self):
        
        #_InfaBaseApp.__init__(self)
        super(MRCIncLoad,self).__init__()
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getTodayWorkDay   ,
                         'C' : self.procJobXml      ,
                         'E' : self.dispExePlan     ,
                         'F' : self.forceRecFlg     ,
                         'P' : self.procMRCJob      ,
                         'R' : self.recMRCJob       ,
                         #'G' : self.postChkFlg      ,                        
                        }
       
        # Infa Environmental variables/
        self.infaEnvVar   = {
                'PMCMD'            : 'mg.pmcmd'           ,
                'INFA_USER'        : 'self.ib.rep_user'   ,
                'INFA_PWD'         : 'self.ib.rep_pwd'    ,
                'DOMAIN'           : 'self.ib.dom_name'   ,
                'INT_SERV'         : 'self.ib.IS'         , 
                'INFA_SHARE'       : 'self.ib.shareDir'   ,  
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,   
                'INFA_APP_CTL'     : 'self.ib.ctlDir'     , 
                'INFA_APP_REC'     : 'self.ib.recDir'     , 
                'LOG_DIR'          : 'self.ib.logDir'     ,
               }


    def _configBean(self):
        cb = jb.ConfigBean()
        cb.pid     = mpid
        cb.appName = self.appName
        cb.confDir = self.ib.cfgDir
        cb.logDir  = self.ib.logDir
        cb.recDir  = self.ib.recDir
        return cb
       
    def getTodayWorkDay(self):
        self.wkday = self.getWorkDay()
        self.log.info("Today Workday is  %d " % self.wkday )
        if self.wkday < 1 or self.wkday > 25 : return 1
        return 0
    
    # THis jobs parses and loads the XML file containing the load plan.                            
    def procJobXml(self):
        rc = -1
        fn = '%s/%s' % (self.ib.cfgDir, self.ib.xmlPlanFile)
        self.log.info("Reading %s ", fn)

        g  = pxml.ProcJobXML(fn,self.log)
        if g is None : return rc
        rc =  g.chkXMLFile() 
        if rc != 0   : return rc
       
        self.grpbl = g.parseAll()
        if len(self.grpbl) < 1:
            self.log.error('Nothing to process!. Check Contents of %s' % fn) 
            return -2
                
        return rc
    # This method allows the user to view a plan file w/o a need to run.
    # Using 0 will display the actual normal run plan. Otherwise will use the given PID.
    def dispExePlan(self):
        cb = self._configBean()
        self.log.debug('Config Bean = %s' % cb)
        
        pid = su.toInt(self.recPid)
        if pid is None : 
            self.error('Invalid PID %s' % pid)
            return -1
     
        if  pid == 0: 
            fxml = '%s/%s.xml' % (cb.confDir,cb.appName)
            self.log.info('Displaying Main Execution plan %s' % fxml) 
        else :
            fxml = '%s/%s/%s%s.xml' % (cb.recDir,self.recPid,cb.appName,self.recPid)
            self.log.info('Displaying Recovery Execution plan %s' % fxml) 
   
        pj = pjl.ProcessJobs(cb,self.log)
        
        grpbl = pj.loadExecPlan(fxml)
        if len(grpbl) < 1 : 
            self.log.error('Nothing to process!. Check Contents of %s' % fxml) 
            return 1

        pj.printExecPlan(grpbl)
        
        return 0
    
       
    def procMRCJob(self):
        cb = self._configBean()
        self.log.debug('Config Bean = %s' % cb)
        fxml = '%s/%s.xml' % (cb.confDir,cb.appName)
        self.log.info('Execute Plan file %s' % fxml)
 
        pj = pjl.ProcessJobs(cb,self.log)
        grpbl = pj.loadExecPlan(fxml)
        if len(grpbl) < 1 : return 1
                
        rc = pj.mainProc(fxml,grpbl)
        return rc
      

    # When recovering user will be prompt to accept, before recover can proceed.
    def forceRecFlg(self):
        self.forceRec = True
        return 0
    
    # Recover job
    def recMRCJob(self):
        cb = self._configBean()
        self.log.debug('Config Bean = %s' % cb)
        
        pid = su.toInt(self.recPid)
        if  pid is None or pid < 2: 
            self.log.error('Recovery PID needs to be numeric and > 1. Current %s' % self.recPid) 
            return 2
        
        fxml = '%s/%s/%s%s.xml' % (cb.recDir,self.recPid,cb.appName,self.recPid)
        self.log.info('Execute Plan file %s' % fxml)
 
        pj = pjl.ProcessJobs(cb,self.log)
        
        if not fu.fileExists(fxml):
            rc = pj.createRecXMLFile(fxml,self.recPid)
            if rc != 0 :
                self.log.error("Could not create recovery file %s" % fxml)
                return 1 
        else : self.log.info('File %s exits. Delete if you want to recreate !')
        
        grpbl = pj.loadExecPlan(fxml)
        if len(grpbl) < 1 : 
            self.log.error('Nothing to process!. Check Contents of %s' % fxml) 
            return 1
        
        # Give the user the option to review and change the Execution plan file for recovery purposes.
        if not self.forceRec:
            pj.printExecPlan(grpbl)
            r = su.getResponse('Please Enter Yes to Continue, any other key to exit.\n','Yes')
            if r is False: return -1
            # Read again, since file could be modified by the user !!
            grpbl = pj.loadExecPlan(fxml)
            if len(grpbl) < 1 : 
                self.log.error('Nothing to process!. Check Contents of %s' % fxml) 
                return 1           
            self.log.info('Reloaded %s ' % fxml) 
            
        rc = pj.mainProc(fxml,grpbl)
        return rc

    
    def _postCheck(self):
        rc = 0
                
        self.ib.schDayOff = 0   # Overwrite from config, since we are using the file name to seed the procdate.
                                # For files created on sunday is filename - 1 day = procdate.
        r = self.getDlyCnt()     
        
        lineCnt = self.srcCount['Ap_scuaphis.txt']
        if r == 0 and self.RowCnt > 0 :
            r = lineCnt - self.RowCnt                
            self.log.info('File Date = %s File LineCnt = %d\tDB RowCnt = %d rc = %s' % (self.fileDate,lineCnt,self.RowCnt,rc))
            if r != 0:
                self.log.error('Load Date = %s does not match Lines - Rows = %d ' % (self.fileDate,lineCnt - self.RowCnt))
                rc = 2
            else : self.log.info('Rec Number from src file matches Rec Number loaded to DB')

        else:    
            self.log.error('Error validating Records - DlyCnt: Rows loaded to DB %s' % self.RowCnt)
            rc = 3

        return rc 
    
    def _setDataDir(self):
        pass
    
    def setArgs(self,Argv):
        if len(Argv) != 3 :
            self.log.error("USAGE : <%s> fx [runSeq] Incorrect Number of arguments (%d)" % (Argv[0], len(Argv)))
            return 1  
        
        self.runSeq = Argv[1] 
        self.recPid = Argv[2]     # Recovery PID or 0 for new runs.

        return 0
    
def main(Args):
    a = MRCIncLoad()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                      # Remove in UX 
    rc=  main(sys.argv)
    
