'''
Created on April 5, 2013

@author: eocampo

 For SAS smoothing process.
 04/22/2013 Added Infa wklf and SAS script names.
            This run will not execute the first Informatica wkf
  Local Server
  infa     15494  1872  0 18:08 pts/0    00:00:00 /bin/sh ./bdl_hist_load.sh
  infa     15506 15494  0 18:08 pts/0    00:00:00 /usr/local/bin/python2.7 /infa/scripts/pc910/lib/adm_script/apps/infasassmooth.pyc ACEF
  infa     15508 15506  0 18:08 pts/1    00:00:00 /usr/bin/ssh -q -oRSAAuthentication=no -o PubkeyAuthentication=no -l sasetl sscsas02

  Remote Server
  sasetl 19196 19183  0 18:08:20 pts/3     0:00 /bin/sh /sas/usr/bin/odom_smooth_in_test.sh
  sasetl 19201 19196  0 18:08:20 pts/3     3:10 /sas/sas/software/SASFoundation/9.2/sasexe/sas -metaautoresources SASApp -noobjectserver -noterminal -autoexec
  
  
 09/13/2013 Added REmote Script and WKFLows.
'''
__version__ = '20130913'

import sys

from apps.infbaseapp import _InfaBaseApp

import cmd.oscmd         as oc 
import procdata.procinfa as pi 

# mandatory to define self.cmdStep

class InfaSASSmooth(_InfaBaseApp):  
    exitOnError = True
    
    def __init__(self):

        super(InfaSASSmooth,self).__init__()
        self.landDir    = 'SrcFiles'
        self.trigFiles  = []                  # Incoming Files
 
        self.verPIDFlg  = False

        #self.ib.srcFile = 'pegbillmonth.txt'  # File that Informatica expects
        # For App Notification
        
        # Allowable commands for this application
        self.cmdStep = { 'A' : self.getLock            ,
                         'B' : self.vPidFlg            ,
                         'C' : self.bsloadTest         ,
                         'D' : self.wSrvCalls          ,
                         'E' : self.wPostProc          ,
                         'F' : self.wBDLHist           ,
                         'G' : self.bsOdomSmooth       ,
                         'H' : self.wBDLFutFlt         ,
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
 
    def _setDataDir(self):
        pass
       
    def vPidFlg(self):
        self.verPIDFlg = True
        return 0

    def bsloadTest(self):
        bfn  = 'run_load_in_test'
        cmd  = '%s/%s.sh; echo $?' % (self.ib.rpath,bfn)
        rcmd = oc.CmdHPUXDriver(self.log,self.ib)
        rmsg = rcmd.execRemCmd(cmd, 'HP-UX')
        rc   = rcmd.parseRemUxResp(0,rmsg)
        if rc < 0 : 
            self.log.error('Running rcmd %s rc = %s' % (cmd,rc))
            return rc

        if self.verPIDFlg is True:

            cmd  = 'cat %s/%s.pid' % (self.ib.rpathr,bfn)           
            rmsg = rcmd.execRemCmd(cmd, 'HP-UX')
            rc   = rcmd.parseRemUxResp(0,rmsg) 
            self.log.debug("rc = %s " % rc)
            if 0 <= rc <= 4 : rc = 0        # As per Charlie, since 1 to 4 are warnings
 
        return rc  

    def wSrvCalls(self):
        self.ib.fld = 'SMO'
        self.ib.wkf = 'wkf_servicecalls'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc

    def wPostProc(self):
        self.ib.fld = 'BDL'
        self.ib.wkf = 'wkf_post_process'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc


    def wBDLHist(self):
        self.ib.fld = 'BDL'
        self.ib.wkf = 'wkf_BDL_Load_History'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        return rc
    
    def bsOdomSmooth(self):
        bfn  = 'odom_smooth_in_test'
        cmd  = '%s/%s.sh; echo $?' % (self.ib.rpath,bfn)
        rcmd = oc.CmdHPUXDriver(self.log,self.ib)
        rmsg = rcmd.execRemCmd(cmd, 'HP-UX')
        rc   = rcmd.parseRemUxResp(0,rmsg)
        if rc < 0 : 
            self.log.error('Running rcmd %s rc = %s' % (cmd,rc))
            return rc

        if self.verPIDFlg is True:

            #p   = rcmd.verifyPID(rmsg)
            #if rc is None or rc < 2:
            #   self.log.error('Invalid Pid %s' % rc)
            #   return -2

            cmd  = 'cat %s/%s.pid' % (self.ib.rpathr,bfn)           
            rmsg = rcmd.execRemCmd(cmd, 'HP-UX')
            rc   = rcmd.parseRemUxResp(0,rmsg) 
            self.log.debug("rc = %s " % rc)
            if 0 <= rc <= 4 : rc = 0        # As per Charlie, since 1 to 4 are warnings
 
        return rc
       
    def wBDLFutFlt(self):
        self.ib.fld = 'BDL'
        self.ib.wkf = 'wkf_BDL_Future_Fleet'
        rc = pi.runWkflWait(self.ib,self.log)
        if rc != 0 : 
            self.log.error('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))
        else : 
            self.log.info('Running  %s.%s rc = %s' % (self.ib.fld,self.ib.wkf,rc))   
        return rc  

def main(Args):
        a = InfaSASSmooth()
        rc = a.main(Args)
        return rc 

if __name__ == '__main__':   
    rc=  main(sys.argv)
