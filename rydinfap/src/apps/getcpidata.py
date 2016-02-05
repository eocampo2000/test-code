'''
Created on Mar 13, 2015

@author: rtarik

EO: Incorporated in Ryder DW Python framework.

'''

__version__ = '20150313'


import utils.strutils   as su
import utils.fileutils  as fu
import procdata.procxml as px

from apps.infbaseapp   import _InfaBaseApp


url_usa = "http://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems"
url_can = "http://www.bankofcanada.ca/en/cpi.html"

# Mandatory to define self.cmdStep
# method _getNextRunDate is sensitive to schedule changes ! 
  
class GetCPIData(_InfaBaseApp):  
    exitOnError = True

    def __init__(self):
        super(GetCPIData,self).__init__()
        self.landDir    = 'SrcFiles/cpi'
        self.tgtDir     = 'SrcFiles'
        self.incFileSet = []    # Incoming Files. Contains full path name.
        self.incFiles   = []
        self.workFiles  = []    # Files that were moved to the working dir (ideally same than incSetFile). 
        self.trigFiles  = []    # Incoming Trigger File.
                       
        self.ts        =  su.getTimeSTamp()
        
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock   ,
                         'B' : self.getUSACPI ,
                         'C' : self.getCanCPI ,     
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
         
    def getUSACPI(self):
        tf = '%s/%s/%s' % (self.ib.shareDir,self.tgtDir,self.ib.usacpifile)
        rc = fu.get_url_data(url_usa, tf)
        self.log.debug('url_usa =  tf %s ' %  tf)
        if rc == 'SUCCESS' : 
            self.log.info('Created file %s ' % tf)
            return 0
        else :        
            self.log.error('Issue Creating %s - %s' % (tf,rc))
            return 1
        
    
    def getCanCPI(self):

        lf = '%s/%s/cancpi%s.html' % (self.ib.shareDir,self.landDir, self.ts)
        tf = '%s/%s/%s' % (self.ib.shareDir,self.tgtDir,self.ib.cancpifile)
        self.log.debug('landing file = %s target file = %s' % (lf,tf))
        rc = fu.get_url_data(url_can, lf)
        if rc == 'SUCCESS' : 
            self.log.info('Created temp file %s ' % lf)
            d = px.parseCanCPI(lf)
            if len(d) < 1 : 
                self.log.warn('No data was retrieved for Canada')
                rc = 2
            else:
                self.log.debug('data len %s from  %s' % (len(d),lf) )
                rc = fu.createFile(tf, d)      
                self.log.debug('Create file %s rc =%s ' % (tf,rc))
                          
        else :        
            self.log.error('%s ' % rc)
            rc = 1
        return rc
    
def main(Args):
    a = GetCPIData()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    import sys
    from setwinenv import setEnvVars   # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
