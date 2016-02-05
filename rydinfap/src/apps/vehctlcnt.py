'''
Created on Feb 13 2014

@author: eocampo

This code will split Vehicke count Control File.

'''
__version__ = '20140213'

import sys, os

import utils.filetransf as ft
import utils.fileutils  as fu
import utils.strutils   as su
from apps.infbaseapp  import _InfaBaseApp
from infbaseutil import _Vehicle
# Mandatory to define self.cmdStep
   
class VehCtlCnt(_InfaBaseApp,_Vehicle):  
    
    exitOnError = True

    def __init__(self):
        #_InfaBaseApp.__init__(self)
        super(VehCtlCnt,self).__init__()
        self.landDir    = 'SrcFiles/vehicle'
       
        self.srcFile     = ('PO099D30.cnt')   # File that Informatica expects. Alphabetical.
        self.ib.fileName = r"'P.PO099D30.WHOUSE.COUNTS'"
        #self.ib.fileName = r"P.PO099D30.WHOUSE.COUNTS"
        # Allowable commands for this application. Make sure to Set 
        self.cmdStep = { 'A' : self.getLock         ,
                         'B' : self.getFtpFiles     ,   
                         'C' : self.getIncFiles     , 
                         'D' : self.cpFileToWorkDir ,
                         'E' : self.archFilesTS     ,
                         'F' : self.splitMainCntFile,                    
                        }
        self.ts        =  su.getTimeSTamp()  
        # FTP is expecting the following env variables, which should not be in a config file.   
        os.environ['FILE'     ] =  (r"\'P.PO099D30.WHOUSE.COUNTS\'")     
        #os.environ['FILE'     ] =  (r"P.PO099D30.WHOUSE.COUNTS")    
        #os.environ['FILE'     ] =  (r"house.cnt")   
        os.environ['RXFILE'   ] =  ('None')    # 
        
        # Infa Environmental variables/
        self.infaEnvVar   = {
                'INFA_SHARE'       : 'self.ib.shareDir'   ,  
                'INFA_APP_CFG'     : 'self.ib.cfgDir'     ,   
                'INFA_APP_LCK'     : 'self.ib.lckDir'     ,                   
               }
    
    def getFtpFiles(self):
        return ft.getn('Vehicle',self.log)
    
    # Wrapper Method
#    def copyFilesWorkDir(self):
#        for i in range (len(self.srcFile)):
#            self.incFiles.append('%s' % self.incFileSet[i][0])
#         
#        return self.cpSrcToTgtFiles()
    def cpFileToWorkDir(self):
        fnp = '%s' % self.incFiles[0]
        t   = '%s/%s' % (self.ib.workDir,self.srcFile)
        rc  = fu.copyFile(fnp, t)
        return rc     

    def archFilesTS(self): return self.archGenFiles(self.incFiles, self.ts,True)
     
#    def 
#    splitMainCtrlFile(self, fnp, sd)   
        
def main(Args):
    a = VehCtlCnt()
    rc = a.main(Args)
    return rc 

if __name__ == '__main__':   
    from setwinenv import setEnvVars  # Remove in UX 
    setEnvVars()                       # Remove in UX 
    rc=  main(sys.argv)
    