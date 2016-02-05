#-------------------------------------------------------------------------------
# Created on Jan 4, 2012
#
# @author: eocampo
#
# Works for version 8.X, 9.X
# Added deleteConnection (for upgrade)
# 20150327 Added cmd to support to startTask and wkf from a task. 
#-------------------------------------------------------------------------------
__version__ = '20150327'

import proc.process   as p
import mainglob  as mg

# This class will run pmcmd Commands
class PMCmd:
    
    # ib infaBean    
    def __init__(self, ib, log):
        self.ib     = ib
        self.logger = log

    # Main execution driver
    def _execCmd(self,c):
        cmd = '%s %s' % (mg.pmcmd, c)
        self.logger.debug("cmd = %s" % cmd)
        return p.runSync(cmd,self.logger)
       
    def getSchedWflIS(self):
        c = 'getservicedetails -sv %s -d %s -u %s -p %s -scheduled'  %  ( self.ib.IS, 
                                                                          self.ib.dom_name,
                                                                          self.ib.rep_user,
                                                                          self.ib.rep_pwd,
                                                                         )
        rc,rm=self._execCmd(c)
        self.logger.debug('rc=%s \nrm=%s' % (rc,rm))    
        return rc,rm    

    def getWflDetails(self,f,w):
        c = 'getworkflowdetails -sv %s -d %s -u %s -p %s -f %s %s '  %  ( self.ib.IS, 
                                                                          self.ib.dom_name,
                                                                          self.ib.rep_user,
                                                                          self.ib.rep_pwd,
                                                                          f,
                                                                          w,
                                                                         )
        rc,rm=self._execCmd(c)
        self.logger.debug('rc=%s \nrm=%s' % (rc,rm))    
        return rc,rm  
    
    # pmcmd  unscheduleworkflow -sv is_development -d Domain_dev  -u $USER -p $PASSWD -f $folder $wflow 
    # w  Workflow
    # f  Folder 
    def schedWFl(self,f,w):
        c = 'scheduleworkflow  -sv %s  -d %s -u %s -p %s -f %s %s'  %  ( self.ib.IS, 
                                                                         self.ib.dom_name,
                                                                         self.ib.rep_user,
                                                                         self.ib.rep_pwd,
                                                                           f,
                                                                           w,)
        rc,rm=self._execCmd(c)
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
     
    # pmcmd  unscheduleworkflow -sv is_development -d Domain_dev  -u $USER -p $PASSWD -f $folder $wflow 
    # w  Workflow
    # f  Folder
    def unSchedWFl(self,f,w):
        
        c = 'unscheduleworkflow  -sv %s  -d %s -u %s -p %s -f %s %s'  %  ( self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           f,
                                                                           w )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
   
    def startTaskWait(self,f,w, t):
        c = 'starttask -sv %s  -d %s -u %s -p %s -f %s -wait -w %s %s'  %  ( self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           f,
                                                                           w,
                                                                           t )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
    
    def startWkflWait(self,f,w):
        c = 'startworkflow -sv %s  -d %s -u %s -p %s -f %s -wait %s'  %  ( self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           f,
                                                                           w )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
   
    def startWkflFromTaskWait(self,f,w,s):
        c = 'startworkflow -sv %s  -d %s -u %s -p %s -f %s -startfrom %s -wait %s'  %  ( self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           f,
                                                                           s,
                                                                           w )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s  s=%s w=%s rc=%s \nrm=%s' % (f,s,w,rc,rm))
        return rc,rm 
    
    def startWkflParamWait(self,p,f,w):
        c = "startworkflow -sv %s  -d %s -u %s -p %s -paramfile '%s' -f %s -wait %s"  %  (self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           p,
                                                                           f,
                                                                           w )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
  
    # pmcmd  aborttask -sv is_development -d Domain_dev  -u $USER -p $PASSWD -f $folder $wflow 
    def abortWkflWait(self,f,w):
        c = 'abortworkflow -sv %s  -d %s -u %s -p %s -f %s -wait %s'  %  ( self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           f,
                                                                           w )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
    
    def abortTaskWait(self,f,w):
        c = 'aborttask -sv %s  -d %s -u %s -p %s -f %s -wait %s'  %  ( self.ib.IS, 
                                                                           self.ib.dom_name,
                                                                           self.ib.rep_user,
                                                                           self.ib.rep_pwd,
                                                                           f,
                                                                           w )
        rc,rm=self._execCmd(c)  
        self.logger.debug('f=%s w=%s rc=%s \nrm=%s' % (f,w,rc,rm))
        return rc,rm
 
    # pmcmd getsessionstatistics -sv $INT_SERV -d $DOMAIN -u $INFA_USER -p $INFA_PWD -f SAS -w wkf_STG_FIS_DLY s_stg_CTRL_TBL_UPD_Status.
    def getTaskDet(self,f,w):
        pass
    
# This class will run pmrep Commands
class PMRep:
    
    def __init__(self, ib, log):
        self.ib     = ib
        self.logger = log
    #  -r Production -h sscinfo1 -o 6005 -n eocampo -x oceanprod
    def repConn(self):
        # c = 'connect -r %s -d %s -n %s -x %s' %  (self.ib.rep_name,
        #                                           self.ib.dom_name,
        #                                           self.ib.rep_user,
        #                                           self.ib.rep_pwd)

        c = 'connect -r %s -h %s -o %s -n %s -x %s' %  (self.ib.rep_name,
                                                  self.ib.dom_host,
                                                  self.ib.dom_port,
                                                  self.ib.rep_user,
                                                  self.ib.rep_pwd)

                            



        rc,rm=self._execCmd(c)
        self.logger.debug('rc=%s \nrm=%s' % (rc,rm))
        return rc,rm

    # Make sure you have a valid connection before invoking this method.
    # f backup file, complete path
    # o overwrite flag
    
    def repBkup(self,f):
        c = 'backup -o %s -f ' %  (f)
        rc,rm=self._execCmd(c)
        self.logger.debug('f=%s rc=%s \nrm=%s' % (f,rc,rm))

        return rc,rm
    
    # Make sure you have a valid connection before invoking this method.
    # f backup file, complete path
    # o overwrite flag  
    
    def repDelConn(self, cn):
        c = 'deleteconnection -n %s -f ' % (cn)
        rc,rm=self._execCmd(c)
        self.logger.debug('f=%s rc=%s \nrm=%s' % (cn,rc,rm))

        return rc,rm
        
        
    def _execCmd(self,c):
        cmd = '%s %s' % (mg.pmrep, c)
        self.logger.debug("cmd = %s" % cmd)
        return p.runSync(cmd,self.logger)
        
    
# This class will run infacmd Commands
class InfaCmd:
    
    def __init__(self, dom, log):
        self.dom    = dom
        self.logger = log
        
    def pingDomain(self):
        c = 'ping -dn %s -dg %s:%s -nn %s -re 60'  % ( self.dom.dom_name, 
                                                       self.dom.dom_host,
                                                       self.dom.dom_port,
                                                       self.dom.dom_node ,
                                                     )
        rc,rm=self._execCmd(c)
        
        return rc,rm
    
    # IS integration service name
    def pingIntService(self):
        c = 'ping -dn %s -dg %s:%s -sn %s  -re 60' % ( self.dom.dom_name, 
                                                       self.dom.dom_host,
                                                       self.dom.dom_port, 
                                                       self.dom.IS
                                                      )
        rc,rm=self._execCmd(c)
        
        return rc,rm

    def _execCmd(self,c):
        cmd = '%s %s' % (mg.infacmd  , c)
        self.logger.debug("cmd = %s" % cmd)
        return p.runSync(cmd,self.logger)
# This class will run infaservice Commands
class InfaSrv:
    
    def __init__(self, dom, log):
        self.dom    = dom
        self.logger = log    
        
    def stopServices(self):
        c = 'shutdown ' 
        rc,rm=self._execCmd(c)        
        return rc,rm
    
    def startServices(self):
        c = 'startup ' 
        rc,rm=self._execCmd(c)     
        return rc,rm

    def _execCmd(self,c):
        
        cmd = '%s %s' % (mg.infasrv, c)
        self.logger.debug("cmd = %s" % cmd)
        return p.runSync(cmd,self.logger)
    
#Ping
#[<-DomainName|-dn> domain_name]
#[<-ServiceName|-sn> service_name]
#[<-GatewayAddress|-dg> domain_gateway_host:port]
#[<-NodeName|-nn> node_name]
#[<-ResilienceTimeout|-re> timeout_period_in_seconds]
#The following table describes infacmd isp Ping options and arguments:


# This class will run infacmd Commands
class InfaSetup:
    
    def __init__(self, dom, log):
        self.dom    = dom
        self.logger = log
        
    def domBkup(self,f):
        c = ' backupdomain -da %s:%s -ds %s -dt %s -du %s -dp %s -bf %s -dn %s -f'  % (self.dom.dom_dbhost, self.dom.dom_dbport,  
                                                                                       self.dom.dom_dbservice,self.dom.dom_dbtype, 
                                                                                       self.dom.dom_dbuser,self.dom.dom_dbpwd,
                                                                                       f, #self.dom.dom_bkup,
                                                                                       self.dom.dom_name)
                                                         
                                                 
        rc,rm=self._execCmd(c)
        
        return rc,rm
    
    def _execCmd(self,c):
        cmd = '%s %s' % (mg.infasetup, c)
        self.logger.debug("cmd = %s" % cmd)
        #return 0, "PLEASE IMPLEMENT FX"
        return p.runSync(cmd,self.logger)
