'''
Created on Feb 8, 2013

@author: eocampo

This class is to Process Jobs.

The highest level is the Group which executes serially/sync based on Priority; within groups you have pipelines which can run in paralallel/async within the same
priority and then will run serially. Lowel number priorities are first in the queue.


GROUP 1  1 --> pipeline A   1
           --> pipeline B   1
           --> pipeline c   1
           --> pipeline D   2
           --> pipeline E   3

GROUP 2  1 --> pipeline A   1
           --> pipeline B   2
           --> pipeline c   2
           --> pipeline D   2
           --> pipeline E   5
           
Based on current logic Group 1 will start execution and then 3 threads will launch simultaneously (priority 1). 
After Group 1 is completed then Group 2 will start. 

'''
__version__ = '20130301'

import time  
import os
import common.log4py   as log4py 
import multiprocessing as mp

import utils.datasrtuct as pq
import utils.fileutils  as fu
import utils.strutils   as su
import proctask         as pt
import procjobxml       as pxml

import bean.jobbean    as jb
from utils.strutils import toSec

class ProcessJobs(object):
    
    summary  = []
    res_list = []    # Standalone process results.
    recDat   = []    # Recovery Data List for GroupBeans.
    
    #wd - Current workday.
    #cb - configuration bean.s
    def __init__(self,wd,cb,log):

        self.wkday = wd 
        self.log   = log
        self.cb    = cb         # This is the configuration bean.
  
    # res is a list [taskName,rc,PID,rt]         
    def logRes(self, res):
        self.res_list.extend(res)
        
    def getRecData(self): 
        self.log.debug('Recdata = ', self.recDat)
        return self.recDat
    
    # It will create a priority queue based on run order.
    # It will only insert in the queue objects that are active.
    # lst List of data objects .
    # qt  queue type for logging purposes
    def bldPQueue(self,lst,qt):
        
        q = pq.PriorityQueue(100)
        for p in lst:
            if p.active == 1:
                q.insert(p, p.order)
                self.log.debug("Bean Type %s " % type(p))
                self.log.info("%s: %s priority=%d will run\tactive=%s"  % (qt,p.name,p.order,p.active))
            else :  self.log.info("%s: %s priority=%d will NOT run\tactive=%s" % (qt, p.name,p.order,p.active))
            
        return q
     
     
    # stime is the time to run in seconds in particular day HH:MM.
    def _waitToRun(self, rtime):
        print "rtime is ",rtime
#        rc = su.isValidDate(stime,'%H:%M')
#        if rc is False : 
#            
#            return 1    
        ct = su.getTodaySec()
        self.log.info('Current Time %d (s) Start time %d (s) ' % (ct,rtime))
        wt = rtime - ct
        if wt  > 0 : 
            self.log.info('Will wait for %d (s)' % (wt))
            time.sleep(wt)
            
        return 0
    
    # Check for a particular workday to run.
    # Not implemented yet ! Return False since this property should not be used for now.
    def _isWorkDay(self,wd2r):
        
        wd2run = su.toInt(wd2r)
        if wd2run is None or  wd2run < 1 or  wd2run > 25:
            self.log.error("Invalid daywork %s. Valid range between 1 to 25"  % wd2run)  
            return False
        
        self.log.info('WorkDay to Run %d\tCurrent workDay %d' % (wd2run,self.wkday))
        if ( self.wkday == wd2run) : return True
            
        return False
        
    # Check Series of condition to determine if task should run today !
    # t task bean.
    def _runToday(self,t):
        
        if t.wrkdy != '': 
            rf = self._isWorkDay(t.wrkdy)
            if rf is False: return rf
        
        if t.stime != '' :    # Only if job has start time !
            rtime =   toSec(t.stime)      # Need to hour minute.      # Need to hour minute. 
            self.log.debug('stime = %s rtime = %s' % (t.stime,rtime))
            self._waitToRun(rtime)

        return True
        
    # This method loads the execute plan
    # fxml Execute Plan File    
    def loadExecPlan(self,fxml):
        grpbl = []
        xh  = pxml.ProcJobXML(fxml,self.log)
        rc =  xh.chkXMLFile()
        if rc != 0 : return grpbl
        grpbl = xh.parseAll()
        return grpbl
 
    # This method creates an execute plan based on xml proc file
    # grpbl contains the list of GroupBeans from xml file.
                
    def _showExecPlan(self,grpbl):  
        self.log.debug("From XML File grpbl %s " % grpbl ) 
        self.log.info("==========> Start building Execution Plan <========== ")
        for g in grpbl:
            if g.active == 1: self.log.info("Group: %-20s order=%d will run active=%s"  % (g.name,g.order,g.active))
            else            : self.log.info("Group: %-20s order=%d will NOT run active=%s" % (g.name,g.order,g.active))
            
            for p in g.pipelines:
                if p.active == 1 : self.log.info("--PipeL: %-23s order=%d will run active=%s"  % (p.name,p.order,p.active))
                else             : self.log.info("--PipeL: %-23s order=%d will NOT run active=%s" % (p.name,p.order,p.active))
                
                for t in p.tasks:
                    if t.active == 1 : 
                        msg = "----Task: %-22s order=%d will run active=%s " % (t.name,t.order,t.active)
                        if t.stime != '' : msg += "starts=%s"  % t.stime
                        if t.wrkdy != '' : msg += "workday=%s" % t.wrkdy
                        self.log.info(msg)
                    else             : self.log.info("----Task: %-22s order=%d will NOT run active=%s" % (t.name,t.order,t.active)) 

                
        self.log.info("==========> Completed Execution Plan <========== ")               
        return 0
    
    def printExecPlan(self,grpbl):  
        print("==========> Start building Execution Plan <========== ")
        for g in grpbl:
            if g.active == 1: print("Group: %-20s order=%d will run active=%s"  % (g.name,g.order,g.active))
            else            : print("Group: %-20s order=%d will NOT run active=%s" % (g.name,g.order,g.active))
            
            for p in g.pipelines:
                if p.active == 1 : print("--PipeL: %-23s order=%d will run active=%s"  % (p.name,p.order,p.active))
                else             : print("--PipeL: %-23s order=%d will NOT run active=%s" % (p.name,p.order,p.active))
                
                for t in p.tasks:
                    if t.active == 1 : 
                        msg = "----Task: %-22s order=%d will run active=%s " % (t.name,t.order,t.active)
                        if t.stime != '' : msg += "starts=%s"  % t.stime
                        if t.wrkdy != '' : msg += "workday=%s" % t.wrkdy
                        print msg
                    else             : print("----Task: %-22s order=%d will NOT run active=%s" % (t.name,t.order,t.active)) 

                
        print("==========> Completed Execution Plan <========== ")               
    
    # Groups run synch, otherwise we could loose control of all spawned processes ! (in terms of numbers)
    # This method will process Groups.
    # This code supports serial Groups run based on group order.
    # Each group contains pipelines Beans that could execute in parallel/serial based on pipeline run order.
    # grpbl contains the list of GroupBeans from xml file.

    def _runGroup(self,grpbl):
        grpRes = [] 
        #rc = -1
        q = self.bldPQueue(grpbl, 'Group')
        if q is None or q.size() < 1: return -1
        gsz = q.size()
        
        for n in range(gsz):
            g = q.pop()
            self.log.debug( "\n-- Group-- = %s\n------------" % g)
            lp = len(g.pipelines)
            self.log.info("=======> Group %s has %d pipelines in Config File. <======= " % (g.name, lp))
            if  lp < 1 : 
                self.log.warn("Will Skip Group %s." % (g.name))
                continue

            #self.log.info("Start Building job execution plan")
            plq = self.bldPQueue( g.pipelines,'Pipeline')
            psz = plq.size()
            self.log.info("Group %s has %d pipelines to Process." % (g.name, psz))
            if (psz < 1):
                self.log.warn("Will Skip Group %s." % (g.name))
                continue
            
            # At this point Group has at least 1 active pipeline.  
            # jb job batch. [ [pipeline1,pipeline2,pipeline3], [pipeline4,pipeline5] ... [pipeline n ..]]             
            jb = self._getPipeLineRunGrp(plq)
            self.log.debug('Jobs batches', jb)
            if len(jb) < 1 :
                self.log.warn('Group %s has no pipeline job group to run!' % g.name)
                continue
            
            # Will start executing All pipelines within a run group.
            grbs = self._execPipelineRunGrp(jb,g)
            for gb in grbs:
                self.log.debug('===============res OBJ = %s' % gb)
                if gb.obj is not None : 
                    self.log.debug('================== APPEND  OBJ = %s' % gb.obj)
                    self.recDat.append(gb.obj)
         
            grpRes.extend(grbs)
            r = 0
            if r == 0 : self.log.info( '=======> completed group %s rc= %d  exitOnErr %d <======= ' % (g.name,r,g.exitErr))
            else      : self.log.error('=======> failed group    %s rc= %d  exitOnErr %d <======= ' % (g.name,r,g.exitErr))   
            #rc += r   #EO Work on this one
            if r != 0 and g.exitErr == 1: break

        rc = self._procRes(grpRes)
        return rc                
 
 
    # This method will create 2 dim array. It will hold the pipelines (job groups) to be run simultaneously.
    # returns jg [pipeline1,pipeline2,pipeline3], [pipeline4,pipeline5] ... [pipeline n ..]
    # jobn are PipelineBeans.
    def _getPipeLineRunGrp(self,q):
        jg = []   ; ta = []
        if q is None or q.size() < 1: return jg   # Sanity Check. By contract should never Fail !
        sz = q.size()
        plb  = q.top()
        cord = plb.order 
        for n in range(sz):
            plb = q.pop()
            self.log.debug( "ele %d == PipeLineBean(plb) = %s" % (n,plb))
            
            if cord == plb.order:     # Same order Pipeline
                ta.append(plb)
                self.log.info("Appending %s order %s" % (plb.name,plb.order))
                              
            else :                    # Lower order Pipeline
                self.log.info("Completed order pipeline. ")
                jg.append(ta)
                ta = []
                cord = plb.order
        return [jg + ta,]
 
    # This method will execute all the pipelines within a group.
    # jg job groups is a 2 dim array containing the pipelines to run.
    # This represent a set of tasks within a pipeline and already order per job order group.
    # jg [pipeline1,pipeline2,pipeline3], [pipeline4,pipeline5] ... [pipeline n ..]. Contains Pipelines Beans objects.
    # It will invoke the pool and run concurrently [pipeline1,pipeline2,pipeline3]. Will wait until this first group completes.
    #    will start the next set [pipeline4,pipeline5] and wait until it completes.
    # gb Group Bean
    # returns a list of ResulBeans pipGrpRes.
    def _execPipelineRunGrp(self,jg, gb):
        rc = 0; pipGrpRes = []; rp = [];   #rt for pipe recovery
        jg.reverse() ; sz = len(jg)    # Stack
        
        self.log.info("Job batch(es) = %d " % len(jg))
        self.log.debug("Job batches(jg) =  ", jg)
        i = 1
        for n in range(sz):           # [pipeline1,pipeline2,pipeline3] plst is a pipeline list that need to run in parallel.
            plst = jg.pop()  
            wpsz =  len(plst)
            self.log.info("=== Creating Pool with %d workers. Iteration %d  == " % (wpsz,i))   # Will create 1 worker per pipeline group
 
            # Create Pool Based on Job
            pe = 0
#            pool = mp.Pool(wpsz)
#            for p in plst:                # for pipe in pl
#                pe+=p.exitErr 
#                pool.apply_async(self._execTasks, args = (p,lgLvl), callback = self.logRes)
#    
#            pool.close()
#            pool.join()
            

                
            # EO TO BE REMOVED : Only for testing/serial run
            
            for p in plst:
                rlst = self._execTasks(p)
                print "RLST = ", rlst
                self.logRes(rlst)
                print "============> result_list is ", self.res_list   
                
                pipGrpRes.extend(rlst) 
                #self.res_list.extend(rlst)
            ############ END TO BE REMOVED 
            
            r = 0; 
            for rb in self.res_list: 
                r+= rb.rc
                if rb.obj is not None:
                    pe+=rb.rc
                    rp.append(rb.obj)
            
#            
#            self.log.debug( "result_list is ", self.res_list)
            
            if r > 0 and pe > 0:
                break
#            for e in self.res_list: print "e = %s" % e
        
            del self.res_list[:]
            i+=1
            # Get all return codes and analyze before continuing !
        
        # For recovery 
        sz = len(jg)
        for n in range(sz):            
            plst = jg.pop()
            print "INSIDE RECOVERY PLST =", plst
            for p in plst:
                pipGrpRes.append(p)
                print "============>RECOV P = %s" %  p
                     
        gb.pipelines = rp
        gb.active = 0  if rc == 0 else 1
        rb = jb.ResultBean()
        rb.name = gb.name; rb.rc = rc; rb.rmsg = 'Group Completed'; rb.pid = os.getpid() ; rb.atype = 'g' ; rb.obj = gb 
        pipGrpRes.insert(0, rb)    
        print "GroupResult  = ", pipGrpRes
        return pipGrpRes
            
    # This method will process tasks within Pipelines. This gets fork therefore needs it own log.
    # It create a priority queue based on  taskLst, only for jobs with active = 1.
    # IMPORTANT : Each pipeline can only execute tasks in serial order.
    # taskLst Beans list with tasks to run for the pipeline.
    # pipeline 1 : job1 -> job2 -> .. job n
    # Need to append all results, so when forked process completes ia available to parent !
    # pb -> Pipe Bean.
    # ldir     -> logging  directory.
    # pipRes : returns a list of resultBean(s), in which PipeLine result is always at index(0). 
    # For recovery purposes this method returns the pipeLines(bean) rb.obj , updated based on execution results.
    # 
    def _execTasks(self,pb):
        pipRes = []; rc = 0; rt = [];   #rt for task recovery.
        mpid    =  os.getpid()        
        logFile = '%s/%s.log' % (self.cb.logDir,pb.name)
        log     =  log4py.Logger().get_instance()
        log.set_loglevel(log4py.LOGLEVEL_VERBOSE)
        log.add_target(logFile)
        log.info("CP logfile:%s" % (logFile))
        
        q = self.bldPQueue( pb.tasks,'Tasks')
        if q is None or q.size() < 1: return pipRes   # Sanity Check. By contract should never Fail !
        sz = q.size()
        log.info('starting pipe %s with %d tasks' % (pb.name, sz))    
        #n = 0 ; 
        
        for n in range(sz):
            t = q.pop()
            if self._runToday(t) is False:
                log.info('Task (%d) %s should not run today.' % (n,t.name))
                rt.append(t)     # Add Bean unchanged.
                log.debug("APPEND t to result bean (rt) %s : " % t)
                rb = jb.ResultBean()
                rb.name = t.name; rb.rc = 0; rb.rmsg = "Not sched to run toady !"; rb.pid = mpid ; 
                pipRes.append(rb)  
                continue
            
            log.info('starting task (%d) %s ExitonError = %d ' % (n,t.name, t.exitErr))  
            r,rmsg = pt.run_task(t,self.log)
            rc+=r
            t.active = 0  if r== 0 else 1
            log.debug("APPEND t to result bean (rt) %s : " % t)
            rt.append(t)
            rb = jb.ResultBean()
            rb.name = t.name; rb.rc = r; rb.rmsg = rmsg; rb.pid = mpid ; 
            pipRes.append(rb)            
            log.info('rc= %d msg = %s ' % (r,rmsg))
            if r != 0 and t.exitErr == 1: break
             
        # For recovery process, this have not run, no changes in 'active' status.   
        log.debug('Task Recovery n=%d  sz=%d ' % (n,sz))
        for  n in range(sz):   
            if q.isEmpty() : break         
            t = q.pop()
            rt.append(t)
            log.debug("APPEND t to result bean (rt) %s : " % t)
             
        # Pipeline overall result        
        pb.active = 0  if rc == 0 else 1
        pb.tasks  = rt
        rb = jb.ResultBean()
        rb.name = pb.name; rb.rc = rc; rb.rmsg = 'PipeLine Completed'; rb.pid = mpid; rb.atype = 'p' ;rb.obj = pb 
        pipRes.insert(0, rb)
        log.debug('==> Pipe Completed pipRes = ', rb )

        return pipRes
    
    def _procRes(self,grpRes):
        self.log.info("\n==========> Application run results <========== ")
        if grpRes is None or len(grpRes) < 1 : return -1
        rc = 0
        for r in grpRes:
            if   r.atype == 'g':
                self.log.info('%-25s%s=%d' % (r.name,'rc',r.rc))
            elif r.atype == 'p':
                self.log.info('---%-25s%s=%d' % (r.name,'rc',r.rc))
            else :
                self.log.info('------%-25s%s=%d' % (r.name,'rc',r.rc))
            rc+= r.rc    
                
        return rc
                
    # Recovery Methods:
    # This method will generate recovery information based on PPID. 
    def createRecXMLFile(self,fxml,recPid):
        # read from serialized object.
        frec = '%s/%s/%s%s.rec' % (self.cb.recDir,recPid,self.cb.appName,recPid)     # Serialized REC object
        if not fu.fileExists(frec) :
            self.log.error('Recovery File %s does not exist! Will not be able to generate recovery plan' % frec)
            return -1 
        
        self.log.info('REC Recovery File : %s' % frec)
        grpbl = fu.loadFSDB(frec)
        self.log.debug("GRPBL =", grpbl)
        if grpbl is None : return -1    

        # Create an xml recovery File
        self.log.info('XML Recovery File : %s' % fxml)
        rj = pxml.RecJobXML(fxml,self.cb.pid,self.log)
        rc = rj.writeXmlFile(grpbl)
        return rc
    
    # Run main jobs
    # Reload plan file since 
    def mainProc(self,fxml,grpbl):           
        # Set LogDir for children.  
        logD = '%s/%s' % (self.cb.logDir,self.cb.pid)
        rs = fu.createDir(logD)  
        if rs == 0 : self.log.info("Created logDir = %s" % logD)
        else       : self.log.error("Could Not Create logDir = %s " % logD)
 
        recD = '%s/%s' % (self.cb.recDir,self.cb.pid)
        rs = fu.createDir(recD)  
        if rs == 0 : self.log.info("Created recDir = %s" % recD)
        else       : self.log.error("Could Not Create recDir = %s " % recD)
                     
        self.cb.logDir = logD
        self.cb.recDir = recD
    
        # Create the execute Plan
        rc = self._showExecPlan(grpbl)
        if rc != 0 : return rc
        
        rc = self._runGroup(grpbl)
        if rc == 0 :                  # TIDI change to rc !=0 .
            frec = '%s/%s%s.rec' % (self.cb.recDir,self.cb.appName,self.cb.pid)
            rdat = self.getRecData()
            r = fu.saveFSDB(frec, rdat)
            self.log.info('Writing Recovery Data to %s == rc = %s' % (frec,r))
         
        return rc
    
def main():
    cb = jb.ConfigBean()
    cb.pid = os.getpid()
    cb.appName = 'mrcincload' 
    cb.confDir = r'C:\apps\config'
    cb.logDir  = r'C:\apps\logs'
    cb.recDir  = r'C:\apps\logs'
    cb.logLvl  = log4py.LOGLEVEL_VERBOSE

    log  =  log4py.Logger().get_instance()
    log.set_loglevel(log4py.LOGLEVEL_DEBUG)
    logFile = '%s/%s.log' % (cb.logDir,cb.appName )
    log.add_target(logFile)
    #log.set_loglevel(log4py.LOGLEVEL_VERBOSE) 
    fxml = '%s/%s.xml' % (cb.confDir,cb.appName)
                          
    pj = ProcessJobs(10,cb,log)

    grpbl = pj.loadExecPlan(fxml)
    if len(grpbl) < 1 : return 1

    rc = pj.mainProc(fxml,grpbl)
    
    print "mainProc returns %s" % rc
    return rc

def main_rec():
    #PID comes from command line. recPID
    recPid = 10156
    cb = jb.ConfigBean()
    cb.pid = os.getpid()
    cb.appName = 'mrcincload' 
    cb.confDir = r'C:\apps\config'
    cb.logDir  = r'C:\apps\logs'
    cb.recDir  = r'C:\apps\logs'
    cb.logLvl  = log4py.LOGLEVEL_VERBOSE

    log  =  log4py.Logger().get_instance()
    log.set_loglevel(log4py.LOGLEVEL_DEBUG)
    logFile = '%s/%s.log' % (cb.logDir,cb.appName )
    log.add_target(logFile)
      
    pj = ProcessJobs(cb,log)
    fxml = '%s/%s/%s%s.xml' % (cb.recDir,recPid,cb.appName,recPid)
    rc = pj.createRecXMLFile(fxml,recPid)
    print "RES = %s " % rc

# EO For standalone testing only. Do not invoke from this method.!    
#def main(fn):
#    mpid = os.getpid()
#    logDir = r'C:\apps\logs'
#    recDir = r'C:\apps\logs'
#
#    #Get the Logname
#    
#    log  =  log4py.Logger().get_instance()
#    log.set_loglevel(log4py.LOGLEVEL_DEBUG)
#    #log.set_loglevel(log4py.LOGLEVEL_VERBOSE)
#    logFile = getLogHandler(fn,log)
#    log.info("==PID==%d" % mpid)
#    log.info("Starting procjobs logfile is %s" % logFile)
#    nd = '%s/%s' % (logDir,mpid)
#    rs = fu.createDir(nd)  
#
#    #fn = r'C:\infa_support\process.xml'    
#    fn  = r'C:\apps\config\mrcincload.xml'    
#    frn = r'%s\%s\mrcincload%s.rec' % (logDir,mpid,mpid)
#    g  = pxml.ProcJobXML(fn,log)
#    if g is None : return -1
#    rc =  g.chkXMLFile()
#    if rc != 0 : return rc
#        
#    grpbl = g.parseAll()
#    if len(grpbl) < 1:
#        log.error('Nothing to process!. Check Contents of %s' % fn) 
#        return -2
#    
#    pj = ProcessJobs( logDir,recDir,log)
#    rc = pj.createExecPlan(grpbl)
#    rc = pj.runGroup(grpbl)
#    rl = pj.getRecData()
#    print "RL ",  rl
#    if rc == 0 :          # Should be !=. This is for testing only.
#        r = fu.saveFSDB(frn, rl)
#        log.info('Writing Recovery Data to %s rc = %s' % (frn,r))
#    return rc

#def main_rec(fn):
#    
#    cb = jb.ConfigBean()
#    cb.appName = '' 
#    cb.confDir = ''
#    cb.logDir  = ''
#    cb.recDir  = ''
#
#    logDir = r'C:\apps\logs'
#    recDir = r'C:\apps\logs'
#    grpbl = []
#    #Get the Logname
#    
#    log  =  log4py.Logger().get_instance()
#    log.set_loglevel(log4py.LOGLEVEL_DEBUG)
#    #log.set_loglevel(log4py.LOGLEVEL_VERBOSE)
#    logFile = getLogHandler(fn,log)
#    print "LogFileName %s" % fn
#    frec = r'C:\apps\logs\6424\mrcincload6424.rec'
#    
#    pj = ProcessJobs( logDir,recDir,log)
#    rc = pj.createRecFile(6424)
#    print "RES = %s " % rc
    
    
    
    #pj = ProcessJobs( logDir, recDir ,log)
    #rc = pj.createRecFile()
    
    #rc = pj.createExecPlan(grpbl)
    
   
if __name__ == '__main__':
    import common.log4py   as log4py 
    from   common.loghdl   import getLogHandler
    rc= main()
    #rc= main_rec()
    print "rc = %s " % rc