'''
Created on Feb 8, 2013

@author: eocampo

 1- Code will parse the PROG_JOB xml file.
 2- Place the tree in a dictionary
 3- Populate the appropriate beans for further processing:
 
- <PROC_GRP>
- <!--  Group run IDL --> 
-   <group name="Group1" order="1" active="1" exit_on_err="1">
-      <pipeline name="Pipe1.1" order="1" active="1" exit_on_err="1">
-          <task name="Task1.1" order="1" active="1" exit_on_err="1">
           <stime>20:00</stime>           -- Optional 
           <wrkdy>20:00</wrkdy>           -- Optional
           <t_type>run_wkf1.1</t_type> 
           <fld>fld1.1</fld> 
           <cmd>wkf_1.1</cmd> 
           </task>
-          <task name="Task1.2" order="1" active="1" exit_on_err="1">
           <t_type>run_wkf1.2</t_type> 
           <fld>fld1.2</fld> 
           <cmd>wkf_1.2</cmd> 
           </task>
       </pipeline>
     </group>
- <group name="Group2" order="2" active="1" exit_on_err="0">
     <pipeline name="Pipe2.1" order="1" active="1" exit_on_err="1">
-       <task name="Task2.1" order="1" active="1" exit_on_err="1">
          <t_type>run_wkf</t_type> 
          <fld>fld2.1</fld> 
          <cmd>wkf_2.1</cmd> 
        </task>
     </pipeline>
  </group>
</PROC_GRP>
 
Added recovery class.

'''

__version__ = '20130301'


import sys
import xml.etree.ElementTree as ET

#import string as ST

import utils.fileutils as fu
import utils.strutils  as su
import utils.xmlutils  as xu
import bean.jobbean    as jb
    
class ProcJobXML(object):  
        
    def __init__(self, fn, log):
        
        self.fn   = fn
        self.log  = log                 # Incoming Files

    def parseTask(self,tsk):
                
        D = {}
        elems = tsk['task']
        for e in elems:D.update(e)  
      
        jbe = jb.TaskBean()
        jbe.name        = tsk.get( '@name'   , '' )   
        jbe.order       = su.toIntPos(tsk.get( '@order'  , -1 ))
        jbe.active      = su.toIntPos(tsk.get( '@active' , -1 ))
        jbe.exit_on_err = su.toIntPos(tsk.get( '@exitErr',  1 ))
        jbe.stime       = D.get( 'stime' , '' )
        jbe.wrkdy       = D.get( 'wrkdy' , '' )        
        jbe.t_type      = D.get( 't_type' , '' )
        jbe.fld         = D.get( 'fld'    , '' )
        jbe.cmd         = D.get( 'cmd'    , '' )
        jbe.host        = D.get( 'host'   , '' )
        jbe.uname       = D.get( 'uname'  , '' )
        jbe.pwd         = D.get( 'pwd'    , '' )
        
        self.log.debug("JOB BEAN %s " % jbe)
        return jbe      
  
    def parsePipe(self,pl):
              
        plbe = jb.PipeLineBean()
        plbe.name        = pl.get('@name', '' )
        plbe.order       = su.toIntPos(pl.get('@order', -1 ))
        plbe.active      = su.toIntPos(pl.get('@active',-1 ))
        plbe.exit_on_err = su.toIntPos(pl.get('@exit_on_err',1 ))

        # Get tasks within the pipeline
        lst = []        
        for tsk in pl['pipeline']:
            jbe = self.parseTask(tsk)
            lst.append(jbe)
        plbe.tasks = lst

        self.log.debug("PIPE BEAN %s " % plbe)
        
        return plbe
                    
    # Method returns rc 0 if no error and the sched and unsched lists.
    def parseGroup(self,grp):
        
        gb = jb.GroupBean()
        gb.name        = grp.get('@name', '' )
        gb.order       = su.toIntPos(grp.get('@order', -1 ))
        gb.active      = su.toIntPos(grp.get('@active',-1 ))
        gb.exit_on_err = su.toIntPos(grp.get('@exit_on_err', 1 ))
        
        lst = []        
        for pl in grp['group']:            
            plb = self.parsePipe(pl)
            lst.append(plb)
        
        gb.pipelines = lst
        
        self.log.debug( "GROUP BEAN  %s "  % gb)   
        return gb
    
    def chkXMLFile(self):
        
        # Check XML if well formed. no DTDT is checked.
        rc,msg = xu.chk_valid_xml(self.fn)
        if rc != 0:
            self.log.error("rc =%d msg = %s " %  (rc,msg))
        return rc 
    
    # This method returns a list of GroupBeans    
    def parseAll(self):
        tbls = []
        gb   = []        
        s = fu.openFile(self.fn,'r')
        if s is None : 
            self.log.error("Could not open file %s" % self.fn)
            return gb
        
        tree = ET.parse(s)
        root = tree.getroot()
        d = xu.xml_to_dict(root)
        
        if d.has_key('PROC_GRP') is not True or len(d) != 1:
            self.log.error("Missing root Element %s" % 'PROC_GRP')
            return gb
        
        if type(d['PROC_GRP']) == type(str( )) : 
            self.log.error("Empty XML")
            return gb
        
        self.log.info("Start parsing file %s " % self.fn)
        self.log.debug("type d['PROC_GRP].values Need to be LIST = " , type(d.values()))
        
        for grp in  d.values()[0]:
            gb = self.parseGroup(grp)      
            tbls.append(gb)  
            
        return tbls     


# This class is used to generate a recover XML file.
# It uses a seriliazed recovery option.
class RecJobXML(object):  
        
    def __init__(self, fn, pid ,log):
        
        self.fn   = fn
        self.pid  = pid
        self.log  = log                 # Incoming Files
        
    def _setElemTask(self,pipel,tb):
        task  = ET.SubElement(pipel, 'task', name = tb.name , order = '%s' % tb.order, active= '%s' % tb.active, exit_on_err = '%s' % tb.exitErr )
        self.log.debug("\t name = " , tb.name , " order =  ", tb.order , " active= ", tb.active , " exit_on_err = " , tb.exitErr)         
        # Optional Schedule related 
        if tb.stime != '' : ET.SubElement(task,"stime").text =  tb.stime
        if tb.wrkdy != '' : ET.SubElement(task,"wrkdy").text =  tb.wrkdy        
        #if tb.wrkdy  > -1 :  '%s' % 27  
        
        # Mandatoty fields Task Fields
        ET.SubElement(task,"t_type").text= tb.t_type
        ET.SubElement(task,"fld"  ).text = tb.fld
        ET.SubElement(task,"cmd").text   = tb.cmd
        
        # CMD fields (optional)
        if tb.host  != '' :ET.SubElement(task,"host" ).text = tb.host
        if tb.uname != '' :ET.SubElement(task,"uname").text = tb.uname
        if tb.pwd   != '' :ET.SubElement(task,"pwd"  ).text = tb.pwd
        
    def _build2XML(self, grpbl):
        root = ET.Element('PROC_GRP')  
        comment = ET.Comment('For Recovery PID %s. Generated on %s ' % (self.pid, su.getTodayDtStr('%m/%d/%Y %H:%M:%S')))
        root.append(comment)
        
        for g in grpbl:
            self.log.debug(" name = " , g.name , " order =  ", g.order , " active= ", g.active , " exit_on_err = " , g.exitErr)
            root.append(ET.Comment('Group %s' % g.name))
            group = ET.SubElement(root, 'group', name = g.name, order = '%s' % g.order, active= '%s' % g.active, exit_on_err = '%s' % g.exitErr )
        
            for p in g.pipelines:   
                self.log.debug("\t name = " , p.name , " order =  ", p.order , " active= ", p.active , " exit_on_err = " , p.exitErr)         
                pipel = ET.SubElement(group, 'pipeline',name = p.name, order = '%s' % p.order, active= '%s' % p.active, exit_on_err = '%s' % p.exitErr )
                for t in p.tasks:
                    self._setElemTask(pipel,t)
    
        return root
    
    def writeXmlFile(self,grpbl):
        f = fu.openFile(self.fn, 'w')
        if f is None : 
            self.log.error("Could not create file %s " % self.fn)
            return -1
        rc = 0 
        root = self._build2XML(grpbl)
        try :
            ET.ElementTree(root).write(self.fn)
        except:
            self.log.error("ENV var not set %s %s\n" % (sys.exc_type,sys.exc_value))
            rc = 1
            
        finally:
            fu.closeFile(f)
            return rc

   
def test_job():
    fn = r'C:\infa_support\process.xml'
    g = ProcJobXML(fn,'')
    gb = g.parseAll()
    #print "GRP ", gb
    print "================= DONE ====================="
    #parseSched(fn)
    
if __name__ == "__main__":
      test_job()