'''
Created on Feb 8, 2013

@author: eocampo
'''

__version__ = '20130301'
  
class GroupBean(object):
    
    name      = ''   # group name
    order     = -1   # run order
    active    = -1   # if active run 1, else 0
    exitErr   = 1    # Exit on Error, default True
    pipelines = []   # List of pipelines beans. (PipeLineBean)
    
    def __str__(self):
        myData =[]
        myData.append("name  =  %s\n"    % self.name      )     
        myData.append("order =  %s\n"    % self.order     ) 
        myData.append("active =  %s\n"   % self.active    ) 
        myData.append("exitErr=  %s\n"   % self.exitErr   ) 
        myData.append("pipelines = %s\n" % ''.join(str(self.pipelines)))
        return ''.join(myData)

class PipeLineBean(object):
    
    name    = ''   # pipeline name
    order   = -1   # run order
    active  = -1   # if active run 1, else 0
    exitErr = 1    # Exit on Error, default True
    tasks   = []   # List of tasks beans (TaskBean).
    
    def __str__(self):
        myData =[]
        myData.append("name  =  %s\n"  % self.name  )     
        myData.append("order =  %s\n"  % self.order ) 
        myData.append("active =  %s\n" % self.active ) 
        myData.append("exitErr=  %s\n" % self.exitErr ) 
        myData.append("tasks  =  %s\n" % ''.join(str(self.tasks))) 
        return ''.join(myData)

# Use for Task. 
class TaskBean(object):
    name    = ''   # task name
    order   = -1   # run order
    active  = -1   # if active run 1, else 0
    exitErr = 1    # Exit on Error, default True
    stime   = ''   # Start Time. Use a string HH:MM
    wrkdy   = ''   # workday 
    t_type  = ''   # task type. Method to execute
    fld     = ''   # directory/folder
    cmd     = ''   # command to run 
    host    = ''   # Use only for SSH
    uname   = ''   # Use only for SSH
    pwd     = ''   # Use only for SSH
    
    def __str__(self):
        myData =[]
        myData.append("name   =  %s\n"   % self.name  )     
        myData.append("order  =   %s\n"  % self.order ) 
        myData.append("active =  %s\n"   % self.active ) 
        myData.append("exitErr = %s\n"   % self.exitErr) 
        myData.append("stime  = %s\n"    % self.stime ) 
        myData.append("wrkdy  = %s\n"    % self.wrkdy ) 
        myData.append("t_type = %s\n"    % self.t_type) 
        myData.append("fld    =  %s\n"   % self.fld   ) 
        myData.append("cmd    =  %s\n"   % self.cmd   ) 
        myData.append("host   =  %s\n"   % self.host  ) 
        myData.append("uname  =  %s\n"   % self.uname ) 
        myData.append("pwd    =  %s\n"   % self.pwd   ) 

        return ''.join(myData)
    
class ResultBean(object):
    
    name  = ''   # object Name
    rc    = -1   # result code
    rmsg  = -1   # result msg
    pid   = 1    # process id 
    atype = 't'  # g,p,t . Task is default.
    obj   = None
    
    def __str__(self):
        myData =[] 
        myData.append("name =  %s\n" % self.name )     
        myData.append("rc   =  %s\n" % self.rc   ) 
        myData.append("rmsg =  %s\n" % self.rmsg ) 
        myData.append("pid  =  %s\n" % self.pid  )     
        myData.append("obj =  %s\n" % ''.join(str(self.obj)))    
        return ''.join(myData)

# Configuration     
class ConfigBean(object):
    pid     = -1
    appName = '' 
    confDir = ''
    logDir  = ''
    recDir  = ''
    logLvl  = ''
 
    def __str__(self):
        myData =[] 
        myData.append("PID     = %d\n" % self.pid )
        myData.append("appName = %s\n" % self.appName )
        myData.append("confDir = %s\n" % self.confDir )
        myData.append("logDir  = %s\n" % self.logDir  )  
        myData.append("recDir  = %s\n" % self.recDir  )
        myData.append("logLvl  = %s\n" % self.logLvl  )
        return ''.join(myData)