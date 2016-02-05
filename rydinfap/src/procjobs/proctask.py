'''
Created on Feb 8, 2013

@author: eocampo

This module process stand alone tasks.
'''
__version__ = '20130217'



# task bean 
import procdata.procinfa as pi
import bean.infabean     as ibe

def run_task(t,log):
    rc = 1 ; m = ''
    log.debug("task Bean is %s" % t)
    if  t.t_type == 'run_wkf': 
        rc,m = _run_wkf(t.fld,t.cmd,log)
    elif t.t_type == 'run_sh': 
        rc,m = _run_sh(t.fld,t.cmd,log)
    elif t.t_type == 'run_rsh': 
        rc,m = _run_rsh(t.host,t.uname,t.pwd, t.cmd,log)
    else:
        log.error("Invalid Task Type %s" % t.t_type)        
    return rc,m

# This method executes Informatica workflows.
def _run_wkf(f,w,log):
    rc = 0; msg = ''
    ib      = ibe.InfaCmdBean()
    ib.fld=f; ib.wkf=w
    print "UNC pi.runWkflWait(ib,log)"
    #rc,msg = pi.runWkflWait(ib,log)
    if rc == 0 : log.info('fld = %s wkfl = %s rc = %d' % (f,w,rc))
    else       : 
        log.error('fld = %s wkfl = %s rc = %d msg = %s' % (f,w,rc,msg))
    return rc,msg

 
# This method executes local tasks.
# cmd command
def _run_sh(fld,cmd,log):
    rc = 0 ; msg = ''
    log.info("Execute SH dir = %s  cmd = %s " % (fld,cmd))
    return rc,msg

# This method executes remote tasks.
# h host
# u username
# p pwd
# cmd command
def _run_rsh(h,u,p,cmd,log):
    rc = 0; msg = ''
    log.info("Execute SSH host = %s user = %s  pwd = %s cmd = %s " % (h,u,p,cmd))
    return rc,msg
