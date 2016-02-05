'''
Created on Jun 20, 2011
This module is used for informatica admin purposes
It gets an http response from domais and it parses to display in our admin interface.
@author: eocampo
'''

import urllib
from datetime import datetime 
import sys

import mainglob as mg
import mainmsg  as mm

from proc import process

# Get Domain Information
# s = server:port No e.g. jbxsd307:6001
# returns  a list with services. 
def _getURLUptime(s):
    #sr = None
    f = urllib.urlopen("http://%s/coreservices/DomainService" % s)
    sr = f.read()
    f.close()     
    return sr


# Process url string
# sr server response
# rl = [('AlertService ', ' enabled'), ('AuthenticationService ', ' initialized'), ('InitTime ', ' Tue Jun 14 13:57:20 EDT 2011'),] 
def _procServUpResp(sr):
    rl = []
    serv = sr.split('\n')
    for r in serv:
        x   = r.split(':',1)
        l = x[0].split("/")[-1],x[1]
        rl.append (l)
    return rl
       
#rl response list
# returns r = startTime, d = days up 
def _renderUptime(rl):
    srv = []
    for s,r in rl:
        if s.strip() == 'InitTime':
            ut = _getUptime(r)
            st = r
        else:
            srv.append("%s %s" % (s,r))    
    return (st,ut,srv)

# This fx converts a string inti a date and returns the datediff, which represents server uptime:
# sd format ='Tue Jun 14 13:57:20 EDT 2011'
def _getUptime(sd):
    t = sd.split()
    n = datetime.strptime('%s %s %s %s' % (t[1],t[2],t[5],t[3]), '%b %d %Y %H:%M:%S')
    td = datetime.now() - n
    return ':'.join(str(td).split('.')[:1])
   
# This method is driver to get data from all domain servers.  
def getServStatus():
    rs = []
    for dom in mg.infa_dom_rs:
        srv =''
        try:
            #Returns Uptime DomainService/InitTime : Sat Nov 19 12:31:42 EST 2011
            sr  = _getURLUptime('%s:%s' %(dom.serv_name,dom.dom_port))
            print "SR = ", sr
        except IOError:
            dom.st,dom.ut = 'Error Connecting', '%s'  % (sys.exc_value)
            rs.append((dom,srv))
            continue
        
        # returns a list [ ('InitTime ', ' Sat Nov 19 12:31:42 EST 2011'),]
        lst = _procServUpResp(sr)
        #dom.st =  Sat Nov 19 12:31:42 EST 2011 ,dom.ut = 58 days, 2:11:55 ,srv = []
        dom.st,dom.ut,srv = _renderUptime(lst) 
        rs.append((dom,srv))
             
    return rs

# Note this function uses a third party client sw for ssh. Need to explore pexpect.
# 
def _getRemConnCred(sn,un):
    
    for s in mg.serv_cred_rs:
        if ( sn == s.sname and un == s.uname ) : return  s.pwd
            
    return ''

# This method is use to verify that the uname /pwd is OK. In order to disable a connection with 
# invalid password, otherwise account will be LOCKED.    
def _verUserConn(sn,un,pw):
    
    cmd='%s\plink.exe -ssh %s@%s -pw %s -batch pwd' % (mg.binDir,un, sn,pw) 
    rc,rcmd = process.runSync(cmd,mg.logger)
    mg.logger.debug("cmd = %s rc = %s " % (cmd,rc)) 
    if rc == 0 and len(rcmd) > 1 : return 0
    else:                          return -2
    
# connects to Remote Server.
# s  servr ID needs to be a number
# un user name
def connRemServ(s,un):
    rc = -1

    if un.strip() == '': return -2
    
    if isinstance(s, str) and s.isdigit(): sid = int(s)
        
    elif isinstance(s, int)               : sid = s
       
    else: return mm.INV_ID_DTYPE 
   
    if not mg.serv_lkup.has_key(sid): return mm.INV_SRV_KEY
    
    sn = mg.serv_lkup[sid]
    
    pwd = _getRemConnCred(sn,un)
    if  pwd.strip() != '':
        rc = _verUserConn(sn,un,pwd)
        # un/pwd OK lets go ahead and invoke the ssh command.
        if rc == 0:
            cmd = '%s\putty.exe -ssh %s@%s -pw %s' % (mg.binDir, un, sn, pwd) 
            mg.logger.debug("cmd = %s " % cmd)
            rc,rm = process.runSync(cmd,mg.logger)
            return 0
            
    return rc
                
     