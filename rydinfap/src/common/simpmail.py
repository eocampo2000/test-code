'''
Created on Feb 14, 2012

@author: eocampo
Added a Pager method.

'''
__version__ = '20120928'

import smtplib

from email.MIMEMultipart  import MIMEMultipart
from email.MIMEBase       import MIMEBase
from email.MIMEText       import MIMEText
from email.Utils          import COMMASPACE, formatdate
from email                import Encoders
import os,sys, re

SUCCESS = 0
FAILURE = 1

# Input a , delimeted string and outputs a list with valid strings.
def valEmailAddr(eStr):
    pattern = ("^.+\\@(\\[?)[a-zA-Z0-9\\-\\.]+\\.([a-zA-Z]{2,3}|[0-9]{1,3})(\\]?)$")
    valEmail = []  
    invEmail = '' 
    ea = eStr.split(',')
    for e in ea:        
        addr = re.findall(pattern, e)
        if addr != None and len(addr) > 0:
            valEmail.append(e)
        else:
            invEmail += '\t%s' % e
            
    return valEmail, invEmail
    
# Use for notification purposes.
# rc   code to send email on Error rc=0 success, otherwise failure.
# subj email subject. 
# text email text.
# lfile list of attachment files []. Notes needs to be sent as a list.
def sendNotif(rc,log,subj,text,lfile,rd='plain'):
    
    touser    = os.environ['MAIL_LIST']   if os.environ.has_key('MAIL_LIST')   else None
    pager     = os.environ['PAGER_LIST']  if os.environ.has_key('PAGER_LIST')  else None
    mailOnErr = True                      if os.environ.has_key('MAIL_ON_ERR') else False
    pgOnErr   = True                      if os.environ.has_key('PAGE_ON_ERR') else False
    fromUser  = os.environ['FROM']        if os.environ.has_key('FROM')        else 'infa@ryder.com'
    if  touser is None and pager is None:
        msg = 'No notification is set MAIL_LIST or/and PAGER_LIST is/are not set'
        return -100,msg
    else : 
        rc = notify(rc, touser, pager, subj, text, mailOnErr, pgOnErr,  fromUser, log, lfile,rd)
        msg = 'Sending Notification rc=%s touser=%s pager=%s subj=%s text=%s pgOnErr=%s mailOnErr=%s fromUser= %s render = %s' % (rc,
                                                                                                                         touser,
                                                                                                                         pager,
                                                                                                                         subj,
                                                                                                                         text,
                                                                                                                         pgOnErr,
                                                                                                                         mailOnErr,
                                                                                                                         fromUser,
                                                                                                                         rd)
        return rc,msg

#===============================================================================
# class that will handle notification(s) based on a pre-defined set of flags (conditions
# and email Information.
# rc      : Return code from a particular test/condition.
# subj    : email subject
# text    : email text
# usr (TO): comma delimeted str (with valid email adresses)
# pg (TO) : comma delimeted str (with valid pager adresses)
# pgOnErr  : Send page for err condition only
# mailOnErr: Mail on error condiiton only
# files  : attachment file list. Default empty
#===============================================================================

def notify(rc, usr, pg , subj, text, mailOnErr,pgOnErr, fu, log, files=[],rd='plain'): 
        rv = SUCCESS
        pager  = []
        touser = []
        try:      
        # ---------------   Pager Section will not include attachments. ---------------------------------        
            if (pg is not None):                                    # We have a list
                pager,ipgr = valEmailAddr(pg)
                if len(pager) >  0:
                    if ipgr != '' : log.error("Invalid pager addresses %s " % ipgr)
                    if (pgOnErr == False):# Always send a page.
                        rv = _sendPage(pager,fu,subj,text)
                        log.info("sendPage ALWAYS to : %s subj= %s RC = %s " % (str(pager),subj,rv))
                    elif (rc != 0 ):                                       # Send a page on Error Only
                        rv = _sendPage(pager,fu,subj, text)
                        log.info("sendPage on Error to %s subj=%s RC = %s " % (str(pager),subj,rv))
                else:
                    log.error("Pager address(es) are invalid %s. Not sending Pages" % ipgr) 
            else:
                log.debug("NOT sending Pages") 
      
        # ---------------  Email Sections                               ---------------------------------
            if(usr is not None and len(usr) > 0):
                touser,ipgr = valEmailAddr(usr)
                if len(touser) > 0:      
                    if ipgr != '' : log.error("Invalid mail addresses %s " % ipgr)                               
                    if (mailOnErr == False):                                 # Always send an email
                        rv = _sendMail(touser, fu, subj, text,files,rd)
                        log.info("sendMail ALWAYS to recipients = %s rv = %s" % (str(touser),rv))
                        
                    elif (rc != 0 ):                                         # Send an email on Error Only
                        rv = _sendMail(touser,fu,subj, text,files,rd)
                        log.info("sendMail on ERROR to recipients = %s rv = %s" % (str(touser),rv))
                        
                else:
                    log.error("Mail address(es) are invalid %s. Not sending Mail" % ipgr) 
                            
            else:
                log.debug("NOT sending Emails")                
        
        except:
            log.error("EXCEP %s %s" % (sys.exc_type,sys.exc_value))
            log.error("EXCEP Check if attachment list and/or users are Lists")
            rv = FAILURE
       
        finally: 
            return rv

# Note make sure if you invoke this method, to catch the exception somewhere in the caller hierarchy.
def _sendMail(toUser, fromUser, subject, text, files=[],rd = 'plain'):

    #smtpServer  = os.environ['SMTP']   if os.environ.has_key('SMTP')  else 'localhost'
    smtpServer = os.environ['SMTP'] if  os.environ.has_key('SMTP') else None
    #smtpServer = 'rydersendmailrelay.ryder.com'
    if smtpServer is None:
        log.error("Need to set env SMTP")
        return 3

    msg = MIMEMultipart()
    msg['From']    = fromUser
    #msg['To']     = toUser
    msg['To']      = COMMASPACE.join(toUser)          # To user needs to be a list !
    msg['Date']    = formatdate(localtime=True)
    msg['Subject'] = subject
    print " MSG TO ", msg['To']
    #msg.attach( MIMEText(text,'html') )
    msg.attach( MIMEText(text,rd) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"'
                       % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(smtpServer)
    smtp.sendmail(fromUser, toUser, msg.as_string())
    smtp.quit()    # Invokes smtp.close. This call should return : (221, '2.0.0 sscldinf.ryder.com closing connection')
    return SUCCESS

# Note make sure if you invoke this method, to catch the exception somewhere in the caller hierarchy.
def _sendPage(toUser, fromUser, subject, text):

    #smtpServer  = os.environ['SMTP']   if os.environ.has_key('SMTP')  else 'localhost'
    smtpServer = os.environ['SMTP'] if  os.environ.has_key('SMTP') else None
    if smtpServer is None :
        log.error("Need to set env SMTP")
        return 3
    
    msg = MIMEText(text)
    msg['From']    = fromUser
    #msg['To']     = toUser
    msg['To']      = COMMASPACE.join(toUser)          # To user needs to be a list !
    msg['Date']    = formatdate(localtime=True)
    msg['Subject'] = subject

    smtp = smtplib.SMTP(smtpServer)
    smtp.sendmail(fromUser, toUser, msg.as_string())
    smtp.quit() # Invokes smtp.close. This call should return : (221, '2.0.0 sscldinf.ryder.com closing connection')
    return SUCCESS
    
if __name__ == '__main__':
    import sys
    import common.log4py as log4py
    log = log4py.Logger().get_instance()
    os.environ['LOG_LEVEL'] = 'DEBUG'
    fn = '/home/infa/.emacs'
    usr = 'ernesto_ocampo@ryder.com,eocampo2000hotmail.com,eocampo2000@hotmail.com'
    pg = '7862631103@mymetropcs.com'
#   rc=notify(1,usr, pg , 'TEST MAIL  -- LOCALHOST', 'THIS IS A TEST MSG WITH ATTACH',False, True,'infa@ryder.com',  log,  files=[fn,])
    rc=notify(1,usr, pg , 'TEST MAIL  -- LOCALHOST', 'THIS IS A TEST MSG WITH ATTACH===',False, False,'infa@ryder.com',  log,  files=[fn,])
    print "rc is %d " % rc
    sys.exit(rc)
