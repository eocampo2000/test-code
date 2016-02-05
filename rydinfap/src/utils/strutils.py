'''
Created on May 18, 2012

@author: eocampo

Added functions to support job run time.
Added functions to support decoding.
Added functions to support Month date arithmetic
Fixed issue w/add month
20140213 Added remove spaces from string.
20140917 Added function for ts difference.
20150615 Added isBlank function. 
'''
__version__ = '20150615'
import re
import time
import base64
from datetime import datetime, timedelta

DL = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun',]

def findStr(msg,stf):

    retVal = -1
    if (msg is None or len(msg) < 2) : return retVal
    fstr = re.compile(r"%s" % stf, re.IGNORECASE)
    resp = re.findall(fstr,msg)
    if resp: retVal = 0        
    return retVal

def isBlank(s):
    if s and s.strip(): return False
    else              : return True
    
def isValidDate(date,fmt='%m/%d/%Y'):
    try:
        time.strptime(date,fmt)
        return True
    except ValueError:
        return False

# returns a date string based on fmt mask.
def getTodayDtStr(fmt='%m/%d/%Y'):
    d=datetime.now() 
    return d.strftime(fmt)

# returns a date string based on fmt mask.
# dto datetime object
def getDtStr(dto,fmt='%m/%d/%Y'): return dto.strftime(fmt)


def toInt(s):
    if type(s) is str: s=s.rstrip()
    try    : 
        i = int(s)
        return i

    except ValueError : return None
    except TypeError  : return None
    
def toLong(s):
    if type(s) is str: s=s.rstrip()
    try    : 
        i = long(s)
        return i

    except ValueError : return None
    except TypeError  : return None

# Note returning -1 in this context is OK !
def toIntPos(s):
    if type(s) is str: s=s.rstrip()
    try    : 
        i = int(s)
        return i
    except ValueError : return -1
    except TypeError  : return -1
     
def toFloat(s):
    if type(s) is str: s=s.rstrip()
    try    : 
        i = float(s)
        return i

    except ValueError : return None
    except TypeError  : return None    

# Specific HH:MM
def toSec(st,sep=':'):
    if len(st) != 5 : return -2
    t = st.split(sep)
    h = toInt(t[0]); m = toInt(t[1])  
    if h is None or m is None : return -3
    return h*3600 + m*60

def toTimeStamp(sTS,fmt='%Y-%m-%d %H:%M:%S'):
    if isValidDate(sTS,fmt) : return datetime.strptime(sTS, fmt)
    else                    : return None
        
# returs a str date dy 
# dy number of days to subtract. (int)
# fd  base date to compare (string).
def getDayMinusStr(dy,fd, fmt='%m/%d/%Y'):
    if isValidDate(fd,fmt) : bd = datetime.strptime(fd, fmt)
    else                   : bd = datetime.now()
    d= bd - timedelta(days= dy)
    return d.strftime(fmt)

# returs a str date dy 
# dy number of days to add.
# fd  base date to compare (string).
def getDayPlusStr(dy,fd, fmt='%m/%d/%Y'):
    if isValidDate(fd,fmt) : bd = datetime.strptime(fd, fmt)
    d= bd + timedelta(days= dy)
    return d.strftime(fmt)

def getTimeSTamp(fmt='%d%02d%02d-%02d%02d'):
    d=datetime.now()
    return ( fmt % (d.year, d.month, d.day,d.hour,d.minute))

# If dt is invalid it will return current's date day
def getDayofWeek(dt,fmt='%m/%d/%Y'):   
 
    if isValidDate(dt,fmt): mdate = datetime.strptime(dt,fmt)
    else                  : mdate = datetime.now()
        
    dayOfWk = mdate.weekday()
    rc = DL[dayOfWk]
    
    return rc   

# Method to add one month
# returs a str date dy 
# mth number of days to add.
# fd  base date to compare (string).

def getMonthPlusStr(mth,fd, fmt='%m/%d/%Y'):
    dt = addmonths(fd,mth,fmt)
    #dt = addmonths(fd,mth,False,fmt)
    if dt is None: return dt
    else         : return dt.strftime(fmt)   
        
# cdate current date to apply date arithmetci
# month integer with number of months to add/subtract 
# eom to retunr end of month date.

def addmonths(cdate,months,fmt='%m/%d/%Y'):
    try:    
        
        if isValidDate(cdate,fmt) : 
            cur_dt = datetime.strptime(cdate, fmt)
        
        else : return None
       
        targetmonths = months+cur_dt.month
        if targetmonths%12 == 0:   
            targetmonth = 12
        else:   
            targetmonth = targetmonths%12   

        tgt_dt = cur_dt.replace(year=cur_dt.year+int((targetmonths)/12),month=(targetmonth%12+1),day=1)
        tgt_dt +=timedelta(days=-1)   
        return tgt_dt  
    except:   
        # There is an exception if the day of the month we're in does not exist in the target month   
        # Go to the FIRST of the month AFTER, then go back one day.   
        tgt_dt = cur_dt.replace(year=cur_dt.year+int((targetmonths)/12),month=(targetmonth%12+1),day=1)   
        tgt_dt +=timedelta(days=-1)    
        return tgt_dt 



def getTodaySec():
    t = getTodayDtStr("%H:%M")
    return toSec(t)

    
#    h = int('%s' % ct.hour) * 3600; m = int('%s' % ct.min) * 60
#    return h + m

# This function converts the following format to seconds:
# HH:MM:SS.sss  
def convToSecs(st,sep=':'):
    if len(st) < 1 : return None
    
    t = st.split(sep)
    
    if   len(t) == 1 : return toFloat(st)   # secs s  = 52.1
    elif len(t) == 2 :                      # MM:SS.s = 3:52.3
        m = toInt(t[0]); s = toFloat(t[1])
        if m is None or s is None : return None
        return m * 60 + s
    elif len(t) == 3 :                      # HHMM:SS.s = 20:13:52.3
        h= toInt(t[0]); m = toInt(t[1]); s = toFloat(t[2])
        if h is None or m is None or s is None : return None
        return h*3600 + m * 60 + s
    else : return None       

# This fx gets 2 strings to subtract. It return diff in seconds.
def ts_diff(ts1,ts2,fmt='%Y-%m-%d %H:%M:%S'):
    d1 = toTimeStamp(ts1,fmt)
    d2 = toTimeStamp(ts2,fmt)
    
    if d1 is None or d2 is None : return None
    diff = d1 - d2
    return diff.total_seconds()
    
# rsp : Message to display the User
# ersp: Expected response.
#
# function is not case sensitive.
def getResponse(rsp,ersp):
    ur = raw_input(rsp)
    if ersp.lower() == ur.lower() : return True
    return False

def remSpacefrStr(s): return ''.join(s.split())
    

def dec64(st):
    
    try:
        r = base64.b64decode(st)
        tok = r.split(':',1)
        if len(tok) != 2 : return [-1,]        # Invalid string
        return tok
    
    except:
        return [-2,]                           # Tampered/Corrupted
        
        
if __name__ == '__main__':

    #dts = ('122012','012013','022013','032013','042013','052013','062013','072013','082013','092013','102013','112013','122013','012014','122015', '012016','022016','032016')
    #dts = ('12012012','01312013','02012013','03312013','04282013','05152013','06162013','07172013','08202013','09252013','10302013','11292013','12312013','01012014',)
    #rc = getResponse('Please Enter Yes to Continue, any other key to exit.\n','Yes' )
    #rc = dec64('c3NjbGRpbmYucnlkZXIuY29tOndlbGNvbWU=')
    #print "Response is " , rc
    #ts = getTimeSTamp()
    #print 'TS = %s' % ts

    
#    print " '1234' %d " % toInt('1234')
#    print "1234 %d " % toInt(1234)
#    print "1234L %d " % toInt(1234L)
#    print "123Y  " , toInt('123Y')
#     d  = getTodayDtStr('%m/%d/%Y %H:%M:%S')
#     d = getTodayDtStr('%m_%d_%Y')
#    print "date is %s" % d
#    for md in range(-1,35):
#        dt = '201207%d' % md
#        d = getDayofWeek(dt,'%Y%m%d')
#        print 'day %d of week %s ' % (md,d)
#        dy =30; fd ='20120909'
#        d = getDayPlusStr(dy,fd, fmt='%Y%m%d')
#        print "date is %s " % d
#        d = getDayofWeek('')
#        print 'day of week %s ' % d
#    
#    rc = findStr('Hello World Received','Received')
#    print "rc " , rc
#    fmt = '%Y%m'
#    rc = isValidDate('199813',fmt)
#    print "rc for '199813' ", rc

#    m='01/31/2013'
#    rc = addmonths(m, 1)
#    print "curr_m = %s next_m = %s" % (m,rc)
##    rc = addmonths('201212', 1, True,'%Y%m')
##    rc = addmonths('201212', 1, True,'%Y%m%d')
#    for dt in dts:   
#       #rc = getMonthPlusStr(1,dt,'%m%d%Y')
#       rc = getMonthPlusStr(1,dt,'%m%d%Y')
#       print "currdt = %s tgt date %s \n\n" % (dt,rc)
#    rc = isValidDate('19982402',fmt)
#    print "rc for '19982402' ", rc
#     rc = getTodaySec()
    s = '  a string  with multiple    white          spaces  '
    s = ''
    s = None
    r = remSpacefrStr(s)
    print "s is %s\nr is %s" % (s,r)