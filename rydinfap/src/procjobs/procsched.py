'''
Created on Jun 14, 2013

@author: eocampo

This module is used to calculate next run for schedude jobs that need to have a particular run sequence.
Lowest level of granularity is once a day. 
# Monday is 0 and Sunday is 6

Fixed Custom Schedule 
'''
__version__ = '20130820'


from datetime import datetime
import utils.strutils as su 

errmsg = ('Success ',                                     # 0 
          'Invalid Date !',                               # 1
          'Last Day Ran Sat or Sund. Probably Recover',   # 2
          'No days have been set to run',                 # 3
          'Invalid Day specified',                        # 4
          'Undefined Schedule Freq',                      # 5
          'Last Day Ran was not sched. Probably Recover', # 6
          'Last Month Ran was not sched. Probably Recover', # 7
       )

errLen = len(errmsg)

custDay = {'Mon':0, 'Tue':1, 'Wed':2, 'Thu':3, 'Fri':4, 'Sat':5, 'Sun':6}


# Runs everyday.
# pd previous run date
# returns next day run  
def _getDlyNextRunDate(pd,sch) : return su.getDayPlusStr(1, pd, '%Y%m%d')

# Weekdays run only Mon thru Friday (0 to 4) 
# pd previous run date
# It will return nrd of the form YYYYMMDD if pd is a weekday, otherwise will return -1
def _getWDayNextRunDate(pd,sch):
    mdate  = datetime.strptime(pd,'%Y%m%d')
    dow    = mdate.weekday()     
    
    if dow > 4    : return   2    # Last ran date is Sat or Sun. Probably a recovery.

    if   dow == 4 : nxtDay = 3    # Friday
    else          : nxtDay = 1    
    
    nrd = su.getDayPlusStr(nxtDay, pd, '%Y%m%d')
 
    return nrd

# Runs once a week    
# pw previous week ran
# It will return 
def _getWklyNextRunDate(pd,sch)  : return su.getDayPlusStr(7, pd, '%Y%m%d')
   
# pd previous run date 
def _getMthlyNextRunDate(pd,sch) : return su.getMonthPlusStr(1, pd, '%Y%m')
    

# This function runs 'Custom Schedules'
#sch is a list for days to run Mon to Sun
# If invalid day return error.
def _getCustNextRunDate(pd,sch):
    nxtDay = -1
    if len(sch) < 1 : return 3
    
    sd = []
    for s in sch:
        if custDay.has_key(s) :
            sd.append(custDay.get(s)) 
        else : return 4
        
    sd.sort()
    
    mdate  = datetime.strptime(pd,'%Y%m%d')
    dow    = mdate.weekday() 
    
    slen = len(sd)
    if slen == 1 : return _getWklyNextRunDate(pd,sch) # Weekly run
    if slen == 7 : return _getDlyNextRunDate(pd,sch)  # daily run 
    
    # sched has 2 ot 6 elements.
    tail = sd[slen -1]
    
    print "tail = %s" % (tail)
    
    i = 0
    for s in sd:
        if s == dow:
            if s == tail : nxtDay = (7 - sd[i] + sd[0])
            else         : nxtDay = (sd[i+1] - s )
            break 
        i+=1       
     
    print "dow = %s s = %s  tail = %s i = %d slen = %s ==nxtDay = %s " % (dow,s,tail,i,slen,nxtDay)   
    if nxtDay != -1  :  return su.getDayPlusStr(nxtDay, pd, '%Y%m%d')   
    else             :  return 6
         
            
# pd  previous run date.  Format YYYYMMDD
# cd  current date.       Format YYYYMMDD     
# scf schedule freq valid 
# log handler.
# rssched for custom schedules. List of days to run. Sort them 
def getNextRunDate(pd,cd,scf, log, sch= []):
    rc = -1
    log.debug ('pd = %s ,cd = %s,scf = %s' % (pd,cd,scf))
    if scf == 'Mthly'  : r = su.isValidDate(pd,'%Y%m')
    else               : r = su.isValidDate(pd,'%Y%m%d')
    if r is False : 
        log.error("prevDayRun = %s is invalid !" % pd)
        return 1
    try:
        nrd = eval('_get%sNextRunDate(pd,sch)' % (scf))
        log.debug('_get%sNextRunDate ret = %s ' % (scf,nrd))
        if nrd == cd :  return 0
        log.debug("errLen = %d " % errLen)
        if nrd < errLen : log.error("%s" % errmsg[nrd])
        else            : log.error("Invalid Run date. Previous Ran date = %s . Next Ran Date should be %s and not %s"  % (pd,nrd,cd))       
    
    except NameError:  
        log.error("Invalid Schedule Frequency %s " % scf)
        rc = 10
    
    return rc