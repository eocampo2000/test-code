'''
Created on Oct 7, 2012

@author: eocampo
This module will render resource utilization.  

'''
import sys 
#import mainglob as mg                 # DO not comment
import common.log4py   as log4py 
import utils.fileutils as fu
import utils.strutils  as su
import utils.stateo    as st
import bean.renderbean as rb
from common.loghdl import getLogHandler


#Get the Logname
log     =  log4py.Logger().get_instance()

# This method gets the IO Transfer rate, based on the following file pattern:
#10052012:15:10:Started 2496528+0 records in
#2496528+0 records out
#real       46.3
#user       14.1
#sys        18.4
#10052012:15:11:Completed Transfer
#10052012:15:11:Remove File /saswork/io_test_transfers/vehicle_monthly_cost.sas7b dat rc=0
# It will return an array of IORateTransBean
def setIORateTransBean(l):
    tb = rb.IORateTransBean()
    

#def getIORateIter():
#    cdata = fu.readFile(fnp)
#    lineIter= iter( cdata.splitlines() ) 
#    for line in lineIter:   
#        if exp.match(line):
#                  for count in range(5):
#                              line = lineIter.next()  
#                              print line 

def getIORateTrans(fnp):
    real =[]; usr = []; syst=[]
     
    data = fu.readFile(fnp)
    if len(data) < 5 : return 1
    lines = data.split('\n')
    i = 0 
    for l in lines:
        i+=1
        if l.strip() == '' : continue
        if   l.startswith('real'): 
                print 'real %s'  % l[5:].strip()
                n = su.convToSecs(l[5:].strip())
                if n is None : print "Error invalid int on line %d " % i
                else         : real.append(n)
        elif l.startswith('user'): 
                print 'user %s'  % l[5:].strip()
                n = su.convToSecs(l[5:].strip())
                if n is None : print "Error invalid int on line %d " % i
                else         : usr.append(n)
        elif l.startswith('sys') :
                print 'sys %s'  % l[5:].strip() 
                n = su.convToSecs(l[5:].strip())
                if n is None : print "Error invalid int on line %d " % i
                else         : syst.append(n)
        
    return  real, usr,syst
    
def main(argv):
    
    if len(argv) > 1 :
        logFile = getLogHandler(argv[1])
        
    else:
        log.error( "USAGE : <%s> fx [args] Not enough arguments " % argv[0])
        sys.exit(1)
    
    

    
if __name__ == '__main__':
    #rc=main(sys.argv)
    lst = []
    lst = getIORateTrans(r'C:\SAS\IO_TEST\transf_data.txt')
    #lst = getIORateTrans(r'C:\SAS\IO_TEST\transf_data256k.txt')
    # r,u,s
    for r in lst:
        min=st.getMin(r); max=st.getMax(r); mean = st.getMean(r); sd = st.getStdDev(r)
        print "elements = %d min =%s max=%s  median=%s  mode = %s mean=%s  range=%s var= %s stddev = %s 1S=%s "  % (len(r),min, max, st.getMedian(r),st.getMode(r),round(mean,2),max-min,st.getVariance(r),round(sd,2), st.getDist(sd, mean))
        print "USER =", r