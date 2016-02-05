'''
Created on Jan 3, 2014

@author: eocampo

This class is to process predecessors

At this point predecessors can be :
INFA
MSSQL

INPUT OF THE FORM :

pred1 = "[['Infa','wkf1','wkf2','wkf4'], ['MSSQL','job1','job2''job3','job4'], ['Infa','wkf3']]"
Will return an array with predecessors methods that need to be invoked.

'''
__version__ = '20140104'

import sys

# Please add 
predmethod = {"Infadly"    : "chkInfaRdyDly",
              "Infamthly"  : "chkInfaRdyMthly",
              "Mssqldly"   : "chkMssqlRdyDly",
              "Mssqlmthly" : "chkMssqlRdyMthly",          
             }


# Will create predecessor methods.
def _crtMethods(pr,log):
    pmet = []
    log.debug("pr is ", pr)
    if len(pr) < 2 : return pmet
    
    mn = pr[0].capitalize()
    met = predmethod.get(mn)
    if met is None : return pmet
    log.debug("met is ", met)
    for e in pr[1:] : 
        pmet.append("%s('%s')" % (met,e))
    return pmet
   
# Returns a list w/predecessor order.    
def getPred(pred,log):
    pa = [] ; pmetEle = []
    try:
        pa = eval(pred)
        if(type(pa) != list) or len(pa) < 1: 
            log.error ("Need to be a list of at least 1 element !")
            log.debug("type(pa)" , type(pa), " len(pa) = " ,len(pa))
            return [] 
        
        print "AFTER TYPE pa =", pa
        for prds in pa:
            pmet =   _crtMethods(prds,log)   
            log.debug("pmet = ",pmet)
            if len(pmet) < 1 : return []
            else : pmetEle.extend(pmet)
            log.debug(" _crtMethods(prds) "   ,    pmetEle)  
               
    except:  
        log.error("EXCEP %s %s "  % (sys.exc_type, sys.exc_info()[1]))
      
    finally : return pmetEle
    
if __name__ == "__main__":
    
    pred1 = "[['Infa','wkf1','wkf2','wkf4'], ['MSSQL','job1','job2','job3','job4'], ['Infa','wkf3']]"
    p = getPred(pred1)
    print ("len p = %d" % len(p))
    
    print "P " , p
    for e in range(len(p)):
        print "pop %s " % p.pop()
        print "---p = ", p 
        
    print "END ---p = ", p 
#    pr = ['Infa', 'wkf1', 'wkf2', 'wkf4']
#    r = _crtMethods(pr)
#    print "\n\n\r = ", r
    