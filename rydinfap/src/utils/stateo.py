'''
Created on Oct 7, 2012

@author: eocampo
'''

import math
from collections import Counter

# data list of numbers to get average
# per contract caller need to check for empty list !

def getMean(data): return sum(data)/len(data) * 1.00

def getMin(data) : return min(data)

def getMax(data) : return max(data)

# data = [1,1,4,4,4,5,5,6,6,6,6,6,7,7]
# d.most_common()  = [(6, 5), (4, 3), (1, 2), (5, 2), (7, 2)]
# d.most_common(1) = [(6, 5)]
def getMode(data,s=True):
    d = Counter(data)
    if s == True : return d.most_common(1)
    else         : return d.most_common()

def getMedian(data):
    srtd = sorted(data) 
    mid = len(data)/2   
    if len(data) % 2 == 0:  # take the avg of middle two        
        return (srtd[mid-1] + srtd[mid]) / 2.0
    else:         
        return srtd[mid] 

def getVariance(data):
    var = -1
    mean =  getMean(data)   
    s    = 0
    for value in data:
        s += (value - mean)**2
    
    try:
        var = s/(len(data) - 1)
    except ZeroDivisionError: 
        print('ZeroDivisionError')
    finally: return(var)

def getStdDev(data): 
    var = getVariance(data)
    if var > 0 : var = math.sqrt(var) 
    return var

# sd Standar Deviation 
# mean
# f 
def getDist(sd,mean,f=1):
    return (mean - sd * f , mean + sd * f )

# EO Test this routine.    
# This fx will return outliners based on range criteria over the mean !
# rs It is a result set based on date, amt
# method returns a date, amount in order to hilite outliners
def getOutliner(data,mean,per):
    outL = []
    p    = ( 1.00 - (per/100.00) ) / 2
    mx =  mean + mean * p 
    mn =  mean - mean * p  
    print( ("pr = %f per %s bdrMax = %f bdrMin = %f" % (p,per,mx,mn)))
    for n in data:
        if n < mn or n > mx:
            outL.append('%f\n' % n)
    return outL
     