'''
Created on Aug 12, 2012

@author: eocampo
'''
'''
Created on Aug 12, 2012

This module is to get __version__ for existing code.
@author: eocampo
20140204 : Added procjobs and save to file capability.
'''
import os.path
import glob
import sys
import hashlib
import time

import utils.fileutils as fu
import utils.strutils  as su

__version__ = '20140204'
USAGE       = ' : %s <base_path> <pack_name> | <base_path>' % os.path.basename(__file__)

modNames = ['apps','bean','cmd','common','datastore','proc','procdata','procjobs','render','standalone','utils',]
# mn module name

if sys.platform != 'win32' :
    fn = '/tmp/getver%s.csv' % su.getTimeSTamp()
else:
    fn = r'C:/tmp/getver%s.csv' % su.getTimeSTamp()


# This method gets the module version information.
# mn : 'utils.filetransf'
def _getVer(mn):
    __version__ = '00000000'
    #from utils.filetransf import __version__
    try:
        exec 'from %s import __version__' % mn
        return  __version__
    except ImportError:    
        return  __version__

def _getHash(fn):
    #print "_getHash(fn) = %s" % fn 
    md5 = hashlib.md5() 
    with open(fn,'rb') as f:      
        for chunk in iter(lambda: f.read(8192), b''):
            md5.update(chunk) 
            
    return md5.hexdigest() 

# base_path = PYTHONPATH for base
# pack_name = package name relative to base_path
def getModVer(base_path,pack_name):
    
    modlst = []
    if not os.path.isdir(base_path) :
        print 'Error base_path %s does not exists' % base_path
        sys.exit(1)    
    
    dir = '%s/%s' % (base_path,pack_name)
    if not os.path.isdir(dir):
        print 'Error module %s does not exists' % pack_name
        return 1
     
    fl = glob.glob('%s/*.pyc' % (dir))
    for f in fl:
        code_file = os.path.basename(f)
        mod_name  = os.path.splitext(code_file)[0]
        if mod_name == '__init__' : continue
        ver = _getVer('%s.%s' % (pack_name,mod_name))
        hv  = _getHash(f)
        mt  = time.strftime("%Y%m%d %I:%M:%S %p",time.localtime(os.path.getmtime(f)))
        sz  = os.path.getsize(f)
        ln  = "%s,%s,%d,%s,%s"  % (mod_name, ver,sz,mt,hv)
        modlst.append(ln)
        #print "%s\t%s\t%d\t%s\t%s"  % (mod_name, ver,sz,mt,hv)
        
    return sorted(modlst)
    
def getCodeVer(base_path):
    verd = {}    
    for m in modNames:
        ml = getModVer(base_path,m)
        verd['%s'% m] = ml
        
    return verd    

# This method will create a CSV file.
# dm dictionary containing mods.
def createCSV(dm):
    data = []
    dks = sorted(dm.keys())
    for k in dks:
        data.append(' %s,\tLabel,\tBytes,\tTimestamp,\tChecksum\n' % k)
        lst = dm[k]
        for m in lst:
            data.append('%s\n' % m)
        
    return (fu.createFile(fn,data))   
#    getModVer(r'C:\Users\eocampo\workspace\rydinfap\src','utils')
#    getCodeVer(r'C:\Users\eocampo\workspace\rydinfap\src')
def main(argv):
    if len(argv) == 2 :
        print 'Involing getCodeVer <%s>' % argv[1]
        dm = getCodeVer(argv[1])
        rc = createCSV(dm)
        print "file creation %s rc = %s" % (fn,rc)
    elif len(argv) == 3 :
        print 'Involing getModeVer <%s> <%s>' % ( argv[1], argv[2])
        dm = getModVer(argv[1],argv[2])
        rc = createCSV(dm)        
        print "file creation %s rc = %s" % (fn,rc)
    else:
        print "Invalid number of args %d USAGE %s" % (len(argv),USAGE)
        sys.exit(1)
                               
if __name__ == '__main__':
    rc=main(sys.argv)