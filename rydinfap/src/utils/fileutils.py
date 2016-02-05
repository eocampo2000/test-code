'''
Created on Feb 22, 2011

@author: emo0uzx

Added FX to check/remove for blank lines from a file.
Addes FX to serialize objects.
Added ModDate method
20131104 Added Write CSV file.
20131120 Added getFileLine, remFileLine, ConcatFile and other file related functions.
20150311 Added Methods to open URL
20150527 Get oldest/newest file in dir.
'''
__version__ = '20150527'

# DOES NOT contain any OS specific commands
import time, sys, os
import os.path
import re
import mmap
import filecmp
import glob
import shutil
import zipfile
import tarfile
import pickle
import csv
import urllib2

success ='SUCCESS' 
#BEG_OF_FILE  = 0 os.SEEK_SET
#CUR_POS_FILE = 1 os.SEEK_CUR
#END_OF_FILE  = 2 os.SEEK_END
STABLE_INTERVAL = 60*1
#TINY_FILE_SIZE  = 1024*10  # 10K
TINY_FILE_SIZE  = 1024*100  # 100K
#TINY_FILE_SIZE = 100                     # Use for TEsting only

def getPIDFileName(fn): return '%s%s' % (fn,os.getpid())
# suf '' Default to all files.
# return a list of files within a given dir
def getDirFiles (path,suf=''):
    return [f for f in os.listdir(path) if f.endswith(suf)]

def getNewestFile(path,fn):
    #return min(glob.iglob('%s/%s' % (path,fn)), key=os.path.getctime)
    fnp = glob.glob('%s/%s' % (path,fn))
    if len(fnp) == 0 : return None
    return max (fnp, key=os.path.getmtime)

def getOldestFile(path,fn):
    #return max(glob.iglob('%s/%s' % (path,fn)), key=os.path.getctime)
    fnp = glob.glob('%s/%s' % (path,fn))
    if len(fnp) == 0 : return None
    return min(fnp, key=os.path.getmtime)
 
# Returns boolean
def isDir(fn):
    return os.path.isdir(fn)

# Returns boolean
def fileExists(fn):
    return os.path.exists(fn)
    
# Returns number of seconds since file was modified.
def chkFileModTime(fn):
    return (time.time() - os.stat(fn)[8])

# fmt "%m/%d/%Y %I:%M:%S %p" = 01/25/2003 11:38:30 AM
def chkFileModDate(fn,fmt='%m/%d/%Y'):
    return time.strftime(fmt,time.localtime(os.path.getmtime(fn)))

def createDir(dpath):
    rc = 1
    try:
        if os.path.exists(dpath) : return 0
        rc = os.mkdir(dpath)
        rc = 0
        
    except IOError, (errno,strerror):
        print ("IOError : could not create dir %s Error %s StrErr %s" % (dpath,errno,strerror ))
    
    except:
        print ("Except : could not create dir %s Error %s StrErr %s" % (dpath,sys.exc_type, sys.exc_value ))
    
    return rc

def createDirs(dpath):
    rc = 1
    try:
        if os.path.exists(dpath) : return 0
        rc = os.makedirs(dpath)
        rc = 0
        
    except IOError, (errno,strerror):
        print ("IOError : could not create dir %s Error %s StrErr %s" % (dpath,errno,strerror ))
    
    except:
        print ("Except : could not create dir %s Error %s StrErr %s" % (dpath,sys.exc_type, sys.exc_value ))
    
    return rc      

# dir directory to look for filr
# refn regular exp filename 
def getFileName(d,refn):
    fn = []
    
    for name in glob.glob('%s/%s' % (d,refn)):
        fn.append(name)
    
    return fn

def getFileBaseName(fn): return os.path.basename(fn) 

def getFileDirPath(fn) : return os.path.dirname(fn)

def crtEmpFile(fn):
    f = openFile(fn,mode='w')
    if f is None : return False
    closeFile(f)
    return os.path.exists(fn)

# Overwrites exiting file
def updFile(fn, data): return createFile(fn,data)
    
def openFile(fn,mode='r+'):
      
    f = None
    
    try:
        f = open(fn,mode)
      
    except IOError, (errno,strerror):
        print ("Error openFile: could not open %s Error %s StrErr %s" % (fn,errno,strerror ))
      
    finally:
        return f   
  
def closeFile(f):
    if(f != None):
        f.close()



# This method extracts data from specified URL and writes a file
# URL to open. 
# df  path to target data file.
def get_url_data(url, df):
   
    try:
        req = urllib2.Request(url)
        fdata = urllib2.urlopen(req)
        f = openFile(df,'w')
        if f is None : 
            return 'Could not Open %s' % df
        f.writelines(fdata.readlines())
        closeFile(f)
        return success
    
    except urllib2.HTTPError, e:
        return 'HTTPError = ' + str(e.code)
         
    except urllib2.URLError, e:
        return 'URLError = ' + str(e.reason)
    
    except:
        return " get_url_data : %s %s " % (sys.exc_type, sys.exc_info()[1])
    
    
# This method returns a row in the file.
# fn File name 
# ln line number to return.
def getFileLine(fn,ln):
    f = openFile(fn)
    if f is None : return None
    i=1; rc = None
    for line in f:
        if ln == i : 
            rc = line
            break
        i += 1    
        
    f.close()     
    return rc

def remFileLine(filePath,ln):
    fileSize=os.stat(filePath)[6]
    if (fileSize>TINY_FILE_SIZE):        
        return _remBigFileLine(filePath,ln)
    else:
        return _remSmallFileLine(filePath,ln)

def _remSmallFileLine(src,ln):
    print "IN _remSmallFileLine filesize %d for %s " % (os.stat(src)[6],src)
    rc = 1
    try:   
        fi       = open(src,'r+')
        lines    = fi.readlines()
        fi.close()
        lines[:] = lines[ln:]
        fi      = open(src,'w')
        fi.writelines(lines)
        fi.close() 
        rc = 0
    except IOError:
        print "Error opening %s : %s %s " % (fi,sys.exc_type, sys.exc_info()[1])
      
    except:
        print "remFileLine:Error : %s %s " % (sys.exc_type, sys.exc_info()[1])
      
    finally:
        if fi : fi.close()    
        return rc    
# This function removes lines from a file. Use for big files
# src source  filename
# ln lines to remove : Positive remove from beginning, negative remove from end, 
def _remBigFileLine(src,ln):
    print "IN _remBigFileLine filesize %d for %s " % (os.stat(src)[6],src)
    rc = 1; i = 0
    tmpf = '%s/%s.tmp' % (os.path.dirname(src),os.path.basename(src)) 
    try:
        fi = open(src,'r+')
        fo = open(tmpf,'w')
        while i < ln : 
            fi.readline()
            i+=1
            
        #fi.readlines()[ln]    
        for line in fi:
            fo.write(line) 
        
        fo.close();fi.close()
        rc = moveFile(tmpf,src)   
                
    except IOError:
        print "Error opening %s or %s : %s %s " % (fi, fo,sys.exc_type, sys.exc_info()[1])
      
    except:
        print "remFileLine:Error : %s %s " % (sys.exc_type, sys.exc_info()[1])
      
    finally:
        if fo : fo.close()
        if fi : fi.close()
        
        return rc    
    
# Get Number of Lines in a file.      
def getLines(fn): 
    f = openFile(fn,"r+") 
    if f is None : return ''
    buf = mmap.mmap(f.fileno(), 0) 
    lines = 0 
    readline = buf.readline 
    while readline(): 
        lines += 1      
    closeFile(f)
    return lines

def getFilseSize(fn): return (os.path.getsize(fn))

def readFile(fn):
    f   = None
    mstr = ''
    try:
        f = open(fn,'r')
        mstr = f.read()
    except:
        print "Error opening %s : %s %s " % (fn, sys.exc_type, sys.exc_value)
    finally:
        if f : closeFile(f)
        return mstr    

# This method excludes blanklines
def readFileLst(fn):
    lines = []
    try:
        f = open(fn,'r')
        lines = f.readlines()
    except:
        print "Error opening %s : %s %s " % (fn, sys.exc_type, sys.exc_value)
    finally:
        if f : closeFile(f)
        return lines 
    
# convenience function to make a file. Data can be a list or a string.
def createFile(fileName, data):
    
    retVal = -1
    try:  
        f = open(fileName, "w")

        import types
        if isinstance(data, types.ListType):
            f.writelines(data)
        else:
            f.write(data)

        f.flush(); f.close()
        retVal = 0
        
    except IOError, (errno,strerror):
        msg  =  "I/O error (%s) %s Redirecting output to stderr" %(errno,strerror) 
        msg +=  "FileName %s could not be created " % fileName
        print "%s" % msg

    except:
        print "==EXCEP %s %s" % (sys.exc_type,sys.exc_value)
    finally: 
        return retVal

#def createFile(fileName, data):
#    
#        retVal = -1
#          
#        print 'CREATE FILE DATA == ', data, ' TYPE ', type(data)
#        f = open(fileName, "w")
#
#        import types
#        if isinstance(data, types.ListType):
#            print  "I AM IN IF "
#            f.writelines(data)
#        else:
#            print "I AM IN ELSE"
#            f.write('%s' % data)
#
#        f.flush(); f.close()
#        retVal = 0
#        
#        return retVal

# This method remove all blank lines for a give file
#  src -> Original File
#  Returns a list with no blank lines.
def remBlankLineLst(src):
    lines = []
    
    if not fileExists(src) : return lines
    
    with open(src) as f_in:     
        lines = filter(None, (line.rstrip() for line in f_in))     
    
    return lines 

def remBlankLineFile(src,tgt=None):
    rc = -1 ; fo = None ; fi = None
    try:
        if tgt is None  :        
            ftmp = '%s/remBlankLineFile_%s.tmp' % (os.path.dirname(src), getPIDFileName(os.path.basename(src)))
            fn = src
        else           : ftmp = '%s/remBlankLineFile_%s.tmp' % (os.path.dirname(tgt), getPIDFileName(os.path.basename(tgt)))   

        fi = open(src ,'r+')
        fo = open(ftmp,'a')
        
        for line in fi:
            if line.strip(): fo.write(line)
        fo.close(); fi.close()      
        rc = 0
        
    except IOError      : 
        print "remBlankLineFile %s " % sys.exc_info()[1]
        rc = 1
    except shutil.Error : 
        print "remBlankLineFile %s " % sys.exc_info()[1]
        rc = 2
    except  : 
        print "remBlankLineFile %s " % sys.exc_info()[1]
        rc = 3
    finally:
        if fo is not None : 
            fo.close()
            if rc == 0 : moveFile(ftmp,fn) 
            else       : delFile(ftmp)     
        return rc   
#    with open(src,"r") as f:
#        lines=f.readlines()
#
#    with open(tgt,"w") as f:  
#        [f.write(line) for line in lines if line.strip() ]
#        #f.writelines(line for line in lines if line.strip()) other approach

# This method remove all blank lines for a give file
# and creates a file with no blank lines.
#  src -> Original File
#  tgt -> Target File with no blanks, when None no target file will be created.
#  Returns number of lines (non-blank data)  or -1 if Error.
def remBlankLines(src,tgt = None):
    rc    = -1
    
    if not fileExists(src) : return rc

    lines = remBlankLineLst(src)
    dl = len(lines)
    if dl > 0 :
        rc = dl
        if tgt is not None:
            lst = []
            for ln in lines: lst.append( ln + '\n')
            r = createFile(tgt,lst)
            if r != 0 : rc = -1
          
    return rc
                
def copyFile(src,tgt):
    try:    
        shutil.copy (src, tgt)
        if os.path.isfile (tgt): return 0
        else                   : return 1
    except IOError: return 1

def moveFile(src,tgt):
    try:
        shutil.move (src, tgt)
        if os.path.isfile (tgt): return 0
        else                   : return 1
    except IOError: return 1
    except shutil.Error : return 2

def delFile(fn):
    try:
        os.remove(fn)
        return 0
    except : return 1

# This method will concatenate a list of files, if fn is not provided then will append to first file.
# fl File list to concatenate
# fn File name to concatenate 
# rmbl remove blank lines flag 
def concatFile(fl,fn=None,rmbl=False):
    rc = -1 ; fo = None ; fi = None  
    if len(fl) < 2 : return 1
    
    try:
        if fn is None  :        
            ftmp = '%s/concatFile_%s.tmp' % (os.path.dirname(fl[0]), getPIDFileName(os.path.basename(fl[0])))
            fn = fl[0]
        else           : ftmp = '%s/concatFile_%s.tmp' % (os.path.dirname(fn), getPIDFileName(os.path.basename(fn)))   

        fo = open(ftmp,'a')
        
        if rmbl == True : 
            for f in fl:
                rc = remBlankLineFile(f)
                if rc != 0 : return rc
               
        for f in fl:
            fi = open(f,'r') 
            shutil.copyfileobj(fi,fo) 
            fi.close()         
        rc = 0
        
    except IOError      : 
        print "concatFile %s " % sys.exc_info()[1]
        rc = 1
    except shutil.Error : 
        print "concatFile %s " % sys.exc_info()[1]
        rc = 2
    except  : 
        print "concatFile %s " % sys.exc_info()[1]
        rc = 3

    finally:
        if fi is not None : fi.close()
        if fo is not None : 
            fo.close()
            if rc == 0 : moveFile(ftmp,fn) 
            else       : delFile(ftmp)     
        return rc
    
# This function tars a file.
def tarFile(src_fld, dest_fld, log, comp='gz'):
    rc = 1
    
    if fileExists(src_fld) is False:                        # Do this check otherwise will produce s 0 bytes tar !
        log.error('File %s does not exist' % src_fld)
        return rc
   
    if comp:
        dest_ext = '.' + comp
    else:
        dest_ext = ''
    
    arcname   = os.path.basename(src_fld)
    dest_name = '%s.tar%s' % (arcname, dest_ext)
    dest_path = os.path.join(dest_fld, dest_name)
    
    log.info("arcname = %s dest_name = %s  dest_path = %s" % (arcname,dest_name, dest_path))
    
    if comp:
        dest_cmp = ':' + comp
    else:
        dest_cmp = ''
    try:
        out = tarfile.TarFile.open(dest_path, 'w'+dest_cmp)
        out.add(src_fld, arcname)
        out.close( )
        rc = 0
        
    except IOError:
        log.error('IO Issue %s %s ' % (sys.exc_type,sys.exc_value))
    
    except tarfile.CompressionError:
        log.error('Compression Issue %s %s ' % (sys.exc_type,sys.exc_value))
    
    except:
        log.error('Except %s %s ' % (sys.exc_type,sys.exc_value))
    return rc
 
 
#fzp archive name
#fn file to compress
def compressFile(fzp,fn):
    try:
        import zlib
        compression = zipfile.ZIP_DEFLATED
    except:
        compression = zipfile.ZIP_STORED
    
    modes = { zipfile.ZIP_DEFLATED: 'deflated',
              zipfile.ZIP_STORED:   'stored',
              }
    

    try:
        zf = zipfile.ZipFile(fzp, mode='w')
        zf.write(fn, compress_type=compression)
        rc = 0
    except : rc = 1
    
    finally:
        zf.close()
        return rc
        
# This function allows us to truncate the very last line in a file
# This is so we can remove the trailer from the feed data files.
# This method assumes that the file is not empty, so check before invoking
def getLastLine(filePath, truncate=0):
    fileSize=os.stat(filePath)[6]
    if (fileSize>TINY_FILE_SIZE):        
        return getLastLineBkwd(filePath, fileSize, truncate)
    else:
        return getLastLineFwd(filePath, truncate)

# For large files, read backwards; use seek.
def getLastLineBkwd(filePath, fileSize, truncate=0):  

    f = open(filePath, "r+")
    f.seek(0, os.SEEK_END) 

    # start at offset 2 to avoid the last 2 (possible newline and C-m chars)
    foundNewLine=0
    line = ""
    
    for i in xrange(2,fileSize):
        f.seek(-i, os.SEEK_END)
        c = f.read(1)

        if (c=="\n"):
            foundNewLine=1
        
            if (truncate):r = f.truncate(f.tell())
            break
        
        else:
            line = c + line # prepend chars since we"re going backwards
  
    if (not foundNewLine):
            print("Warning: only 1 line found in file: (%s)" % filePath, "WARN")
            if (truncate): f.truncate(f.tell())
    else:
        return line
  
    f.close()


# For smaller files, use this function to remove the last line; it
# is simpler and handles the case where the file might be empty better
# than getLastLineBkwd
def getLastLineFwd(filePath, truncate=0):  
    f = open(filePath, "r")
    lines = f.readlines()
    f.close()

    if (truncate):
        # utils.debugPrint( self.systemName, "found %s lines" % len(lines) )
        f = open(filePath, "w")
        f.writelines(lines[:-1])
        f.close()

    return lines[-1]


# This method will a CSV file
# fn  = filename 
# fld = number of fields per row
# Function returns an array of List.
def readCSV(fn,fld,sep=',',strp=' \t\n\r'):
    
    rows = []
    bad  = []
    f = openFile(fn,'r+')
    if (f == None):
        return (rows,bad)
    i = 0
    while(1):
        i+=1
        line = f.readline()
        if not line:break
        line = line.strip(strp)
        if len(line) == 0 : continue
        li   = line.split(sep)
        ln = len(li)
        if(ln == fld):
            rows.append(li) 
        else :
            if (len(line.strip()) != 0) : 
                bad.append('%d:(%d)> %s\n' % (i,ln,line))

    closeFile(f)
    return (rows,bad)

# data is a 2 dim array. 
def writeCSV(fn,data):
    
    retVal = -1
    try:  
        f = open(fn, "wb")
        c = csv.writer(f)
        for r in data:
            c.writerow(r) 
        f.flush(); f.close()
        retVal = 0
        
    except IOError, (errno,strerror):
        msg  =  "I/O error (%s) %s Redirecting output to stderr" %(errno,strerror) 
        msg +=  "FileName %s could not be created " % fn
        print "%s" % msg

    except:
        print "==EXCEP %s %s" % (sys.exc_type,sys.exc_value)
    finally: 
        return retVal
# This method renames control files with a timestamp and archives control files.
# fls : List with full path for files to be archived.
# ts Timestamp
# cf compress flag.
#def archFiles(self,fls,ts,cf=False)    : 
#    rc = 0
#    for src in fls:
#        fn = getFileBaseName(src)
#        d = '%s/%s' % (self.ib.archDir, fn)
#        r = moveFile(src, d)
#        if r != 0 : rc += 1
#        print('Moving file %s to %s rc= %s' % (src, d, rc))
#            
#        if cf is True:
#            zf = '%s/%s.%s' % (self.ib.archDir , fn, self.ib.archSuff)
#            r = compressFile(zf, d)
#            if r != 0 : print ("Cannot compress %s r = %s. " % (zf, r))
#            else      : print ("Compressed %s r = %s " % (zf, r))
#    return rc

# Generic method to load configuration files 
# fn file name to load
# ib is the container to set values to
def loadConfigFile(fn,ib,log):
    rc = 0
    f = openFile(fn, 'r+')
    if f is None : return 1
    i = 0  ; s = 0
    
    while 1:  
        ln = f.readline()
        if not ln : break
        
        i+=1
        ln = ln.partition('#')[0]
        ln.rstrip()
        v,s,d = ln.partition('=')
        if s == '=':
            try:
                exec "ib.%s = '%s'" % (v,d.strip())
                log.debug('Setting ib.%s = %s' % (v,d)) 
            
            except AttributeError:
                log.error( 'ib.%s = %s' % (v,d))
                log.error( '%s ==> %s ' % (sys.exc_type, sys.exc_value))
                rc = 1
                return rc
                
            except SyntaxError, (errno,strerror):
                log.error("%s" % errno)
                log.error("line [%d] '%s'" % (i,ln))
                rc = 1
                return rc
                           
    closeFile(f)
    return rc

# Find diff in dir level only
def diff(d1,d2,):

    # Determine the items that exist in both directories
    d1_contents = set(os.listdir(d1))
    d2_contents = set(os.listdir(d2))
    common = list(d1_contents & d2_contents)
    common_files = [ f 
                    for f in common 
                    if os.path.isfile(os.path.join(d1, f))
                    ]
    print 'Common files:', common_files
    
    # Compare the directories
    match, mismatch, errors = filecmp.cmpfiles(d1, 
                                               d2, 
                                               common_files)
    print 'Match:', match
    print 'Mismatch:', mismatch
    print 'Errors:', errors


#recursively find diff in subdirectories
# ignore = ['*pyc','common_file'])
def diffDir(d1,d2, ign =[]):
    fc = filecmp.dircmp(d1,d2,ignore = ign)
    print fc.report_full_closure()
    
    
# Serialize object in FS
def saveFSDB(fn,obj):
    f = openFile(fn,'w')
    if f is None : return -1
    pickle.dump(obj,f)
    f.close()
    return 0
    
# Load object from FS    
def loadFSDB(fn):
    f = openFile(fn)
    if f is None : return None
    obj = pickle.load(f) 
    f.close() 
    return obj







