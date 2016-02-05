'''
Created on Sep 21, 2012

@author: eocampo

This is a utility class to be used for different loads.
chkTrailer  populates   self.procFiles
New Class Style.
20130507 : Added _LeaseCreditFile class
20131119 : Added _TalentMapFile class
20140122 : Added _SAPEmployeeFile class
20140213 : Added _Vehicle class
20141008 : Changed talentmap token, fix rc code for finally.
20150130 : Changed _getDateRunStr and _getFileProcName for Miss Vehicle file. 
           Modified _checkTrailer and added  _chkDlyTrailer and _chkMthlyTrailer methods.
20150526 : Modified logic in _checkTrailer to allow 0 record files.         
'''

__version__ = '20150526'

import sys 
import string

import utils.fileutils as fu
import utils.strutils  as su

# Walker Loads .
# Filenames should have the following formats:
# Ap_scuaphis_20120725.txt 
# Ap_sce5100_20120914_1.txt
# Mehods operates after the 2nd '_' and will remove the last 4 char (.txt) file sufffix.
# In Walker flat files loads by convention there is a trailer line of the form 201206030000001875.

class _WalkerFlatFile(object):  
    
    def __init__(self):
        pass
    
    # Gets File processing name as specified in self.ib.srcFile list.
    # If error with name, returns empty string.
    def _getFileProcName(self,fn):
            
            idx = fn.find('_',5,len(fn))
            if idx < 3: 
                self.log.error('fn = %s bad idx = %d' % (fn,idx))
                return ''        
            
            else: 
                srcf = '%s.txt' % fn[0:idx]
                self.log.debug('File %s ' % srcf) 
                return srcf
    
    # This method returns a valid date string and an iteration run.
    # Use this method for jobs that are scheduled to run more than once a day !
    def _getDayRun(self,dayr,dpLen):
        
        d = None; r = -1
        if (len(dayr) != dpLen) : 
            self.log.debug('dayr=%s (%d) len does not match DP_LEN %s ' % (dayr,len(dayr),dpLen))
            return d,r
        
        d= dayr[:8]; r = dayr[9:10]
        
        rc = su.isValidDate(d, '%Y%m%d')
        if rc is False   : d = None
        rc = su.toInt(r) 
        if rc is None    : r = -1
        else             : r = rc
        return d,r
    
    # This method returns a valid date string.
    # Use this method for jobs that are scheduled to run only once per day and do not have an iter field.
    def _getDay(self,dayr,dpLen):
        
        d = None
        if (len(dayr) != dpLen) : 
            self.log.debug('dayr=%s (%d) len does not match DP_LEN %s ' % (dayr,len(dayr),dpLen))
            return d, -1
        
        d= dayr[:8]
        
        rc = su.isValidDate(d, '%Y%m%d')
        if rc is False   : d = None
        return d,1
            
    # This is a generic method, that will return a substring. 
    # Starts looking at the 5 character (based on filename)
    # After the first '_' and will remove the .XXX suffix from a file. 
    # e.g. Ap_scg4100_20120812_2.txt  returns YYYYMMDD_R
    # e.g. Ap_schuaphis_20120812.txt  returns YYYYMMDD
    def _getDateRunStr(self,fn):
        
        dayr = None

        # idx = fn.find('_',3,len(fn))
        idx = fn.find('_',5,len(fn))
        if idx < 3 : 
            self.log.error('Invalid fileSet format %d ' % idx) 
            return dayr
        
        dayr = fn[idx+1:-4]
        self.log.debug('fn = %s dayr = %s' % (fn,dayr)) 
        
        return dayr
    
    # This method needs to get last_run from storage.
    # Calculate based on run schedule when is next run as well as iteration.  
    # curr_dayr  YYYYMMDD   
    # prev_dayr  YYYYMMDD 
    # pd  previous run date.
    # pr  previous run iteration, daily is always 1
    # RPD  RUN_PER_DAY Load frequency in a day.
    def _chkNextRun(self,cur_dayr,prev_dayr,pd,pr,RPD):
        self.log.debug('pd = %s\tpr = %s\tRPD = %s' % (pd,pr,RPD))
        self.log.info('Prev Succeed run = %s\tCurr IN File Run = %s' % (prev_dayr,cur_dayr))
    
        if pd is None or pr == -1:
            self.log.error('Invalid date %s OR invalid run Iter %d on PREV_RUN' % (pd,pr)) 
            return 1
        
        # Same day. Next Iteration
        if pr < RPD:
            cr = pr + 1; cd = pd
            self.log.debug('(pr < RUN_PER_DAY)\tpr = %s cr = %s cd = %s ' % (pr,cr,cd))
        
        # Next day. First Iteration.
        elif pr == RPD:
            cr = 1; cd = self._getNextRunDate(pd)
            self.log.debug(' (pr == RUN_PER_DAY)\tpr= %s cr = %s cd = %s ' % (pr,cr,cd))
        
        # Invalid Iteration
        else: 
            self.log.error(' Out of range run iteration pr %s ' % (pr))
            return 2
                
        # For daily load freq > 1 Use date and iteration.
        if (RPD > 1): nxt_run = '%s_%s' % (cd,cr)         # 
        else        : nxt_run = cd     
            
        if  nxt_run  != cur_dayr:
            self.log.error('Invalid Load sequence: Current Date Load (file) %s DOES not match Next Sched Date Load  %s !' % (cur_dayr,nxt_run))
            self.log.error('Previous Load Date = %s (succesful run). Source System has not send files in order OR maybe missed a load !' % prev_dayr)
            return 3
        
        # Good to Go 
        self.log.info ('Previous Load Date = %s === Next Sched Date Load = %s Current Date Load Run = %s' % (prev_dayr,nxt_run,cur_dayr) )
        
        return 0
    
    def _chkDlyTrailer(self,fnp,fdr):
 
        # Check if valid Date & truncate last line (trailer Line)
        tr         = fu.getLastLine(fnp,1).rstrip()     # Trailer Line
        dt, rowc = tr[:8],int(tr[8:])
        date = '%s/%s/%s' % (dt[4:6],dt[6:8],dt[:4])
        if su.isValidDate(date,'%m/%d/%Y') is False : 
            self.log.error('fn %s === \tInvalid date %s!' % (fnp,date))
            return -1
              
        fd  = fdr[:8]
        if dt != fd :
            self.log.error('fn %s === \tFile Date = %s do not match trailer = %s!' % (fnp,fd,dt))
            return -2     
        
        return rowc
    
    def _chkMthlyTrailer(self,fnp,fdr):
 
        # Check if valid Date & truncate last line (trailer Line)
        tr         = fu.getLastLine(fnp,1).rstrip()     # Trailer Line
        dt, rowc = tr[:6],int(tr[6:])
        date = '%s%s' % (dt[:4],dt[4:])
        if su.isValidDate(date,'%Y%m') is False : 
            self.log.error('fn %s === \tInvalid date %s!' % (fnp,date))
            return -1
              
        fd  = fdr[:6]
        if dt != fd :
            self.log.error('fn %s === \tFile Date = %s do not match trailer = %s!' % (fnp,fd,dt))
            return -2     
        
        return rowc
    
    # This method encapsulates all the intelligence regarding the trailer lines. 
    # Check if trailer line is of the form 201206030000001875  # YYYYMMDD # of rows.
    # Get the number of lines of the file lc
    # Get the file name, to extract the date and it should match trailer date.
    # fnp file name (complete path) list 
    # fn  file name
    # fdr file day run YYYYMMDD or YYYYMMDD_R or YYYYMM
    # IT TRUNCATES THE TRAILER LINE.
    # BAsed on FDR length will determine if monthly (YYYYMM) or Daily
    
    def _checkTrailer(self,fnp,fn,fdr):
        self.log.info("Filename : %s " %  fnp)
        fd = -1; ln =-1; tr=-1; date=-1;rowc=-1; dt=-1; lc=-1
        self.log.debug('fnp=%s fn=%s fdr=%s' % (fnp,fn,fdr))
        try:
            # Check if valid Date & truncate last line (trailer Line)
#            tr         = fu.getLastLine(fnp,1).rstrip()     # Trailer Line
#            dt, rowc = tr[:8],int(tr[8:])
#            date = '%s/%s/%s' % (dt[4:6],dt[6:8],dt[:4])
#            if su.isValidDate(date,'%m/%d/%Y') is False : 
#                self.log.error('fn %s === \tInvalid date %s!' % (fnp,date))
#                return 1
#              
#            fd  = fdr[:8]
#            if dt != fd :
#                self.log.error('fn %s === \tFile Date = %s do not match trailer = %s!' % (fnp,fd,dt))
#                return 2            
            
            # Dly
            if len(fdr) > 6 : 
                rowc = self._chkDlyTrailer(fnp,fdr)
                if rowc < 0 :
                    self.log.debug('_chkDlyTrailer : rowc = %s !' % (rowc))       
                    return 1
            # Mthly
            else:
                rowc = self._chkMthlyTrailer(fnp,fdr)
                if rowc < 0 :
                    self.log.debug('__chkMthlyTrailer : rowc = %s !' % (rowc))       
                    return 1
                
            # Check Number of lines 
            ln  = fu.getLines(fnp)
            dc =  int(ln) - int(rowc)             # ln file has 2 extra lines header/trailer, but at this point trailer rec had been removed.
            lc = int(ln) - 1
            if  dc != 1 :
                self.log.error('fn %s === \tFile record count %s do not match trailer = %s # of records diff = %d!' % (fnp,rowc,lc,dc-1))
                return 3
            
            f = self._getFileProcName(fn)
            if f in self.srcCount:
                self.srcCount[f] = lc
                self.log.info('file  %s\tLineCnt = %d ' % (fn,lc))
            else:
                self.log.error("UNKNOWN KEY %s" % f)  # Should never get this Error !
                return 4
            
            return 0
        
        # Invalid datatypes fields for verification process !!
        except:            
            self.log.error(' ==EXCEP %s ' % (sys.exc_info()[1]))
            return 5
        
        finally:
            self.log.debug('fn = %s == date %s\t lines %s' % (fnp,fd,ln))
            self.log.debug('trl= %s == date %s\t lines %s' % (tr,dt,rowc))
    
    # fL file list to process .   
    # cur_dayr  
    # returns procFiles Files to process. Means that trailer,date and line count matches.
    def chkTrailer (self,fL,fn,fdr):
        procFiles  = []
        for fnp in fL:
            if fu.fileExists(fnp) is False:
                self.log.error('File %s does not exists ' % fnp)
                continue
            if fu.getFilseSize(fnp) == 0 : 
                self.log.error('File %s is Empty !' % fnp)
                continue
          
            rc = self._checkTrailer(fnp,fn,fdr)
            if rc == 0 : procFiles.append(fnp)

        return procFiles      
            
    # This method to check is a data set is complete.
    # All of the Incoming file need to have the same dayr,
    # Otherwise set is not complete and should error out.
    # dayr could be  YYYYMMDD(1 day run) or YYYYMMDD_R (more than 1 run per day)
    # self.incFiles list of incoming files including paths.
    def chkCompleteSet(self, dayr,fL):

        rc = 0    
        
        for fnp in self.incFiles:
            fn = fu.getFileBaseName(fnp)
            idx = fn.find('_',3,len(fn))
            if idx < 3: 
                rc+=1        
                self.log.error('fn = %s bad idx = %d' % (fn,idx)) 
                continue
            
            ns = fn[idx+1:-4]
            if dayr != ns:
                rc+=1
                self.log.error('fn = %s bad pattern = %s dayr = %s ' % (fn,ns,dayr))
                continue 

            srcf = '%s.txt' % fn[0:idx]
            self.log.debug('Key SrcFile %s ' % srcf)
            
            # Check if file is a member of pre-defined set. 
            try : self.ib.srcFile.index(srcf) 
                  
            except ValueError:
                self.log.error('filename ', srcf, ' not found in set list :', self.ib.srcFile) 
                rc+=1        
            
            self.log.debug('fn = %s ns = %s' % (fn,ns)) 
        
        return rc


    
class _LeaseCreditFile(_WalkerFlatFile):  
    
    def __init__(self):
        pass
    
    # This is a generic method, that will return a substring
    # Cortera_Trx_Extract_20130502.txt 
    # returns 20130502
    def _getDateRunStr(self,fn):
        
        dayr = None
        if len(fn) < 13 : return dayr
        dayr = fn[-12:-4]
        self.log.debug('fn = %s dayr = %s' % (fn,dayr)) 
        return dayr
    
    
    
class _IMSSftyLocFile(_WalkerFlatFile):  
    
    def __init__(self):
        pass
    
    # This is a generic method, that will return a substring
    # IMS_Sfty_Location_201304.csv
    # returns 201304
    def _getDateRunStr(self,fn):
        
        dayr = None
        if len(fn) < 28 : return dayr
        dayr = fn[-10:-4]
        self.log.debug('fn = %s dayr = %s' % (fn,dayr)) 
        return dayr
    
    # This method returns a valid date string.
    # Use this method for jobs that are scheduled to run only once per month.
    def _getMonth(self,dayr,dpLen):
        
        d = None
        if (len(dayr) != dpLen) : 
            self.log.debug('dayr=%s (%d) len does not match DP_LEN %s ' % (dayr,len(dayr),dpLen))
            return d, -1
        
        d= dayr[:6]
        
        rc = su.isValidDate(d, '%Y%m')
        if rc is False   : d = None
        return d,1
    
    
class _TalentMapFile(object):  
    
    FILE_OFFSET_ROW = 4
    HDR_ROW         = 4
    
    def __init__(self):
        pass

    # From file of (fnp): 
    # File structure:  
    #     1st line FileName
    #     2nd line number of records
    #     3rd line blank
    #     4th line header record.
    # Get the filename, shoudl match 1st line.
    # Get the number of lines (ln)  
    # Get the number of data rows from header (rowc) (offset is FILE_OFFSET_ROW) of 
    # fnp file name (complete path) list 
    # rowchr Format =  Records: 59

    def getHdrRow(self,fnp): return fu.getFileLine(fnp,_TalentMapFile.HDR_ROW)
        
    def _checkHeader(self,fnp):
        rc  = 0 
        ln  =-1; rowc=-1; dc = -1 ;idx=-5
        tok = 'Records:'           
        
        try:
            # Check header count 
            fname  = fu.getFileLine(fnp,1).strip()
            rowchr = fu.getFileLine(fnp,2).strip()
            self.log.debug('fname = %s rowchr = %s' % (fname,rowchr))
            
            #Check filename (base Filenamme)
            
            idx    = string.find(rowchr,tok)
            if idx == -1 : 
                self.log.error('Token "%s" not found in file %s ' % (tok,fname))
                rc = idx
                return rc
            rowc   = rowchr[idx + len(tok):]
             
            # Check Number of lines 
            ln  = fu.getLines(fnp)            
            dc =  int(ln) - int(rowc) - _TalentMapFile.FILE_OFFSET_ROW          # ln file has 4 extra lines header
                
            if  dc != 0 :
                    self.log.error('fn %s === \tFile record count %s do not match header = %s # of records diff = %d!' % (fnp,ln,rowc,dc))
                    rc = 1
           
        except:
            self.log.error("EXCEP %s %s "  % (sys.exc_type, sys.exc_info()[1]))
            rc = 2
            
        finally : 
            self.log.debug("fnp = %s rowchr = %s rowc = %s ln = %s dc = %s idx= %s" % (fnp,rowchr,rowc,ln,dc,idx))
            return rc
               
    def chkHeader(self,fL):       
        procFiles  = []
        for fnp in fL:
            if fu.fileExists(fnp) is False:
                self.log.error('File %s does not exists ' % fnp)
                continue
            if fu.getFilseSize(fnp) == 0 : 
                self.log.error('File %s is Empty !' % fnp)
                continue
          
            rc = self._checkHeader(fnp)
            if rc == 0 : procFiles.append(fnp)

        return procFiles
    
    
    
class _SAPEmployeeFile(object): 
    
    # This method gets the trl_date from the file trailer
    # returns the date format YYYYMMDD otherwise None
    def getFileProcDate(self,fnp):
        tr  = fu.getLastLine(fnp)
        tok = tr.split()
        self.log.debug('Last line = ', tr , ' tokens ', tok)
        if len(tok) != 4 :
            self.log.error('filename %s === \tInvalid Number of tokens.It has %d need to be 4 !' % (fnp,len(tok)))
            return None
            
        if su.isValidDate(tok[1],'%Y%m%d') is False : 
            self.log.error('fn %s === \tInvalid date %s!' % (fnp,tok[1]))
            return None
        
        return tok[1]
        
    # last line 'TRAILER 20140117 084539 0000000027'
    # fnp file name path
    # trl_date file date run. Should be the next day run based on previous run in ctl file. format YYYYMMDD
    # SE Sets self.curr
    def _checkTrailer(self,fnp,trl_date):
        self.log.info("Filename : %s " %  fnp)
        fd = -1; ln =-1; tr=-1; rowc=-1; dt=-1; lc=-1;tok = []
        try:
            # Check if valid Date & truncate last line (trailer Line)
            tr         = fu.getLastLine(fnp,1).rstrip()     # Trailer Line
            tok        = tr.split()
            self.log.debug('Last line = ', tr , ' tokens ', tok)
            if len(tok) != 4 :
                self.log.error('filename %s === \tInvalid Number of tokens.It has %d need to be 4 !' % (fnp,len(tok)))
                return 1
            
            if tok[0] != 'TRAILER':
                self.log.error('filename %s === \tInvalid TRAILER LINE.It need to start with TRAILER insted of !' % (fnp,tok[0]))
                return 2
            
            dt = tok[1]
            if su.isValidDate(dt,'%Y%m%d') is False : 
                self.log.error('fn %s === \tInvalid date %s!' % (fnp,dt))
                return 3
            
            rowc = su.toInt(tok[3])
            if rowc is None:
                self.log.error('fn %s === \tInvalid row count %s!' % (fnp,tok[3]))
                return 4
            
            if dt != trl_date:
                self.log.error('fn %s === \tInvalid Trailer ran date of %s. Should be %s  ' % (fnp,dt,trl_date))
                return 5
                    
            # Check Number of lines 
            ln  = fu.getLines(fnp)      # Number of lines w/o trailer.
            dc =  int(ln) - rowc        # ln file has 2 extra lines header/trailer, but at this point trailer rec had been removed.
            self.log.debug('fn %s === \tFile record count %s trailer = %s diff %s' % (fnp,ln,rowc,dc))
            if  dc != 0 :
                self.log.error('fn %s === \tFile record count %s do not match trailer = %s # of records diff = %d!' % (fnp,ln,rowc,dc))
                return 6
            
            return 0
        
        # Invalid datatypes fields for verification process !!
        except:            
            self.log.error(' ==EXCEP %s ' % (sys.exc_info()[1]))
            return 5
        
        finally:
            self.log.debug('fn  %s == date %s\t lines %s' % (fnp,fd,ln))
            self.log.debug('trl %s == date %s\t lines %s' % (tr,dt,rowc))
     
    # fL file list to process .   
    # trl_date Trailer Date  
    def chkTrailer (self,fL,trl_date):
        rc = 0
        for fnp in fL:
            if fu.fileExists(fnp) is False:
                self.log.error('File %s does not exists ' % fnp)
                continue
            if fu.getFilseSize(fnp) == 0 : 
                self.log.error('File %s is Empty !' % fnp)
                continue
            
            r = self._checkTrailer(fnp,trl_date)
            if r != 0 : rc = 1

        return rc        

"""
This class will manipulate a single control file and split it into 4 for different vehicle loads.
Mainframe Team was unable to provide a better way ! (Trailer lines on each file)
'P.PO099D30.WHOUSE.COUNTS'  rename to PO099D30.cnt' 
00017807000008930000007900334275          020714
'P.PO870D30.WHOUSE.COMP.STD'(PO870D30comp.dat) 8
'P.PO875D30.WHOUSE.CURR.STD'(PO875D30.dat)     8
'P.PO880D30.WHOUSE.HIST.STD'(PO880d30.dat)     8
'P.PO225D15.UMPK(0)'(PO225D15.dat)             8
filler                                        42
Date                                           6
Note Date could be curr_day or curr_date - 1
"""                                               
class _Vehicle(object): 
    
    # fnp filepath to count control file
    # sd  source directory 
    def splitMainCntFile(self):
        #Get Header.
        fnp = '%s/%s' % (self.ib.workDir,self.srcFile)
        s   = fu.readFile(fnp)
        hdr = su.remSpacefrStr(s)
        if len(hdr) != 38 :
            self.log.error('Invalid len= %d  for %s. Need to be 38\nhdr=%s' % (len(hdr),fnp,hdr))
            return 1
    
        self.log.debug('hdr = %s' % hdr)
        
        # Get counts 
        po870D30 = su.toInt(hdr[0:8])
        po875D30 = su.toInt(hdr[8:16])
        po880D30 = su.toInt(hdr[16:24])
        po225D15 = su.toInt(hdr[24:32])
        rdate    = hdr[32:]
        
        self.log.debug('po870D30=%s po875D30=%s po880D30=%s po225D15=%s rdate=%s' % (po870D30, po875D30,po880D30,po225D15,rdate))
        
        # Sanity check.
        if  (po870D30 is None or po875D30 is None or po880D30 is None or po225D15 is None or su.isValidDate(rdate,'%m%d%y') == False) :
            self.log.error('po870D30=%s po875D30=%s po880D30=%s po225D15=%s rdate=%s' % (po870D30, po875D30,po880D30,po225D15,rdate))
            return 2
        
        # Create count files.
        rc = 0 
        r = fu.createFile('%s/po870d30comp.cnt' % self.ib.workDir, '%08d%s' % (po870D30,rdate))  ; rc+= r
        self.log.info (' Creating file %s/po870d30comp.cnt rc = %s' % (self.ib.workDir, r))
        r = fu.createFile('%s/po875D30.cnt' % self.ib.workDir, '%08d%s' % (po875D30,rdate))  ; rc+= r
        self.log.info (' Creating file %s/po875d30.cnt rc = %s' % (self.ib.workDir, r))
        r = fu.createFile('%s/po880D30.cnt' % self.ib.workDir, '%08d%s' % (po880D30,rdate))  ; rc+= r
        self.log.info (' Creating file %s/po880d30.cnt rc = %s' % (self.ib.workDir, r))
        r = fu.createFile('%s/po225D15.cnt' % self.ib.workDir, '%08d%s' % (po225D15,rdate))  ; rc+= r
        self.log.info (' Creating file %s/po225d15.cnt rc = %s' % (self.ib.workDir, r))

        return rc 
    
    # This is generated split file.
    # fnp filepath to the count file.
    # hdr_date should be run date based on schedule. Shoudl be offset and offset -1
    # NNNNNNNNMMDDYY =  12345678021412
    
    def _chkHeader(self,fnp,rdoff):
        RUN_DATE = su.getTodayDtStr(fmt='%y%m%d')
        rdayoffset = su.toInt(rdoff)
        
        hdr   = fu.readFile(fnp)
        
        if len(hdr) != 14 :
            self.log.error('Invalid len= %d  for %s. Need to be 14\nhdr=%s' % (len(hdr),fnp,hdr))
            return 1
            
        rowc     = su.toInt(hdr[0:8])
        hdr_date = hdr[8:]                  # Run Date from file.
          
        dt = su.getDayMinusStr(rdayoffset, RUN_DATE, '%Y%m%d')

        # Check run date.
        if dt != hdr_date:
            self.log.warn('fn %s === \tInvalid Header ran date of %s. Should be %s  ' % (fnp,dt,hdr_date))
        
        if rdayoffset < 1 : 
            self.log.error('Could not attempt next day since rdayoffset is %s < 1' % rdayoffset)
            return 2
            
        dt = su.getDayMinusStr(rdayoffset - 1, RUN_DATE, '%Y%m%d')
        # Check run date + 1.
        if dt != hdr_date:
            self.log.error('fn %s === \tInvalid Header ran date of %s. Should be %s  ' % (fnp,dt,hdr_date))
            return 3
        
        # Check Number of lines 
        ln  = fu.getLines(fnp)      # Number of lines w/o trailer.
        dc =  int(ln) - rowc        # ln file has 2 extra lines header/trailer, but at this point trailer rec had been removed.
        self.log.debug('fn %s === \tFile record count %s trailer = %s diff %s' % (fnp,ln,rowc,dc))
        if  dc != 0 :
            self.log.error('fn %s === \tFile record count %s do not match trailer = %s # of records diff = %d!' % (fnp,ln,rowc,dc))
            return 4
        
        return 0