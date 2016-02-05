'''
Created on Oct 1, 2012

@author: eocampo

Convenience class for utility pre defined commands to be used via remote cmd invocation.
'''
__version__ = '20121001'

    
class CmdWinStr(object):
    pass        

class CmdUXStr((object)):
       
    #--------------------------------------------------------------------------------------------
    # System configuration: lcpu=4 mem=4095MB ent=0.20
    #  kthr     memory              page              faults              cpu             time
    #-------- ----------- ------------------------ ------------ ----------------------- --------
    # r  b  p   avm   fre  fi  fo  pi  po  fr   sr  in   sy  cs us sy id wa    pc    ec hr mi se
    # 0  1  0 1032323  5288 443   4   0   0   0    0 214 1455 878 19 27 41 13  0.12  60.2 09:11:45
    servUtil  = """" rc=`vmstat -It %s %s` ; echo $rc """ 


class CmdLinuxStr(CmdUXStr):
    
    diskUse = 'df -h'  
    
      
class CmdHPUXStr(CmdUXStr):
    
    diskUse = 'bdf'
    
    
class CmdAIXStr(CmdUXStr):
   
    diskUse = 'df -g'  
    
    # Commands  USER PID %CPU  %MEM   ELAPSED/PAGEIN TIME COMMAND
    topProc = """" rc=`echo "cpu="  ; ps -e -o user,pid,pcpu,pmem,etime,time,comm  | grep -v root | sort -rnk2 | head -%s` ; echo $rc ; \
                 rc=`echo "=mem=" ; ps -e -o user,pid,pcpu,pmem,etime,time,comm  | grep -v root |sort -rnk3 | head -%s` ; echo $rc ; \
                 rc=`echo "=run=" ; ps -e -o user,pid,pcpu,pmem,etime,time,comm | grep -v root |sort -rnk5 | head -%s` ; echo $rc ; \
                 rc=`echo "=dsk=" ; ps -e -o user,pid,pcpu,pmem,pagein,time,comm | grep -v root |sort -rnk4 | head -%s` ; echo $rc """  
   
        
    # Commands  USER PID %CPU  %MEM   ELAPSED  PAGEIN TIME COMMAND
    pmtdmProc = """" rc=`ps -e -o \\" ['%U', %p, %C, '%x', '%t', '%a'], \\"  | grep pmdtm | grep -v grep` ; echo $rc """     