'''
Created on Aug 12, 2011

Process Configuration File 
@author: emo0uzx

<RYDINFAP>

<APP_GLOB>
<softdepo>\\\\goxsf101\imds$\Data Warehouse </softdepo>
<bindir>C:\Unix_Scripts</bindir>
<XOR>1<XOR>
</APP_GLOB>

<DATA_SOURCE>
<DS_Driver status="1" name="InfApp" type="Oracle">
    <uname>utest</uname>
    <pwd>ptest  </pwd>
    <DS_Str>TESTDB</DS_Str>
 </DS_Driver>

<DS_Driver status="0" name="InfApp" type="SQLLite">
    <uname></uname>
    <pwd></pwd>
    <DS_Str>softinv.dbf</DS_Str>
 </DS_Driver>

<DS_Driver status="0" name="InfApp" type="SQLLite">
    <DS_Str>softinv.dbf</DS_Str>
 </DS_Driver>

</DATA_SOURCE>

</RYDINFAP>
'''


import xml.etree.ElementTree as ET
import string as ST
import sys

# import applications.softinv.modules.datastore.dbutil as ds 
# import applications.softinv.modules.utils.fileutils as futil
import datastore.dbutil as ds 
import utils.fileutils as futil

import mainglob as mg

# node : Iterator for DATA_SOURCE TAG (tree.getiterator('DATA_SOURCE'))
# This method will traverse all the tree until it finds the appropriate one
# returns connect Str for a particular DATA_SOURCE.

def _parseDS(node):
     
    s  = {'CONFIG_DB': None,
          'DB_DRIVER'  : None}
        
    for elem in node:
        for e in elem:
            
            if e.tag != 'DS_Driver' : continue
            
            u = None; p = None; dsn = None; t = None 
            if e.get('status') =='1':
                t = e.get('type')
                if t is None or len (ST.strip(t)) < 1: continue
                mg.logger.info("Status is one for DS_DRIVER type = %s" % t)

                for x in e:
                    if x.tag == 'uname' : u = ST.strip(x.text)
                    if x.tag == 'pwd'   : p = ST.strip(x.text)
                    if ST.upper(x.tag) == 'DS_STR' : dsn = ST.strip(x.text)
                     
                # get Connect String                     
                if dsn is None or len (dsn) < 1: 
                    mg.logger.error (" %s DSN is null or blank" % e.get('name'))
                    continue
                
                mg.logger.debug("Invoking getDSConnStr t = %s ,u = %s ,p = %s ,dsn = %s " % (t,u,p,dsn))
                return {'CONFIG_DB': ds.getDSConnStr(t, u, p, dsn),
                        'DB_DRIVER'   : t
                        }                    
    return s



# node : Iterator for APP_GLOBAL TAG (tree.getiterator('APP_GLOBAL'))
# Method returns a dict with pre-defined values

def _parseGlob(node):
    g = {}

    for elem in node:
        for e in elem:
            g[e.tag] = ST.strip(e.text)
    
    return g


def parseConfigXML(fn):
      
    d = {}
    
    f = futil.openFile(fn,'r')
    mg.logger.debug("Config file %s " % fn)
    if f is None : 
        mg.logger.error("Config file %s NOT FOUND !!!" % fn)
        return None
    try: 
        tree = ET.parse(f)
        if tree == '' : 
            mg.logger.error("Empty Config File")
            return d
    
        rn = tree.getroot()
        if ST.upper(rn.tag) != 'RYDINFAP' :
            mg.logger.error("Did not find root element: 'RYDINFAP' ")
            return d 
        
        n =  tree.getiterator('APP_GLOB')
        if len(n) > 0 : d = _parseGlob(n)
        else          :  mg.logger.error("Did not find element: 'APP_GLOB' ")
        

        n =  tree.getiterator('DATA_SOURCE')
        if len(n) > 0 : d.update(_parseDS(n))
        else          :  mg.logger.error("Did not find element: 'DATA_SOURCE' ")
        
    except ET.ParseError: 
            mg.logger.error("==EXCEP %s %s "  % (sys.exc_type, sys.exc_value))
           

    finally:   return d