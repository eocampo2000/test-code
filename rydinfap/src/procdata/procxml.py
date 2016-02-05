'''
Created on May 15, 2012

@author: eocampo
 
 XML files parser. Please specify the main LAYOUT.
 TODO encapsulate it into a class
 EO 20150313 : Added parseCanCPI, _parseCanCPITable methods.

'''
__version__ = '20150313'


import  utils.fileutils as fu
import xml.etree.ElementTree as ET
import sys
import string as ST
from bs4 import BeautifulSoup

# Use for informatica schedules
#<IS_SCHED>
#  <FOLDER>
#     <WF sched="1">wkf_name</WF>
#     <WF sched="1">wkf_name</WF> 
#   </FOLDER>
#</IS_SCHED>

sched   = []
unsched = []

# Method parses <WF status="1">wkf_name</WF>
def _parseFolder(fld,node):

    for elem in node:
        if elem.tag == 'wkf':
            sc = elem.get('sched')
            wf = ST.strip(elem.text)
            if sc == '1'  :  sched.append('%s.%s' % (fld,wf))
            else          :  unsched.append('%s.%s' % (fld,wf))
            
# Method returns rc 0 if no error and the sched and unsched lists.
def parseSched(fn):
    f = fu.openFile(fn,'r')    
    if f is None : return (1, [], [])
    
    try:
        tree = ET.parse(f)
        # Empty tree
        if tree == '' : return (2, [], [])
            
        rt = tree.getroot()
        
        # 1- Check TREE root
        if ST.upper(rt.tag) != 'IS_SCHED' : return (3, [],[])
        
        for elem in tree.getiterator():
            for e in elem:
                if ST.lower(e.tag) == 'folder' :
                    _parseFolder(e.get('name'), e.getchildren())
    
    except ET.ParseError:
            print ("Error %s \t %s " %  (sys.exc_type, sys.exc_value))
            return (4, [],[])

    finally : return 0, sched, unsched


# Method to parse CPI Table
def _parseCanCPITable(rows,sep='\t'):
    d =[]
    
    for row in rows:
        cells = row.findChildren('td')
        if len(cells) < 3 : continue
        i = 0
        
        wline = "CAN"
        for cell in cells:
                i = i + 1
                value = cell.string
                value = value.replace("-", sep)           
                if i < 3:
                    wline = wline  + sep + value                   
                else:
                    d.append(wline + '\n') 
                    break
    return d  

def parseCanCPI(fn):
    data = fu.readFile(fn)
    if data is None or data is '' : return []

    soup = BeautifulSoup(data)
    table = soup.find("table", { "class" : "table table-bordered table-striped table-hover cpi" })
    rows = table.findChildren(['tr'])
    return _parseCanCPITable(rows)
       
def test_schd():
    fn = r'C:\infa_support\schedules\sched.xml'
    rc,s,u = parseSched(fn)
    #parseSched(fn)
    print "rc = ", rc, "\tsched = ", s , "\tunsched ", u
    
def test_cpi():
    fn = 'C:\\apps\\cpi_data_us.html' 
    d = parseCanCPI(fn)
    #parseSched(fn)
    print "len(d ) = ", len(d), "data= ", d 
if __name__ == "__main__":
        #test_schd()
        test_cpi()