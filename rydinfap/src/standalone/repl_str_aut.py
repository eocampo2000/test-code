'''
Created on Jul 15, 2015

@author: eocampo

repl_str.py BDL_SCHEMA=BDL BDL_SCHEMA=BDL_UPG /home/eocampo/tests/param

'''
import fileinput, glob, string, sys, os
from os.path import join
# replace a string in multiple files
#filesearch.py

repl_dict = [('_SCHEMA=BDL','_SCHEMA=BDL_UPG','/home/eocampo/tests/param'),
             ('_SCHEMA=bdl','_SCHEMA=BDL_UPG','/home/eocampo/tests/param'),
             ('_SCHEMA=IDL','_SCHEMA=IDL_UPG','/home/eocampo/tests/param'),
           ]
if len(sys.argv) < 2:
    print "usage: %s search_text replace_text directory" % os.path.basename(sys.argv[0])
    sys.exit(0)

stext = sys.argv[1]
rtext = sys.argv[2]
if len(sys.argv) ==4:
    path = join(sys.argv[3],"*")
else:
    path = "*"

print "finding: " + stext + " replacing with: " + rtext + " in: " + path

files = glob.glob(path)
for line in fileinput.input(files,inplace=1):
    lineno = 0
    lineno = string.find(line, stext)
    if lineno >0:
        line =line.replace(stext, rtext)

    sys.stdout.write(line)