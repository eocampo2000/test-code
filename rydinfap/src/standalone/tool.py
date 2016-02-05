'''
Created on May 29, 2013

@author: eocampo
'''
__version__ = '20130529'

import base64
import socket

def enc64():
    print "hostname : %s" % socket.gethostname()  
    hn  = raw_input("Enter Hostname:")
    pw  = raw_input("Enter Password:")
    pwd = '%s:%s' % (hn,pw)
    base64encoded = base64.b64encode(pwd)
    print "encoded pwd = %s" % base64encoded


def dec64():
    be = raw_input("Enter Encrypted Password:")
    base64decoded = base64.b64decode(be)
    
if __name__ == "__main__":
    enc64()