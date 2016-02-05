'''
Created on Feb 7, 2012

@author: eocampo
'''
'''
Created on Feb 11, 2011

@author: emo0uzx
'''
__version__ = '20130529'

import base64

from itertools import izip, cycle
 
def _xor_crypt_string(data, key):
    return ''.join(chr(ord(x) ^ ord(y)) for (x,y) in izip(data, cycle(key)))

def encrypt(data,key):
    return _xor_crypt_string(data,key)

def decrypt(eData,key):
    return _xor_crypt_string(eData,key)

def encod64(st): return base64.b64encode(st)

def decod64(st): return base64.b64decode(st)
