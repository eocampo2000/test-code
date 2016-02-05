'''
Created on Feb 11, 2013

@author: eocampo
'''
__version__ = '20130217'

import xml.parsers.expat 

def chk_valid_xml(fn): 
    try: 
        parser = xml.parsers.expat.ParserCreate() 
        parser.ParseFile(open(fn, "r")) 
        return (0, '%s is well-formed' % fn )
    
    except Exception, e: 
        return(1, "%s is %s" % (fn, e))
    
# This method reads and xml element root recursively and places the result in a dictionary.
# Child attributes will have the @
# Use the following  to invoke:
#    tree = ElementTree.parse(f)
#    root = tree.getroot()
#    d    = xml_to_dict(root)
#   
def xml_to_dict(el):
    d={}
    if el.text:
        d[el.tag] = el.text
    else:
        d[el.tag] = {}
    children = el.getchildren()
    if children:
        d[el.tag] = map(xml_to_dict, children)

    d.update(('@' + k, v) for k, v in el.attrib.iteritems())

    return d        