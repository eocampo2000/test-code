'''
Created on Mar 21, 2011
This module will process all lkup tables.
@author: emo0uzx
'''
#Function use to populate lookup objects and will return a list for each table.
# Use generically for all lkups.


#import applications.softinv.modules.datastore.dblkup as dblkup

import datastore.dblkup as dblkup                            # DO NOT DELETE 
# dsHandler -> Data Storage Handler
# logger    -> handler to logger
# obj       -> object to look. Valid objs : 'ServSel','DBServSel''DomSel','RepoSel'
def buildLkupObj(dsHandler,logger,obj):
    lkupObj  = []
    lkupList = dsHandler.runQry(eval ( "dblkup.lkup%sQry" % obj ))
    
    for rs in lkupList:
        lkupObj.append(rs)
        #logger.debug("buildLkupObj :obj = %s  dbID = %s Len = %d" % (obj,rs[0], len(rs)))
        
    return lkupObj

# Functions to populate All Dynamic data into a dictionary    
# Function use to populate All tables into a dictionary.
# lkObj => List of objects to iterate.
# Returns mg.env_lkup {'RepoSel': [(4, 'RS_IM_P'), (5, 'RS_IM_P'), (3, 'RS_IM_Q')],
#                      'DomSel': [(4, 'DOM_IM_P'), (5, 'DOM_IM_P'), (3, 'DOM_IM_Q'), (1, 'DOM_IM_S')], 
#                      'ServSel': [(3, 'GOXSA1066'), (6, 'GOXSA1115'), (5, 'GOXSD390'),(2, 'JBXSD307'), (1, 'JBXSD403'), (7, 'JBXSD415')], 
#
def buildInfaLkupObj(stHandler , logger, lkObj):    
    lkupObj = {}
    
    for lkup in lkObj:
        lkupObj[lkup] = buildLkupObj(stHandler ,logger,lkup)
        #logger.debug("buildInfaLkupObj :lkup obj = %s " % (lkupObj))
    
    return lkupObj