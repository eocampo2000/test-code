'''
Created on Jan 3, 2012

This method is used for Informatica Repository/Domain Services DB Store.
# Currently hosted in oracle environments.
@author: eocampo
'''
class DBInfaRepo: 
        
    # Software Inventory    
    selSchedIdx = 'WORKFLOW_NAME,SUBJECT_AREA'               
    selSchedQry = """ 
                SELECT DISTINCT SUBJECT_AREA,                        
                       WORKFLOW_NAME
                FROM REP_WORKFLOWS 
                WHERE SCHEDULER_NAME IS NOT NULL
                ORDER BY SUBJECT_AREA,WORKFLOW_NAME """
    
    selRepWflIdx = 'SUBJ,WKFL'  
    selRepWflQry = """ SELECT DISTINCT SUBJECT_AREA, WORKFLOW_NAME
                      FROM REP_WORKFLOWS ORDER BY WORKFLOW_NAME """              
class DBInfaDom: 
    pass




class DBInfaAdmin:
    # This query returns currently running/failed status.
    selWkfStatusIdx  = 'SUBJ,WKFL,SESS,ST_TIME,END_TIME,STAT,ERR_CODE,ERR_MSG'
    selWkfStatusQry  = \
    """SELECT SUBJ.SUBJ_NAME,WFLOW.TASK_NAME WORKFLOW_NAME,
                INST_RUN.INSTANCE_NAME SESSION_NAME,
                INST_RUN.START_TIME,
                INST_RUN.END_TIME,
                DECODE(INST_RUN.RUN_STATUS_CODE, 1,'SUCCEEDED',2, 'DISABLED', 3, 'FAILED', 4, 'STOPPED', 5, 'ABORTED', 6, 'RUNNING', 7, 'SUSPENDING', 8, 'SUSPENDED', 9, 'STOPPING', 10, 'ABORTING', 11, 'WAITING', 12, 'SCHEDULED', 13, 'UNSCHEDULED', 14, 'UNKNOWN', 15, 'TERMINATED') RUN_STATUS,
                INST_RUN.RUN_ERR_CODE RUN_ERR_CODE,
                INST_RUN.RUN_ERR_MSG RUN_ERR_MSG 
         FROM OPB_SUBJECT SUBJ,(SELECT * FROM OPB_TASK WHERE TASK_TYPE IN (68, 71)) WFLOW, OPB_TASK_INST_RUN INST_RUN 
         WHERE  SUBJ.SUBJ_NAME not like  '~%'      
           AND INST_RUN.START_TIME > SYSDATE -1       
           AND INST_RUN.RUN_STATUS_CODE >  2  
           AND WFLOW.SUBJECT_ID = INST_RUN.SUBJECT_ID 
           AND WFLOW.TASK_ID= INST_RUN.WORKFLOW_ID 
           AND WFLOW.VERSION_NUMBER = INST_RUN.VERSION_NUMBER 
           AND SUBJ.SUBJ_ID = WFLOW.SUBJECT_ID
           ORDER BY RUN_STATUS, END_TIME """