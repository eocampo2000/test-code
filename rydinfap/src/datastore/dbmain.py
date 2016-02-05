#------------------------------------------------------------
# Version 1.0  20101107
#
# dbchksrv.py
#
# Creation Date:    2010/11/07
# Modification Date:
# Description: This module contains all DB database related functions/queries
#  
# 20110211 EO Added ServerCredentials objects.            
# BE AWARE FOR BIND VARIABLES there is a difference 
# paramstyle
# 
# String constant stating the type of parameter marker
# formatting expected by the interface. Possible values are [2]:
# 
# 'qmark' Question mark style,
# e.g. '...WHERE name=?'
# 'numeric' Numeric, positional style,
# e.g. '...WHERE name=:1'
# 'named' Named style,
# e.g. '...WHERE name=:name'
# 'format' ANSI C printf format codes,
# e.g. '...WHERE name=%s'
# 'pyformat' Python extended format codes,
# e.g. '...WHERE name=%(name)s'

# >>> import cx_Oracle
# >>> cx_Oracle.paramstyle
# 'named'
# >>> import sqlite3
# >>> sqlite3.paramstyle
# 'qmark'

class DBMain: 
        
    # Software Inventory    
    selEnvIdx = 'ENVNAME,BU,PROD_ID,SERVNAME,ALIAS,OS,OS_VER,OS_PATCH,BINARY,VER,PATCH,LIC_PATH'               
    selEnvQry = """ 
                 SELECT E.NAME,
                        E.BU,
                        E.PROD_ID,
                        S.NAME,
                        S.ALIAS,
                        S.OS,
                        S.VER,
                        S.PATCH, 
                        D.BIN, 
                        D.VERSION ,
                        D.PATCH ,
                        E.LIC_PATH
                 FROM  ENV E, SERVER S, SOFTVER D
                 WHERE E.SERV_ID = S.ID
                   AND E.SVER_ID = D.ID  
                 ORDER BY E.PROD_ID, E.BU, E.NAME """
    
    # Lkup query   
    selProdIdx = 'ID,NAME,VEND_ID'
    selProdQry = 'SELECT ID, NAME, VEND_ID FROM PROD ORDER BY VEND_ID,ID'

    selVendorIdx ='ID,NAME,ADDRESS1,ADDRESS2,ZIP,URL'
    selVendorQry= """
               SELECT ID, 
                      NAME,
                      ADDRESS1,
                      ADDRESS2,
                      ZIP,
                      URL 
               FROM VENDOR 
               ORDER BY NAME """
               
    selVendContIdx = 'ID,VEND_ID,TITLE,LAST_NAME,FIRST_NAME,ADDRESS1,ADDRESS2,ZIP,PH1,PH2,EMAIL'
    selVendContQry = """
                SELECT  ID, 
                   VEND_ID,
                   TITLE , 
                   LAST_NAME, 
                   FIRST_NAME, 
                   ADDRESS1   , 
                   ADDRESS2 , 
                   ZIP , 
                   PH1 ,
                   PH2 , 
                   EMAIL 
               FROM VENDCONT  """
               
    selSoftVerIdx = 'ID,VEND_ID,PROD_ID,VERSION,PATCH,BIN,DIR_PATH,DOWN_DATE,DOWN_EMAIL,SUPP_MATRIX_OS,SUPP_MATRIX_DB'
    selSoftVerQry = """
                SELECT ID  ,
                   VEND_ID ,
                   PROD_ID ,
                   VERSION ,
                   PATCH ,
                   BIN ,
                   DIR_PATH ,
                   DOWN_DATE,
                   DOWN_EMAIL ,
                   SUPP_MATRIX_OS,
                   SUPP_MATRIX_DB
            FROM SOFTVER
            ORDER BY VEND_ID,PROD_ID,ID """

    selSoftEOLIdx = 'ID,VEND_ID,PROD_ID,VERSION,REL_DATE,EOSTD_DATE,EOEXT_DATE'
    selSoftEOLQry = """
                SELECT A.ID, 
                       A.VEND_ID, 
                       A.PROD_ID,
                       A.VERSION, 
                       A.REL_DATE,
                       A.EOSTD_SUPP_DATE, 
                       A.EOEXT_SUPP_DATE
                FROM SOFTEOL A
                ORDER BY  A.PROD_ID, A.VERSION """

    ###########################################################################################            
    #  Credential Information
    ###########################################################################################    
    selServCredIdx = 'ENV_BU,ENV_NAME,SERV_ID,SERV_NAME,ALIAS,OS,AUTH,UNAME,PWD,DESC,STATUS,LOAD_DATE'  
    selServCredQry = """ 
                   SELECT E.BU,
                          E.NAME,
                          S.ID,
                          S.NAME,
                          S.ALIAS,
                          S.OS,
                          S.AUTH,
                          SC.UNAME,
                          SC.PWD,
                          SC.DESCR,     
                          SC.STATUS,   
                          SC.LOAD_DATE            
                   FROM SERVER S 
                        LEFT OUTER JOIN SERVER_CRED SC ON S.ID = SC.ID
                        JOIN ENV  E  ON S.ID =  E.SERV_ID      
                   GROUP BY E.BU,E.NAME,S.ID,S.NAME,S.ALIAS,S.OS,S.AUTH,SC.UNAME,SC.PWD,SC.DESCR,SC.STATUS,SC.LOAD_DATE   
                   ORDER BY E.BU,E.NAME """ 
    
    selDBCredIdx = 'DBREPO_ID,DB_NAME,DB_IDENT,DB_HOST,DB_TYPE,DB_PORT,DB_ALIAS,DB_VER,AUTH,UNAME,PWD,DESC,STATUS,LOAD_DATE'
    selDBCredQry = """
                  SELECT D.DBREPO_ID, 
                       D.DB_NAME, 
                       D.DB_IDENT,
                       D.DB_HOST, 
                       D.DB_TYPE, 
                       D.DB_PORT, 
                       D.DB_ALIAS, 
                       D.DB_VER,
                       S.AUTH,
                       SC.UNAME,
                       SC.PWD,
                       SC.DESCR,     
                       SC.STATUS,   
                       SC.LOAD_DATE   
                    FROM DBREPO D
                         LEFT OUTER JOIN DB_CRED SC ON D.DBREPO_ID = SC.ID
                         LEFT OUTER JOIN SERVER  S  ON D.DB_HOST   = S.NAME
                     """
                          
    selInfaDomCredIdx = 'ENV_BU,ENV_NAME,DOM_ID,DOM_NAME,DOM_PORT,SERV_NAME,OS,AUTH,UNAME,PWD,DESC,STATUS,LOAD_DATE'  
    selInfaDomCredQry = """                 
                  SELECT  E.BU,
                          E.NAME,
                          D.DOM_ID,
                          D.DOM_NAME,
                          D.DOM_PORT,
                          S.NAME,
                          S.OS,
                          S.AUTH,
                          SC.UNAME,
                          SC.PWD,
                          SC.DESCR,     
                          SC.STATUS,   
                          SC.LOAD_DATE      
                   FROM INFA_DOMAIN D 
                        JOIN SERVER S  ON D.NODE_ID    = S.ID
                        LEFT OUTER  JOIN INFA_DOM_CRED SC ON D.DOM_ID = SC.ID
                        JOIN ENV E ON D.ENV_ID   = E.ID 
                        ORDER BY E.BU,E.NAME """   
    
    selInfaRepoCredIdx = 'ENV_BU,ENV_NAME,REPO_ID,REPO_NAME,SERV_NAME,OS,AUTH,DB_NAME,UNAME,PWD,DESC,STATUS,LOAD_DATE'  
    selInfaRepoCredQry = """                 
              SELECT       E.BU,
                           E.NAME,
                           RS.REPO_ID,       
                           RS.SERVICE_NAME,     
                           S.NAME, 
                           S.OS,
                           S.AUTH,
                           DB.DB_NAME,
                           RC.UNAME, 
                           RC.PWD,
                           RC.DESCR,     
                           RC.STATUS,   
                           RC.LOAD_DATE             
                FROM INFA_REPO_SERV RS
                     JOIN INFA_NODE ND   ON RS.NODE_ID   = ND.NODE_ID 
                     JOIN SERVER S ON ND.SERV_ID = S.ID
                     LEFT JOIN INFA_REPO_CRED RC ON RS.REPO_ID = RC.ID
                     JOIN DBREPO DB ON RS.DBREPO_ID = DB.DBREPO_ID  
                     JOIN ENV E ON S.ID = E.SERV_ID 
                     WHERE E.ID = RS.ENV_ID                    
                GROUP BY E.BU,E.NAME,RS.REPO_ID,RS.SERVICE_NAME,S.NAME, S.OS,S.AUTH,DB.DB_NAME,RC.UNAME, RC.PWD, RC.DESCR,RC.STATUS,RC.LOAD_DATE   
                ORDER BY E.BU,E.NAME      """  
   
    
    # Use to create dict for credential keys.                                         
    #selServerIDIdx = 'SERVER_ID,SERV_NM'
    #selCredIDIdx     = 'ID,SNAME' 
    
#    selServerIDQry   = "SELECT ID, NAME FROM SERVER" 
#    selDBIDQry       = "SELECT DBREPO_ID, DB_NAME FROM DBREPO"
#    selInfaDomIDQry  = "SELECT DOM_ID, DOM_NAME FROM INFA_DOMAIN"
#    selInfaRepoIDQry = "SELECT REPO_ID, SERVICE_NAME FROM INFA_REPO_SERV"
    # Main Server Data for Informatica PC Environment
    # Use for PowerCenter (PC) environments.
    selInfaPCIdx = 'ENV_BU,ENV_NAME,SERV_NAME,SERV_OS,SERV_VER,AUTH,DOM_NAME,DOM_VER,DOM_HF,NODE_NAME,INT_NAME,REPO_NAME,DB_IDENT,DB_VER'
    selInfaPCQry = """  SELECT  E.BU,
                                E.NAME, 
                                S.NAME,  
                                S.OS,
                                S.VER,
                                S.AUTH,
                                D.DOM_NAME,
                                SV.VERSION, 
                                SV.PATCH,
                                ND.NODE_NAME ,
                                I.SERVICE_NAME  as INT_SERV ,
                                RS.SERVICE_NAME  as REPO_SERV,
                                DB.DB_IDENT,
                                DB.DB_VER
                        FROM INFA_NODE ND
                          JOIN SERVER S        ON S.ID = ND.SERV_ID
                          JOIN INFA_INT_SERV I ON I.NODE_ID = ND.NODE_ID  
                          JOIN ENV E           ON S.ID = E.SERV_ID 
                          JOIN SOFTVER SV      ON E.SVER_ID = SV.ID
                          LEFT JOIN INFA_REPO_SERV RS ON RS.NODE_ID = ND.NODE_ID 
                          LEFT JOIN INFA_DOMAIN D ON D.NODE_ID  = ND.NODE_ID
                          LEFT JOIN DBREPO DB  ON RS.DBREPO_ID = DB.DBREPO_ID 
                        WHERE SV.PROD_ID = 1 
                          AND E.ID = I.ENV_ID
                          AND E.ID = RS.ENV_ID
                        ORDER BY E.BU, E.NAME, D.DOM_NAME"""  
  
    # Gral data
    # EO TODO remove cred information.
    selInfaDomIdx = 'DOM_ID,NODE_ID,DBREPO_ID,ENV_ID,ENV_NAME,BU,DOM_NAME,DOM_PORT,VERSION,PATCH,ENV_DIR,ENV_FILE,' \
                    'SERV_NAME,ALIAS,OS,VER,PATCH,DB_HOST,DB_ALIAS,DB_VER,DB_TYPE,DB_NAME,DB_IDENT,DB_PORT,DB_UNAME,DB_PWD'
    selInfaDomQry =  """ SELECT D.DOM_ID,
                                D.NODE_ID, 
                                D.DBREPO_ID, 
                                D.ENV_ID,
                                E.NAME, 
                                E.BU, 
                                D.DOM_NAME, 
                                D.DOM_PORT, 
                                SV.VERSION,
                                SV.PATCH,
                                D.ENV_DIR,
                                D.ENV_FILE,
                                S.NAME, 
                                S.ALIAS,
                                S.OS ,
                                S.VER,
                                S.PATCH,
                                DB.DB_HOST,
                                DB.DB_ALIAS,
                                DB.DB_VER,
                                DB.DB_TYPE,
                                DB.DB_NAME,
                                DB.DB_IDENT,
                                DB.DB_PORT,
                                RC.UNAME, 
                                RC.PWD
                            FROM INFA_DOMAIN D 
                                 JOIN SERVER S  ON D.NODE_ID    = S.ID
                                 JOIN DBREPO DB ON D.DBREPO_ID  = DB.DBREPO_ID
                                 JOIN ENV E ON D.ENV_ID   = E.ID
                                 JOIN SOFTVER SV ON E.SVER_ID = SV.ID
                                 LEFT JOIN INFA_REPO_CRED RC ON D.dom_id = RC.ID                                
                             ORDER BY  E.BU, E.NAME """


    selInfaRepIdx = 'NODE_ID,SERV_ID,DOM_ID,NODE_NAME,NAME,ALIAS,OS,PATCH,DOM_NAME,DOM_PORT,VERSION,NAME,ENV_ID,DBREPO_ID,' \
                    'ENV_DIR,ENV_FILE,SERVICE_NAME,UNAME,PWD,DB_HOST,DB_ALIAS,DB_VER,DB_TYPE,DB_NAME,DB_IDENT,DB_PORT,UNAME,PWD'
                    
    selInfaRepQry = """ 
                    SELECT ND.NODE_ID,
                           ND.SERV_ID,
                           ND.DOM_ID,  
                           ND.NODE_NAME,
                           S.NAME, 
                           S.ALIAS,
                           S.OS,
                           S.PATCH,
                           D.DOM_NAME,
                           D.DOM_PORT,
                           SV.VERSION, 
                           E.NAME,
                           RS.ENV_ID,       
                           RS.DBREPO_ID,
                           RS.ENV_DIR, 
                           RS.ENV_FILE,
                           RS.SERVICE_NAME, 
                           RC.UNAME, 
                           RC.PWD,
                           DB.DB_HOST,
                           DB.DB_ALIAS,
                           DB.DB_VER,
                           DB.DB_TYPE,
                           DB.DB_NAME,
                           DB.DB_IDENT,
                           DB.DB_PORT,
                           DBC.UNAME, 
                           DBC.PWD  
                FROM INFA_REPO_SERV RS
                     JOIN INFA_NODE ND   ON RS.NODE_ID   = ND.NODE_ID 
                     JOIN DBREPO DB ON RS.DBREPO_ID = DB.DBREPO_ID  
                     LEFT JOIN INFA_REPO_CRED RC ON RS.REPO_ID = RC.ID
                     JOIN SERVER S ON ND.SERV_ID = S.ID
                     JOIN INFA_DOMAIN D ON ND.DOM_ID = D.DOM_ID
                     LEFT JOIN DB_CRED DBC ON  DB.DBREPO_ID = DBC.ID
                     JOIN ENV E ON S.ID = E.SERV_ID 
                     JOIN SOFTVER SV ON E.SVER_ID = SV.ID
                     WHERE SV.PROD_ID = 1   """

    # Credentials
    insCredIdx = 'ID,UNAME,PWD,DESCR'
    insCredVar = ('ID','UNAME','PWD','DESCR')
#    SQLLite
#    insServCredQry = """
#                    INSERT INTO SERVER_CRED(
#                           SERV_ID,                       
#                           UNAME,                        
#                           PWD,                           
#                           DESCR           
#                           )
#                    VALUES(
#                      ?,   -- SERV_ID
#                      ?,   -- UNAME,
#                      ?,   -- PWD,  
#                      ?    -- DESCR 
#                 ) """
#
#
#    updCredStatQry = """
#                    UPDATE %s
#                    SET STATUS = ?                      
#                    WHERE ID   = ?                        
#                    AND UNAME  = ?                          
#                  """

    # --- Oracle Syntax ---
    # Gral methods use for credentials operations. Need to use table_name
    insCredQry = """ INSERT INTO %s(
                           ID,                       
                           UNAME,                        
                           PWD,                           
                           DESCR           
                           )
                    VALUES(
                      :ID,
                      :UNAME,
                      :PWD,  
                      :DESCR 
                 ) """
    
    updCredQry = """  UPDATE %s
                          SET PWD   = :PWD,
                              DESCR = :DESCR,
                              LOAD_DATE = SYSDATE
                          WHERE ID = :ID
                            AND UNAME   = :UNAME """
                            
    delCredQry = """  DELETE FROM  %s
                          WHERE ID = :ID
                            AND UNAME   = :UNAME """ 
    
    # Server Credential Status  
    updCredStatQry = """  UPDATE %s 
                          SET STATUS = :STATUS
                          WHERE ID   = :ID
                            AND UNAME = :UNAME """                             


#    def bldEnvSumQry(self, env = None, host=None, dbhost = None):
#       ec = ''   
#       
#       if (env):
#         ec = " and i.env_name like '%%%s%%' " %   env      
#       if (host):
#         ec += " and n.host_name like '%%%s%%' " % host
#       if (dbhost):   
#         ec += " and db.db_host like '%%%s%%' " %  dbhost
#       
#       ec += " order by r.env_name "
#       return self.envSumQry + ec
#   
