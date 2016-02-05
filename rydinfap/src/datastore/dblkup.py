#------------------------------------------------------------
# Version 1.0  20110321
#
# dblkup.py
#
# Creation Date:     2011/03/21
# Modification Date:
# Description: This module contains lookup tables DB related functions.
#
# Modification Date: 2009/07/06
#===============================================================================
#===============================================================================


# This lkup are used in webinterfaces.
# Server Lkups
infaLkupEnv      = ('ServSel','DBServSel','DomSel','RepoSel')

lkupServSelQry   = "SELECT DISTINCT ID,NAME FROM SERVER ORDER BY NAME"
lkupDBServSelQry = "SELECT DISTINCT DBREPO_ID,DB_NAME FROM DBREPO ORDER BY DB_NAME"
lkupDomSelQry    = "SELECT DISTINCT DOM_ID,DOM_NAME FROM INFA_DOMAIN ORDER BY DOM_NAME"
lkupRepoSelQry   = "SELECT DISTINCT REPO_ID,SERVICE_NAME FROM INFA_REPO_SERV ORDER BY SERVICE_NAME"

# Users Lkup
infaLkupUser     = ('ServUser','DBUser','DomUser','RepoUser')
lkupServUserQry  = """ SELECT S.NAME,A.UNAME 
                       FROM SERVER_CRED A 
                          JOIN SERVER S ON S.ID = A.ID
                       ORDER BY S.NAME, A.UNAME """

lkupDBUserQry    = """ SELECT D.DB_NAME , A.UNAME
                       FROM DB_CRED A
                         JOIN DBREPO D ON D.DBREPO_ID = A.ID
                       ORDER BY D.DB_NAME , A.UNAME """
                       
lkupDomUserQry   = """ SELECT D.DOM_NAME, A.UNAME 
                       FROM INFA_DOM_CRED A
                         JOIN INFA_DOMAIN D ON D.DOM_ID = A.ID
                       ORDER BY D.DOM_NAME, A.UNAME """ 

lkupRepoUserQry  = """ SELECT D.SERVICE_NAME , A.UNAME
                       FROM INFA_REPO_CRED A
                         JOIN INFA_REPO_SERV D ON D.REPO_ID = A.ID
                       ORDER BY D.SERVICE_NAME , A.UNAME """
