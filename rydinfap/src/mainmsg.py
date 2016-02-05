'''
Created on Jan 3, 2012

@author: eocampo
'''
__version__ = '20120224'

# DB ERRORS
DB_GRAL_ERR    = -1
DB_CON_ERR     = -101
DB_CON_HDL_ERR = -102
DB_CONSTR_ERR  = -103
DB_OPER_ERR    = -104
DB_NO_HANDLER  = -105

# APP ERRORS
INV_LOAD_FILENAME = -200
INV_CRED_TBL_KEY  = -201
SNAME_NOT_FOUND   = -203
INV_ID_DTYPE      = -204
INV_TBL_KEY       = -205
INV_SRV_KEY       = -206

# I/O Errors
FILE_NOT_EXIST   = -300
NOT_FILE_OBJ     = -301

# INFA ERRORS
PMCMD_INV_APP_ARGS    = -400        # Missing dict keys for fld,wkfl.
PMCMD_INV_SIZE_ARGS   = -401        # Empty elememnts keys for fld,wkfl.