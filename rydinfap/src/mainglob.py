# Globals.
# Settings

__version__ = '20120228'

imgDir    = ''
logger    = ''
dbHandler = ''
softdepo  = '' # '\\\\goxsf101\imds$\Data Warehouse'
docdepo   = '' # '\\\\goxsf101\imds$\Data Warehouse'
binDir    = r'C:\unix_util'
infadir   = ''
XOR       = ''

pmcmd      = '' 
pmrep      = '' 
infacmd    = '' 
infasetup  = ''
infasrv    = ''

#Data
# Software Inventory
prod_rs       = []
env_rs        = []    
vendor_rs     = []
vend_cont_rs  = []
soft_ver_rs   = []
soft_eol_rs   = []

env_pid       = {}      # Contains env in which key is prod_id.
vend_pid      = {}      # Contains prod in which key is vend_id
vend_cont_pid = {}      # Contains vend_contact in which key is vend_id
soft_ver_pid  = {}      #
soft_eol_pid  = {}

# Informatica Main 
infa_dom_rs  = []       # Use for domain uptime.
infa_pc_rs   = []       # Power Center Env  mg.main_infa_rs   [<procdata.procmain.InfaMainBean instance at 0x00C2C940>, 
infa_pwx_rs  = []       # Power Exchange 
#Lkups
env_lkup     = {}       # 'RepoSel': [('RS_IM_P', 4), ('RS_IM_P', 5), ('RS_IM_Q', 3)], 'DomSel': [('DOM_IM_P', 4), ('DOM_IM_P', 5)],'ServSel':[
user_lkup    = {}
serv_lkup    = {}

#Credentials
serv_cred_rs = []       # Beans of ServCredBean. ServerCredBean: env_bu=DSS env_name=QA serv_id=19 sname(srv)=JBXSD170 alias=DSSQA os= AIX fena= None 
                        # uname=infa pwd=pwd_JBXSD170 desc=None status=1 load_date=2011-06-08 16:57:07
db_cred_rs   = []
dom_cred_rs  = []
repo_cred_rs = []
int_cred_rs  = []



# Lkups
def printGlob():
    myData =[]
    myData.append("Main Global :\n")
    myData.append('imgDir    = %s\n' %    imgDir    ) 
    myData.append('logger    = %s\n' %    logger    )
    myData.append('dbHandler = %s\n' %    dbHandler )
    return ''.join(myData)

def printSoftInv():
    print 'prod_rs       = %s' %  prod_rs 
    print 'env_rs        = %s' %  env_rs       
    print 'vendor_rs     = %s' %  vendor_rs  
    print 'vend_cont_rs  = %s' %  vend_cont_rs  
    print 'env_pid       = %s' %  env_pid
    print 'vend_pid      = %s' %  vend_pid
    print 'vend_cont_pid = %s' %  vend_cont_pid 

def printInfaMain():
    print 'infa_pc_rs  = %s' %  infa_pc_rs 
    print 'infa_pwx_rs = %s' %  infa_pwx_rs 
    print 'env_lkup    = %s' %  env_lkup  
    print 'user_lkup   = %s' %  user_lkup
