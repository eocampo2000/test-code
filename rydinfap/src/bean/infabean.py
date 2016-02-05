'''
Created on Jan 4, 2012

@author: eocampo
'''
__version__ = '20130530'

# Use for informatica commands.
class InfaCmdBean:
    dom_host   = None
    dom_name   = None  
    dom_port   = None
    dom_node   = None
    dom_dbhost = None
    dom_dbservice = None
    dom_dbport = -1
    dom_dbtype = None
    dom_dbuser = None
    dom_dbpwd  = None
    dom_dbxpwd = None
    rep_name   = None
    rep_user   = None
    rep_pwd    = None
    rep_xpwd   = None
    rep_dbname = None
    rep_dbuser = None
    rep_dbpwd  = None
    rep_dbxpwd = None
    IS         = None
    rep_bkup   = None
    dom_bkup   = None
    eFlg       = True   # Exit on Error Flag 
    xpwd_dbg   = False
    
    #'ID,NAME,VEND_ID'
    def setInfaCmdBean(self,data,idx):  
        self.dom_host   = data[idx.DOM_HOST ] 
        self.dom_name   = data[idx.DOM_NAME ] 
        self.dom_port   = data[idx.DOM_PORT ]
        self.dom_node   = data[idx.DOM_NODE ]
        self.dom_dbhost = data[idx.DDB_HOST ]
        self.dom_dbservice = data[idx.DDB_SERVICE ]
        self.dom_dbport = data[idx.DDB_PORT ]
        self.dom_dbtype = data[idx.DDB_TYPE ]
        self.dom_dbuser = data[idx.DDB_USER ]
        self.dom_dbpwd  = data[idx.DDB_PWD  ]    
        self.dom_dbxpwd = data[idx.DDB_XPWD ]    
        self.rep_name   = data[idx.REP_NAME ] 
        self.rep_user   = data[idx.REP_USER ]   
        self.rep_pwd    = data[idx.REP_PWD  ]  
        self.rep_xpwd   = data[idx.REP_XPWD ]  
        self.rep_dbname = data[idx.DB_NAME  ] 
        self.rep_dbuser = data[idx.DB_USER  ] 
        self.rep_dbpwd  = data[idx.DB_PWD   ]
        self.rep_dbxpwd = data[idx.DB_XPWD  ]
        self.IS         = data[idx.IS       ]
        self.rep_bkup   = data[idx.REPO_BKUP]
        self.dom_bkup   = data[idx.DOM_BKUP ]
        self.eFlg       = data[idx.EFLG     ]
        self.xpwd_dbg   = data[idx.XPWD_DBG ]
  
    def getInfaCmdBean(self):    
        return [ 
              self.dom_host   ,                
              self.dom_name   ,
              self.dom_port   ,
              self.dom_node   ,
              self.dom_dbhost ,
              self.dom_dbservice,
              self.dom_dbport ,
              self.dom_dbtype ,
              self.dom_dbuser ,
              self.dom_dbpwd  ,
              self.dom_dbxpwd ,
              self.rep_name   ,
              self.rep_user   ,
              self.rep_pwd    ,
              self.rep_xpwd   ,
              self.rep_dbname ,               
              self.rep_dbuser , 
              self.rep_dbpwd  ,
              self.rep_dbxpwd ,
              self.IS         ,
              self.rep_bkup   ,   
              self.dom_bkup   ,
              self.eFlg       ,
              self.xpwd_dbg   ,
                ]
                     
    def __str__(self):
        myData =[]
        myData.append("InfaCmdBean:\n")
        myData.append("dom_host    = %s\n" % self.dom_host   )
        myData.append("dom_name    = %s\n" % self.dom_name   )
        myData.append("dom_port    = %s\n" % self.dom_port   )
        myData.append("dom_node    = %s\n" % self.dom_node   )
        myData.append("dom_dbhost  = %s\n" % self.dom_dbhost )
        myData.append("dom_dbservice  = %s\n" % self.dom_dbservice )
        myData.append("dom_dbport  = %s\n" % self.dom_dbport )
        myData.append("dom_dbtype  = %s\n" % self.dom_dbtype )
        myData.append("dom_dbuser  = %s\n" % self.dom_dbuser )
        myData.append("dom_dbpwd   = %s\n" % self.dom_dbpwd  )       
        myData.append("dom_dbxpwd  = %s\n" % self.dom_dbxpwd )       
        myData.append("rep_name    = %s\n"  % self.rep_name  )  
        myData.append("rep_user    = %s\n" % self.rep_user   )
        myData.append("rep_pwd     = %s\n" % self.rep_pwd    )
        myData.append("rep_xpwd    = %s\n" % self.rep_xpwd   )
        myData.append("rep_dbname  = %s\n" % self.rep_dbname )
        myData.append("rep_dbuser  = %s\n" % self.rep_dbuser )
        myData.append("rep_dbpwd   = %s\n" % self.rep_dbpwd  )
        myData.append("rep_dbxpwd  = %s\n" % self.rep_dbxpwd )
        myData.append("IS          = %s\n" % self.IS         )
        myData.append("rep_bkup    = %s\n" % self.rep_bkup   )   
        myData.append("dom_bkup    = %s\n" % self.dom_bkup   )   
        myData.append("eFlg        = %s\n" % self.eFlg       )
        myData.append("xpwd_dbg    = %s\n" % self.xpwd_dbg   )
        return ''.join(myData)  
    
# Informatica Environments.
class InfaDomBean:
    dom_id    = -1                     
    serv_id   = -1           
    dbrepo_id = -1           
    env_id    = -1               
    env_name  = None               
    bu        = None                    
    dom_name  = None           
    dom_port  = None           
    version   = None           
    patch     = None               
    env_dir   = None           
    env_file  = None           
    serv_name = None                    
    alias     = None               
    os        = None               
    ver       = None                    
    patch     = None               
    db_host   = None           
    db_alias  = None           
    db_ver    = None               
    db_type   = None           
    db_name   = None           
    db_ident  = None
    db_port   = -1
    db_uname  = None 
    db_pwd    = None

    def setInfaDomBean(self,data,idx):  
        self.dom_id    = data[idx.DOM_ID   ]
        self.serv_id   = data[idx.SERV_ID  ]
        self.dbrepo_id = data[idx.DBREPO_ID]
        self.env_id    = data[idx.ENV_ID   ]
        self.env_name  = data[idx.ENV_NAME ]
        self.bu        = data[idx.BU       ]
        self.dom_name  = data[idx.DOM_NAME ]
        self.dom_port  = data[idx.DOM_PORT ]
        self.version   = data[idx.VERSION  ]
        self.patch     = data[idx.PATCH    ]
        self.env_dir   = data[idx.ENV_DIR  ]
        self.env_file  = data[idx.ENV_FILE ]
        self.serv_name = data[idx.SERV_NAME]
        self.alias     = data[idx.ALIAS    ]
        self.os        = data[idx.OS       ]
        self.ver       = data[idx.VER      ]
        self.patch     = data[idx.PATCH    ]
        self.db_host   = data[idx.DB_HOST  ]
        self.db_alias  = data[idx.DB_ALIAS ]
        self.db_ver    = data[idx.DB_VER   ]
        self.db_type   = data[idx.DB_TYPE  ]
        self.db_name   = data[idx.DB_NAME  ]
        self.db_ident  = data[idx.DB_IDENT ]
        self.db_port   = data[idx.DB_PORT  ]
        self.db_uname  = data[idx.DB_UNAME ]
        self.db_pwd    = data[idx.DB_PWD   ]


    def getInfaDomBean(self):    
        return [ 
               self.dom_id    , 
               self.serv_id   , 
               self.dbrepo_id , 
               self.env_id    , 
               self.env_name  , 
               self.bu        , 
               self.dom_name  , 
               self.dom_port  , 
               self.version   , 
               self.patch     , 
               self.env_dir   , 
               self.env_file  , 
               self.serv_name , 
               self.alias     , 
               self.os        , 
               self.ver       , 
               self.patch     , 
               self.db_host   , 
               self.db_alias  , 
               self.db_ver    , 
               self.db_type   , 
               self.db_name   , 
               self.db_ident  , 
               self.db_port   , 
               self.db_uname  , 
               self.db_pwd    ,] 

    def __str__(self):
        myData =[]
        myData.append("dom_id    = %s\n" %  self.dom_id   )
        myData.append("serv_id   = %s\n" %  self.serv_id  )
        myData.append("dbrepo_id = %s\n" %  self.dbrepo_id)
        myData.append("env_id    = %s\n" %  self.env_id   )
        myData.append("env_name  = %s\n" %  self.env_name )
        myData.append("bu        = %s\n" %  self.bu       )
        myData.append("dom_name  = %s\n" %  self.dom_name )
        myData.append("dom_port  = %s\n" %  self.dom_port )
        myData.append("version   = %s\n" %  self.version  )
        myData.append("patch     = %s\n" %  self.patch    )
        myData.append("env_dir   = %s\n" %  self.env_dir  )
        myData.append("env_file  = %s\n" %  self.env_file )
        myData.append("serv_name = %s\n" %  self.serv_name)
        myData.append("alias     = %s\n" %  self.alias    )
        myData.append("os        = %s\n" %  self.os       )
        myData.append("ver       = %s\n" %  self.ver      )
        myData.append("patch     = %s\n" %  self.patch    )
        myData.append("db_host   = %s\n" %  self.db_host  )
        myData.append("db_alias  = %s\n" %  self.db_alias )
        myData.append("db_ver    = %s\n" %  self.db_ver   )
        myData.append("db_type   = %s\n" %  self.db_type  )
        myData.append("db_name   = %s\n" %  self.db_name  )
        myData.append("db_ident  = %s\n" %  self.db_ident )
        myData.append("db_port   = %s\n" %  self.db_port  )
        myData.append("db_uname  = %s\n" %  self.db_uname )
        myData.append("db_pwd    = %s\n" %  self.db_pwd   )
        return ''.join(myData)
    

class InfaWFBean:
    subj = None               
    wkfl = None  
    
    def setInfaWFBean(self,data,idx):  
        self.subj = data[idx.SUBJ ]
        self.wkfl = data[idx.WKFL ]

    def getInfaWFBean(self):    
        return [ 
               self.subj , 
               self.wkfl , 
               ]       
        
    def __str__(self):
        myData =[]
        myData.append("subj = %s\n" %  self.subj )
        myData.append("wkfl = %s\n" %  self.wkfl )       
        return ''.join(myData)

    
class WkfStatusBean:
    
    subj    = None
    wkfl    = None
    st_time = None
    end_tim = None
    status  = None
    err_cd  = -1
    err_msg = None

    def setWkfStatusBean(self,data,idx):  
        self.subj     = data[idx.SUBJ    ]
        self.wkfl     = data[idx.WKFL    ]
        self.sess     = data[idx.SESS    ] 
        self.st_time  = data[idx.ST_TIME ]
        self.end_time = data[idx.END_TIME]
        self.status   = data[idx.STAT    ]
        self.err_cd   = data[idx.ERR_CODE]
        self.err_msg  = data[idx.ERR_MSG ]

    def getWkfStatusBean(self):  
        return [
             self.subj     ,
             self.wkfl     ,
             self.sess     ,
             self.st_time  ,
             self.end_time ,
             self.status   ,
             self.err_cd   ,
             self.err_msg  ,
                ]

    def __str__(self):
        myData =[]
        myData.append("subj    = %s\n" % self.subj )
        myData.append("wkfl    = %s\n" % self.wkfl )       
        myData.append("sess    = %s\n" % self.sess     )
        myData.append("st_time = %s\n" % self.st_time  )
        myData.append("end_time= %s\n" % self.end_time )
        myData.append("status  = %s\n" % self.status   )
        myData.append("err_cd  = %s\n" % self.err_cd   )
        myData.append("err_msg = %s\n" % self.err_msg  )        
        return ''.join(myData)
  
    