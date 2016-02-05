'''
Created on Nov 11, 2010

@author: EMO0UZX

# This module contains bean definitions for main objects.
# IDX is an enumeration type defined in maindb.py which is a client of this module.
# Note: use None to enforce NOT NULL constraint at DB level
# Each class represents a Business Object.
# set method is used for selects, it converts a list into a bean.
# get method is used for inserts/updates, it converts a bean into a list.
# Use table name as bean basename e.g. table prod , 
# 
# 20110211 EO Added ServerCredentials (ServerCredBean).
'''

__version__ = '20120923'

class EnvBean:
    
    envname   = None  
    bu        = None
    prod_id   = -1
    servname  = None
    alias     = None
    os        = None
    os_ver    = None
    os_patch  = None
    binary    = None
    ver       = None
    patch     = None
    lic_path  = None
    
    #'ENVNAME,BU,PROD_ID,SERVNAME,ALIAS,OS,OS_VER,OS_PATCH,BINARY,VER,PATCH,LIC_PATH'        
    def setEnvBean(self,data,idx):  
        self.envname   = data[idx.ENVNAME   ] 
        self.bu        = data[idx.BU        ]
        self.prod_id   = data[idx.PROD_ID   ]
        self.servname  = data[idx.SERVNAME  ]      
        self.alias     = data[idx.ALIAS     ]
        self.os        = data[idx.OS        ]
        self.os_ver    = data[idx.OS_VER    ]      
        self.os_patch  = data[idx.OS_PATCH  ]      
        self.binary    = data[idx.BINARY    ]      
        self.ver       = data[idx.VER       ]
        self.patch     = data[idx.PATCH     ]
        self.lic_path = data[idx.LIC_PATH ]

    def getEnvBean(self):    
        return [ 
                self.envname  ,
                self.bu       ,
                self.prod_id  ,
                self.servname ,
                self.alias    ,
                self.os       ,
                self.os_ver   ,
                self.os_patch ,
                self.binary   ,
                self.ver      ,
                self.patch    ,
                self.lic_path,]
                          
    def __str__(self):
        myData =[]
        myData.append("EnvLicBean:\n")
        myData.append("name      = %s\n" % self.envname   )
        myData.append("bu        = %s\n" % self.bu        )
        myData.append("prod_id   = %s\n" % self.prod_id   )  
        myData.append("servername= %s\n" % self.servname  )
        myData.append("alias     = %s\n" % self.alias     )
        myData.append("os        = %s\n" % self.os        )
        myData.append("os_ver    = %s\n" % self.os_ver    )
        myData.append("os_patch  = %s\n" % self.os_patch  )
        myData.append("binary    = %s\n" % self.binary    )
        myData.append("ver       = %s\n" % self.ver       )
        myData.append("patch     = %s\n" % self.patch     )
        myData.append("lic_path  = %s\n" % self.lic_path  )
        return ''.join(myData)

# Product    
class ProdBean:
    id       = -1
    name     = None  
    vend_id  = -1

    #'ID,NAME,VEND_ID'
    def setProdBean(self,data,idx):  
        self.id      = data[idx.ID      ]
        self.name    = data[idx.NAME    ] 
        self.vend_id = data[idx.VEND_ID ]

    def getProdBean(self):    
        return [ 
                self.id      , 
                self.name    ,
                self.vend_id ,
                ]
                     
    def __str__(self):
        myData =[]
        myData.append("ProdBean:\n")
        myData.append("id      = %s\n" % self.id      )  
        myData.append("name    = %s\n" % self.name    )
        myData.append("vend_id = %s\n" % self.vend_id )
        return ''.join(myData)  

class VendorBean:
    id    = -1      
    name  = None
    addr1 = None
    addr2 = None
    zip   = None
    url   = None

    #'ID,NAME,ADDRESS1,ADDRESS2,ZIP,URL'
    def setVendorBean(self,data,idx):  
        self.id    = data[idx.ID       ]
        self.name  = data[idx.NAME     ] 
        self.addr1 = data[idx.ADDRESS1 ]
        self.addr2 = data[idx.ADDRESS2 ]
        self.zip   = data[idx.ZIP      ]
        self.url   = data[idx.URL      ]

    def getVendorBean(self):    
        return [ 
                self.id   , 
                self.name ,
                self.addr1,
                self.addr2,
                self.zip  ,
                self.url  ,
               ]
                   
    def __str__(self):
        myData =[]
        myData.append("VendorBean:\n")
        myData.append("id   = %s\n" % self.id   )  
        myData.append("name = %s\n" % self.name )
        myData.append("addr1= %s\n" % self.addr1)  
        myData.append("addr2= %s\n" % self.addr2)  
        myData.append("zip  = %s\n" % self.zip  )
        myData.append("url  = %s\n" % self.url  )  
        return ''.join(myData)


class VendContBean:
    id         = -1      
    vend_id    = -1  
    title      = None    
    last_name  = None
    first_name = None
    addr1      = None
    addr2      = None
    zip        = None
    ph1        = None
    ph2        = None
    url        = None

    def setVendContBean(self,data,idx):  
        self.id         = data[idx.ID]      
        self.vend_id    = data[idx.VEND_ID]  
        self.title      = data[idx.TITLE]      
        self.last_name  = data[idx.LAST_NAME]
        self.first_name = data[idx.FIRST_NAME]
        self.addr1      = data[idx.ADDRESS1] 
        self.addr2      = data[idx.ADDRESS2] 
        self.zip        = data[idx.ZIP]      
        self.ph1        = data[idx.PH1]      
        self.ph2        = data[idx.PH2]      
        self.url        = data[idx.EMAIL]    

    def getVendorBean(self):    
        return [ 
            self.id          ,
            self.vend_id     ,
            self.title       ,
            self.last_name   ,
            self.first_name  ,
            self.addr1       ,
            self.addr2       ,
            self.zip         ,
            self.ph1         ,
            self.ph2         ,
            self.url         ,  
        ]
                   
    def __str__(self):
        myData =[]
        myData.append("id          = %s\n" % self.id        )
        myData.append("vend_id     = %s\n" % self.vend_id   )
        myData.append("title       = %s\n" % self.title     )
        myData.append("last_name   = %s\n" % self.last_name )
        myData.append("addr1       = %s\n" % self.addr1     )
        myData.append("addr2       = %s\n" % self.addr2     )
        myData.append("zip         = %s\n" % self.zip       )
        myData.append("ph1         = %s\n" % self.ph1       )
        myData.append("ph2         = %s\n" % self.ph2       )
        myData.append("url         = %s\n" % self.url       )
        return ''.join(myData)
    
class SoftVerBean:
    id             = -1      
    vend_id        = -1  
    prod_id        = -1    
    version        = None
    patch          = None
    bin            = None
    dir_path       = None
    down_date      = None
    down_email     = None
    supp_matrix_os = None
    supp_matrix_db = None

    def setSoftVerBean(self,data,idx):  
        self.id          = data[idx.ID  ]           
        self.vend_id     = data[idx.VEND_ID ]      
        self.prod_id     = data[idx.PROD_ID ]        
        self.version     = data[idx.VERSION ]      
        self.patch       = data[idx.PATCH ]                
        self.bin         = data[idx.BIN ]           
        self.dir_path    = data[idx.DIR_PATH ]     
        self.down_date   = data[idx.DOWN_DATE]     
        self.down_email  = data[idx.DOWN_EMAIL ]   
        self.supp_matrix_os = data[idx.SUPP_MATRIX_OS]  
        self.supp_matrix_db = data[idx.SUPP_MATRIX_DB]      

    def getSoftVerBean(self):    
        return [ 
            self.id             ,
            self.vend_id        ,
            self.prod_id        ,
            self.version        ,
            self.patch          ,
            self.bin            ,
            self.dir_path       ,
            self.down_date      ,
            self.down_email     ,
            self.supp_matrix_os ,
            self.supp_matrix_db ,
        ]
                   
    def __str__(self):
        myData =[]
        myData.append("id             = %s\n" % self.id             )
        myData.append("vend_id        = %s\n" % self.vend_id        )
        myData.append("prod_id        = %s\n" % self.prod_id        )
        myData.append("version        = %s\n" % self.version        )
        myData.append("patch          = %s\n" % self.patch          )
        myData.append("bin            = %s\n" % self.bin            )
        myData.append("dir_path       = %s\n" % self.dir_path       )
        myData.append("down_date      = %s\n" % self.down_date      )
        myData.append("down_email     = %s\n" % self.down_email     )
        myData.append("supp_matrix_os = %s\n" % self.supp_matrix_os )
        myData.append("supp_matrix_db = %s\n" % self.supp_matrix_db )
        return ''.join(myData)                        


class SoftEOLBean:
    id              = -1      
    vend_id         = -1  
    prod_id         = -1 
    version         = None
    rel_date        = None
    eostd_supp_date = None
    eoext_supp_date = None
 
    def setSoftEOLBean(self,data,idx):  
    
        self.id                = data[idx.ID  ]              
        self.vend_id           = data[idx.VEND_ID ]      
        self.prod_id           = data[idx.PROD_ID ]      
        self.version           = data[idx.VERSION ]      
        self.rel_date          = data[idx.REL_DATE ]        
        self.eostd_supp_date   = data[idx.EOSTD_DATE ]          
        self.eoext_supp_date   = data[idx.EOEXT_DATE]     

    def getSoftEOLBean(self):    
        return [ 
           self.id              ,
           self.vend_id         ,
           self.prod_id         ,
           self.version         ,
           self.rel_date        ,
           self.eostd_supp_date ,
           self.eoext_supp_date ,
        ]
                   
    def __str__(self):
        myData =[]
        myData.append("id                     = %s\n" % self.id            )
        myData.append("vend_id                = %s\n" % self.vend_id       )
        myData.append("prod_id                = %s\n" % self.prod_id       )
        myData.append("version                = %s\n" % self.version       )
        myData.append("self.rel_date        = %s\n" % self.rel_date        )
        myData.append("self.eostd_supp_date = %s\n" % self.eostd_supp_date )
        myData.append("self.eoext_supp_date = %s\n" % self.eoext_supp_date )
        return ''.join(myData)                        

# This class stores server gral information from several tables.
# EO TODO Revise and remove user/pwd info
class ServGralBean:
    
    id       = -1 
    name     = None
    alias    = None
    os       = None
    ver      = None
    patch    = None
    owner    = None
    uname    = None
    pwd      = None
    descr    = None
    env_bu   = None
    env_name = None
    auth     = None
        
    #ID,ENV_ID,NAME,ALIAS,OS,VER,PATCH,OWNER,UNAME,PWD,DESCR          
    def setServGralBean(self,data,idx):  
        self.id       = data[idx.ID       ]  
        self.name     = data[idx.NAME     ]     
        self.alias    = data[idx.ALIAS    ]     
        self.os       = data[idx.OS       ]     
        self.ver      = data[idx.VER      ]     
        self.patch    = data[idx.PATCH    ]     
        self.owner    = data[idx.OWNER    ]     
        self.uname    = data[idx.UNAME    ]     
        self.pwd      = data[idx.PWD      ]     
        self.descr    = data[idx.DESCR    ]
        self.env_bu   = data[idx.ENV_BU   ]     
        self.env_name = data[idx.ENV_NAME ]  
        self.auth     = data[idx.auth     ]  

    def getServGralBean(self):    
        return [ 
             self.id     ,   
             self.name   ,   
             self.alias  ,   
             self.os     ,   
             self.ver    ,   
             self.patch  ,   
             self.owner  ,   
             self.uname  ,   
             self.pwd    ,
             self.descr  ,
             self.env_bu ,
             self.env_name ,
             self.auth ,]   
                                
    def __str__(self):
        myData =[]
        myData.append("ServerGralBean:\n")
        myData.append("id         = %s\n" %  self.id       )         
        myData.append("name       = %s\n" %  self.name     )
        myData.append("alias      = %s\n" %  self.alias    )
        myData.append("os         = %s\n" %  self.os       )
        myData.append("ver        = %s\n" %  self.ver      )
        myData.append("patch      = %s\n" %  self.patch    )
        myData.append("owner      = %s\n" %  self.owner    )
        myData.append("uname      = %s\n" %  self.uname    )
        myData.append("pwd        = %s\n" %  self.pwd      )
        myData.append("descr      = %s\n" %  self.descr    )
        myData.append("env_bu     = %s\n" %  self.env_bu   )
        myData.append("env_name   = %s\n" %  self.env_name )
        myData.append("auth       = %s\n" %  self.auth     )
        return ''.join(myData)

# Informatica Environments.
class InfaDomBean:
    dom_id    = -1                     
    node_id   = -1           
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
        self.node_id   = data[idx.NODE_ID  ]
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
               self.node_id   , 
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
        myData.append("node_id   = %s\n" %  self.node_id  )
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
    
class InfaPCBean:
    
    env_bu    = None
    env_name  = None 
    serv_name = None  
    serv_os   = None
    serv_ver  = None
    auth      = None
    dom_name  = None
    dom_ver   = None 
    dom_hf    = None
    node_name = None  
    int_name  = None  
    repo_name = None 
    db_ident  = None
    db_ver    = None
 
    def setInfaPCBean(self,data,idx):  
        self.env_bu    = data[idx.ENV_BU    ]
        self.env_name  = data[idx.ENV_NAME  ] 
        self.serv_name = data[idx.SERV_NAME ]  
        self.serv_os   = data[idx.SERV_OS   ]
        self.serv_ver  = data[idx.SERV_VER  ]
        self.auth      = data[idx.AUTH      ]
        self.dom_name  = data[idx.DOM_NAME  ]
        self.dom_ver   = data[idx.DOM_VER   ] 
        self.dom_hf    = data[idx.DOM_HF    ]
        self.node_name = data[idx.NODE_NAME ]  
        self.int_name  = data[idx.INT_NAME  ]  
        self.repo_name = data[idx.REPO_NAME ] 
        self.db_ident  = data[idx.DB_IDENT  ]
        self.db_ver    = data[idx.DB_VER    ]

    def getInfaPCBean(self):    
        return [ 
        self.env_bu    ,
        self.env_name  ,
        self.serv_name ,
        self.serv_os   ,
        self.serv_ver  ,
        self.auth     ,
        self.dom_name  ,
        self.dom_ver   ,
        self.dom_hf    ,
        self.node_name ,
        self.int_name  ,
        self.repo_name ,
        self.db_ident  ,
        self.db_ver    ,]
        

    def __str__(self):
        myData =[]
        myData.append("env_bu    = %s\n" %  self.env_bu    )  
        myData.append("env_name  = %s\n" %  self.env_name  )
        myData.append("serv_name = %s\n" %  self.serv_name )
        myData.append("serv_os   = %s\n" %  self.serv_os   )
        myData.append("serv_ver  = %s\n" %  self.serv_ver  )
        myData.append("auth      = %s\n" %  self.auth      )
        myData.append("dom_name  = %s\n" %  self.dom_name  )
        myData.append("dom_ver   = %s\n" %  self.dom_ver   )
        myData.append("dom_hf    = %s\n" %  self.dom_hf    )
        myData.append("node_name = %s\n" %  self.node_name )
        myData.append("int_name  = %s\n" %  self.int_name  )
        myData.append("repo_name = %s\n" %  self.repo_name )
        myData.append("db_ident  = %s\n" %  self.db_ident  )
        myData.append("db_ver    = %s\n" %  self.db_ver    )
        return ''.join(myData)
      
# Credentials. 
# sname represents the service name use as key.
class ServCredBean:
    
    env_bu     = None  
    env_name   = None
    serv_id    = -1
    sname      = None    # Server Name
    alias      = None
    os         = None
    auth       = None
    uname      = None
    pwd        = None
    desc       = None
    status     = None
    load_date  = None
        
    def setServCredBean(self,data,idx):  
        self.env_bu    = data[idx.ENV_BU    ]     
        self.env_name  = data[idx.ENV_NAME  ]  
        self.serv_id   = data[idx.SERV_ID   ]  
        self.sname     = data[idx.SERV_NAME ]     
        self.alias     = data[idx.ALIAS     ]     
        self.os        = data[idx.OS        ]     
        self.auth      = data[idx.AUTH      ]  
        self.uname     = data[idx.UNAME     ]     
        self.pwd       = data[idx.PWD       ]  
        self.desc      = data[idx.DESC      ]
        self.status    = data[idx.STATUS    ] 
        self.load_date = data[idx.LOAD_DATE ]  

    def getServCredBean(self):    
        return [ 
             self.env_bu    ,
             self.env_name  ,
             self.serv_id   ,   
             self.sname     ,   
             self.alias     ,   
             self.os        ,   
             self.auth      ,  
             self.uname     ,   
             self.pwd       ,
             self.desc      , 
             self.status    , 
             self.load_date , 
             ]

    def __str__(self):
        myData =[]
        myData.append("ServerCredBean:\n")
        myData.append(" env_bu    = %s\n" % self.env_bu   )         
        myData.append(" env_name  = %s\n" % self.env_name )
        myData.append(" serv_id   = %s\n" % self.serv_id  )
        myData.append(" sname(srv)= %s\n" % self.sname    )
        myData.append(" alias     = %s\n" % self.alias    )
        myData.append(" os        = %s\n" % self.os       )
        myData.append(" auth      = %s\n" % self.auth     )
        myData.append(" uname     = %s\n" % self.uname    )
        myData.append(" pwd       = %s\n" % self.pwd      )
        myData.append(" desc      = %s\n" % self.desc     ) 
        myData.append(" status    = %s\n" % self.status   )
        myData.append(" load_date = %s\n" % self.load_date)
        return ''.join(myData)

class DBCredBean:
    
    dbrepo_id = -1
    sname     = None   # DB Name
    db_ident  = None
    db_host   = None
    db_type   = None
    db_port   = None
    db_alias  = None
    db_ver    = None
    auth      = None
    uname     = None
    pwd       = None
    desc      = None
    status    = None
    load_date = None
        
    def setDBCredBean(self,data,idx):  
        self.dbrepo_id = data[idx.DBREPO_ID ]     
        self.sname     = data[idx.DB_NAME   ]  
        self.db_ident  = data[idx.DB_IDENT  ]  
        self.db_host   = data[idx.DB_HOST   ]    
        self.db_type   = data[idx.DB_TYPE   ]      
        self.db_port   = data[idx.DB_PORT   ]     
        self.db_alias  = data[idx.DB_ALIAS  ]  
        self.db_ver    = data[idx.DB_VER    ]  
        self.auth      = data[idx.AUTH      ]  
        self.uname     = data[idx.UNAME     ]     
        self.pwd       = data[idx.PWD       ]     
        self.desc      = data[idx.DESC      ]
        self.status    = data[idx.STATUS    ] 
        self.load_date = data[idx.LOAD_DATE ]  
        
    def getDBCredBean(self):    
        return [ 
             self.dbrepo_id ,    
             self.sname     ,    
             self.db_ident  ,    
             self.db_host   ,    
             self.db_type   ,    
             self.db_port   ,      
             self.db_alias  ,    
             self.db_ver    ,    
             self.auth      ,
             self.uname     ,
             self.pwd       ,
             self.desc      , 
             self.status    , 
             self.load_date , 
             ]     
                                
    def __str__(self):
        myData =[]
        myData.append("DBCredBean:\n")
        myData.append("dbrepo_id  = %s\n" % self.dbrepo_id )         
        myData.append("sname(db)  = %s\n" % self.sname     )
        myData.append("db_ident   = %s\n" % self.db_ident  )
        myData.append("db_host    = %s\n" % self.db_host   )
        myData.append("db_type    = %s\n" % self.db_type   )
        myData.append("db_port    = %s\n" % self.db_port   )
        myData.append("db_alias   = %s\n" % self.db_alias  )
        myData.append("db_ver     = %s\n" % self.db_ver    )
        myData.append("auth       = %s\n" % self.auth      )        
        myData.append("uname      = %s\n" % self.uname     )
        myData.append("pwd        = %s\n" % self.pwd       )
        myData.append("desc       = %s\n" % self.desc      ) 
        myData.append("status     = %s\n" % self.status    )
        myData.append("load_date  = %s\n" % self.load_date )
        return ''.join(myData)

# Infa Domain Credentials
class InfaDomCredBean:
    env_bu    = None  
    env_name  = None
    dom_id    = -1
    sname     = None       #Domain Name
    dom_port  = -1
    serv_name = None
    os        = None
    auth      = None
    uname     = None
    pwd       = None
    desc      = None
    status    = None
    load_date = None
        
    def setInfaDomCredBean(self,data,idx):  
        self.env_bu    = data[idx.ENV_BU    ]     
        self.env_name  = data[idx.ENV_NAME  ]  
        self.dom_id    = data[idx.DOM_ID    ]  
        self.sname     = data[idx.DOM_NAME  ]    
        self.dom_port  = data[idx.DOM_PORT  ]       
        self.serv_name = data[idx.SERV_NAME ]      
        self.os        = data[idx.OS        ]     
        self.auth      = data[idx.AUTH      ]  
        self.uname     = data[idx.UNAME     ]     
        self.pwd       = data[idx.PWD       ] 
        self.desc      = data[idx.DESC      ]
        self.status    = data[idx.STATUS    ] 
        self.load_date = data[idx.LOAD_DATE ]      

    def getInfaDomCredBean(self):    
        return [ 
             self.env_bu    ,    
             self.env_name  ,    
             self.dom_id    ,    
             self.sname     ,    
             self.dom_port  ,    
             self.serv_name ,    
             self.os        ,      
             self.auth      ,    
             self.uname     ,
             self.pwd       ,]     
                                
    def __str__(self):
        myData =[]
        myData.append("InfaDomCredBean:\n")
        myData.append(" env_bu    = %s\n" % self.env_bu    )         
        myData.append(" env_name  = %s\n" % self.env_name  )
        myData.append(" dom_id    = %s\n" % self.dom_id    )
        myData.append(" sname(dom)= %s\n" % self.sname     )
        myData.append(" dom_port  = %s\n" % self.dom_port  )
        myData.append(" serv_name = %s\n" % self.serv_name )
        myData.append(" os        = %s\n" % self.os        )
        myData.append(" auth      = %s\n" % self.auth      )
        myData.append(" uname     = %s\n" % self.uname     )
        myData.append(" pwd       = %s\n" % self.pwd       )
        myData.append(" desc      = %s\n" % self.desc      ) 
        myData.append(" status    = %s\n" % self.status    )
        myData.append(" load_date = %s\n" % self.load_date )
        return ''.join(myData)


class InfaRepoCredBean:
    
    env_bu    = None  
    env_name  = None
    repo_id   = -1
    sname     = None        # Repo Name
    serv_name = None
    os        = None
    auth      = None
    db_name   = None    
    uname     = None
    pwd       = None
    desc      = None
    status    = None
    load_date = None
    
    def setInfaRepoCredBean(self,data,idx):  
        self.env_bu    = data[idx.ENV_BU    ]     
        self.env_name  = data[idx.ENV_NAME  ]  
        self.repo_id   = data[idx.REPO_ID   ]  
        self.sname     = data[idx.REPO_NAME ]    
        self.serv_name = data[idx.SERV_NAME ]      
        self.os        = data[idx.OS        ]     
        self.auth      = data[idx.AUTH      ]  
        self.db_name   = data[idx.DB_NAME   ]       
        self.uname     = data[idx.UNAME     ]     
        self.pwd       = data[idx.PWD       ]   
        self.desc      = data[idx.DESC      ]
        self.status    = data[idx.STATUS    ] 
        self.load_date = data[idx.LOAD_DATE ]    

    def getInfaRepoCredBean(self):    
        return [ 
             self.env_bu    ,    
             self.env_name  ,    
             self.repo_id   ,    
             self.sname     ,    
             self.serv_name ,    
             self.os        ,      
             self.auth      ,    
             self.db_name   ,    
             self.uname     ,
             self.pwd       ,
             self.desc      , 
             self.status    , 
             self.load_date , ]     
                                
    def __str__(self):
        myData =[]
        myData.append("InfaRepoCredBean:\n")
        myData.append("env_bu     = %s\n" % self.env_bu    )         
        myData.append("env_name   = %s\n" % self.env_name  )
        myData.append("repo_id    = %s\n" % self.repo_id   )
        myData.append("sname(rep) = %s\n" % self.sname     )
        myData.append("serv_name  = %s\n" % self.serv_name )
        myData.append("os         = %s\n" % self.os        )
        myData.append("auth       = %s\n" % self.auth      )
        myData.append("db_name    = %s\n" % self.db_name   )
        myData.append("uname      = %s\n" % self.uname     )
        myData.append("pwd        = %s\n" % self.pwd       )
        myData.append("desc       = %s\n"  % self.desc    ) 
        myData.append("status     = %s\n"  % self.status  )
        myData.append("load_date  = %s\n"  % self.load_date)
        return ''.join(myData)

# Use this class to create lkups.    
class CredIDBean:
    id   = -1
    sname = None
    
    def setCredIDBean(self,data,idx):  
        self.id    = data[idx.ID   ]  
        self.sname = data[idx.SNAME ]  
   
    def getCredIDBean(self):    
        return [ 
             self.id     ,   
             self.sname,]   

    def __str__(self):
        myData =[]
        myData.append("CredIDBean:\n")
        myData.append("id        = %s\n" %  self.id    )         
        myData.append("sname     = %s\n" %  self.sname ) 
        return ''.join(myData)


# Credentials. 
# Remote Program Execution
class RemCredBean:
    
    sname      = None    # Server Name
    alias      = None
    os         = None
    auth       = None
    uname      = None
    pwd        = None
        
    def __str__(self):
        myData =[]
        myData.append("RemCredBean:\n")
        myData.append(" sname(srv)= %s\n" % self.sname    )
        myData.append(" alias     = %s\n" % self.alias    )
        myData.append(" os        = %s\n" % self.os       )
        myData.append(" auth      = %s\n" % self.auth     )
        myData.append(" uname     = %s\n" % self.uname    )
        myData.append(" pwd       = %s\n" % self.pwd      )
        return ''.join(myData)

    
# Empty Container
class PlaceHolderBean:
    pass


