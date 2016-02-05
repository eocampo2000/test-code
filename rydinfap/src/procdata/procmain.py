'''
Created on Nov 11, 2010

@author: EMO0UZX
# Description: Process Data for/from main application. 
20131114  Modified DB related FX to accommodate changes on dbutil.
'''
__version__ = '20131114'

#--------------  EO Modified for web2py. Fully qualify the package:

#from applications.softinv.modules.datastore.dbutil    import getIDX
#from applications.softinv.modules.datastore.dbmain    import DBMain
#from applications.softinv.modules.bean.mainbean       import *

#--------------  End of Modification 
from datastore.dbutil    import getIDX      # Do not comment use invoke dynamically @ runtime
from datastore.dbmain    import DBMain      # Do not comment use invoke dynamically @ runtime
from datastore.dbinfa    import DBInfaAdmin # Do not comment use invoke dynamically @ runtime
from bean.mainbean       import *           # Do not comment use invoke dynamically @ runtime
from bean.infabean       import *           # Do not comment use invoke dynamically @ runtime



def classFactory(className,classType,init):
    from new import classobj
    return classobj(className,(classType,),init)

# Base class for all db queries.
# sl Based on the return list, elements that will not be included. Those elements that are to be excluded need to 
#    be at the end of the list.
# dbh existing db handlet.
# log handler.   
# Note can raise TypeError if one of the bindings is None
# md.dbHandler = DBSQLLite( "%s\\data\\softinv.dbf" % md.appPath, md.logger) 

class _Proc():
    def __init__(self,t,dbh,log,sl):
        self.qry       = t       
        self.dbHandler = dbh
        self.logger    = log
        self.strpLst   = sl       
    
    # Select result set, need to be one row only.
    # If the select succeeds will populate cInst with the appropriate Bean, returning an instance of the bean.
    # otherwise returns {}
    # This method uses the setBeanXXXX method.

    def select(self,id):
        cInst = None
        qryStr = "%s WHERE ID = %d " % eval("%sDB.sel%sQry,id" % (self.qry,self.qry))  
        rs  = self.dbHandler.runQry(qryStr) 
        self.logger.debug("id = %d table = %s rs len = %d \nQry=%s " % (id,self.qry,len(rs),qryStr))
        if (rs is not None and len(rs) == 1 ):  
            cInst = {}
            className=  '%sBean' % self.qry 
            classObj = classFactory(className,eval(className),{})
            cInst = classObj()
            self.logger.debug("Class name = %s DIR = %s" %  (classObj.__name__ , dir(classObj)))               
            eval('cInst.set%sBean(rs[0],getIDX(%sDB.sel%sIdx))' % (self.qry,self.qry,self.qry))
        
        return cInst
    
    # Select all records from a given table
    # Returns an array of beans, specific to the table. 0 if table empty !
    def selectAll(self):
        cInstLts = None
        qryStr = eval("DBMain.sel%sQry" % (self.qry))
        rs  = self.dbHandler.runQry(qryStr)
        self.logger.debug("table = %s rs len = %d \nQry=%s " % (self.qry,len(rs),qryStr))
        if (rs is not None and len(rs) > 0 ):  
            cInstLts = []
            for r in rs:
                className=  '%sBean' % self.qry 
                classObj = classFactory(className,eval(className),{})
                cInst = classObj()
                #self.logger.debug("Class name = %s DIR = %s" %  (classObj.__name__ , dir(classObj)))               
                eval('cInst.set%sBean(r,getIDX(DBMain.sel%sIdx))' % (self.qry,self.qry))
                cInstLts.append(cInst)       
        return cInstLts

#===============================================================================   
# This class will work with the DS for the Software Inventory Driver.
# Constructor argument t :  table name to query.
# Will instantiate the appropriate Bean class and will return the bean information.
# examples of Main Bean classes : ServGralBean
#===============================================================================  
class ProcRyderInfa(_Proc):
    
    def __init__(self,t,dbh,log):
        _Proc.__init__(self,t,dbh,log,sl=0)
        
#===============================================================================   
# This class will work with the DS for the .
# Constructor argument t :  table name to query.
#===============================================================================  
class ProcServer(_Proc):

    # Use for credentials only as it stores results from different tables on CredIDBean    
    def selectAllCred(self):
        cred = None
        qryStr = eval("DBMain.sel%sQry" % (self.qry))
        rs  = self.dbHandler.runQry(qryStr)
        self.logger.debug("table = %s rs len = %d \nQry=%s " % (self.qry,len(rs),qryStr))
        if (rs is not None and len(rs) > 0 ):
            cred = []  
            for r in rs:
                o = CredIDBean()
                o.setCredIDBean(r,getIDX(DBMain.selCredIDIdx))
                cred.append(o)       
        return cred
    
    def __init__(self,t,dbh,log):
        _Proc.__init__(self,t,dbh,log,sl=0)
        
    def insCred(self, tn, ds):
        rs  = self.dbHandler.exeQry(DBMain.insCredQry % tn,ds)
        return rs 
    
    def updCred(self, tn, ds):
        rs  = self.dbHandler.exeQry(DBMain.updCredQry % tn,ds)
        return rs 
    
    def delCred(self, tn, ds):
        rs  = self.dbHandler.exeQry(DBMain.delCredQry % tn,ds)
        return rs 
    
    def updCredStat(self,tn,ds):
        rs  = self.dbHandler.exeQry(DBMain.updCredStatQry % tn,ds)
        return rs 
    
class ProcInfaDB():
    
    def __init__(self,t,dbh,log):
        self.qry       = t       
        self.dbHandler = dbh
        self.logger    = log
 
    # Use for Informatica Admin/App Scripts    
    # Select all records from a given table
    # Returns an array of beans, specific to the table. 0 if table empty !
    def selectAll(self):
        cInstLts = None
        qryStr = eval("DBInfaAdmin.sel%sQry" % (self.qry))
        rs  = self.dbHandler.runQry(qryStr)
        self.logger.debug("table = %s rs len = %d \nQry=%s " % (self.qry,len(rs),qryStr))
        if (rs is not None and len(rs) > 0 ):  
            cInstLts = []
            for r in rs:
                className=  '%sBean' % self.qry 
                classObj = classFactory(className,eval(className),{})
                cInst = classObj()
                #self.logger.debug("Class name = %s DIR = %s" %  (classObj.__name__ , dir(classObj)))               
                eval('cInst.set%sBean(r,getIDX(DBInfaAdmin.sel%sIdx))' % (self.qry,self.qry))
                cInstLts.append(cInst)       
        return cInstLts