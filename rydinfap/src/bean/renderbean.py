'''
Created on Oct 7, 2012

@author: eocampo
'''

class IORateTransBean(object):
    
    st_time  = -1  
    end_time = -1  
    cpu_real = -1
    cpu_user = -1
    cpu_sys  = -1
        
    def setServCredBean(self,data,idx):  
        pass
    
    def getServCredBean(self):    
        return [ 
                self.st_time  , 
                self.cpu_real , 
                self.cpu_user , 
                self.cpu_sys  , 
                self.end_time ,   
             ]

    def __str__(self):
        myData =[]
        myData.append(" IORateTransBean:\n")
        myData.append("st_time  = %s\n" % self.st_time  )         
        myData.append("cpu_real = %s\n" % self.cpu_real )
        myData.append("cpu_user = %s\n" % self.cpu_user )
        myData.append("cpu_sys  = %s\n" % self.cpu_sys  )
        myData.append("end_time = %s\n" % self.end_time )
        return ''.join(myData)
        