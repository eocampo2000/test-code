'''
Created on Jan 25, 2012

@author: eocampo
'''

# USe for informatica commands.
class TaskRefBean:
    busTskNm = None
    taskDate = None  
    jobName  = None
    sev      = -1
    
    #'ID,NAME,VEND_ID'
    def setTaskRefBean(self,data,idx):  
        self.busTskNm  = data[idx.BUSTSK]
        self.tskDate   = data[idx.TSKDT ] 
        self.jobName   = data[idx.JOBNM ]
        self.sev       = data[idx.SEV   ]

    def getTaskRefBean(self):    
        return [ 
                 self.busTskNm ,
                 self.taskDate ,  
                 self.jobName  ,
                 self.sev      ,                
                ]
                     
    def __str__(self):
        myData =[]
        myData.append("TaskRefBean:\n")
        myData.append("busTskNm = %s\n" %  self.busTskNm )  
        myData.append("taskDate = %s\n" %  self.tskDate  )
        myData.append("jobName  = %s\n" %  self.jobName  )
        myData.append("sev      = %s\n" %  self.sev      )

        return ''.join(myData)  
