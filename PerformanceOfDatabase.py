import psutil
import shutil
import os
from datetime import datetime
import writeToErrorsInLogFiles
from timeit import default_timer
class PerformanceOfDatabase:

    
    def TimePerformanceStart(self):
        self.timeStart = datetime.now()
        self.second=default_timer()
        

    def TimePerformanceEnd(self,which_case,file_mb):
        self.timeEnd = datetime.now()
        self.timeDifference = self.timeEnd-self.timeStart
        self.second=default_timer()-self.second 
        self.pipeline=file_mb/self.second
        print(which_case," TIME : ",self.timeDifference)
        print(which_case," DATA PIPELINE : ",self.pipeline,"MB/SN")
        return self.pipeline,self.timeDifference

    def RamPerformance(self,which_case):
        print(which_case," RAM usage percent: %",psutil.virtual_memory())
        

    def CpuPerformance(self,which_case):
        print(which_case," CPU usage percent: %",psutil.cpu_percent())
    



        