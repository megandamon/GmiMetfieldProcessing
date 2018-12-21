
#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         October 11 2007
#
# DESCRIPTION:
# This class contains routines for common tasks associated with automation
#------------------------------------------------------------------------------

import os
import sys
import re
from time import time, sleep

from GmiAutomationConstants import GmiAutomationConstants

class CommonUtilities:


    #---------------------------------------------------------------------------  
    # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
    #
    # DESCRIPTION: 
    # Constructor.
    #---------------------------------------------------------------------------  

    def __init__ (self):
        self.constants = GmiAutomationConstants ()

    #---------------------------------------------------------------------------  
    # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
    #
    # DESCRIPTION: 
    # Destructor.
    #---------------------------------------------------------------------------  
    
    def __del__ (self):
        self.constants.__del__ ()

    
    def isValueInList (self, value, list):

        for item in list:
            if value == item: return item

        return None


    def getCreateRemoteDirCmd (self, remoteSys, directory):

        sysCommand = self.constants.SSHPATH + 'ssh ' + \
                     remoteSys + ' mkdir -p ' + directory
        
        return sysCommand

    def qsubFileAndWait (self, fileName):

        if len(fileName) == 0:
            raise self.constants.ERROR

        systemCommand = self.constants.QSUBPATH + "qsub " + fileName
        print "in qsub : ", systemCommand

        try:            
            child = os.popen(systemCommand)
            jobId = child.read()
            jobId = jobId.rstrip ('\n')
            err = child.close()
            self.watchQueueJob (jobId) 
        except:
            raise

        return jobId


    def watchQueueJob (self, jobId):


        if len (jobId) == 0:
            raise self.constants.ERROR

        qstatCommand = self.constants.QSTATPATH + "qstat " + jobId + \
                       " | " + self.constants.GREPPATH + "grep " + \
                       ' ".borg" | ' + self.constants.AWKPATH + \
                       "awk '{print $1}'"

        print qstatCommand
    
        child = os.popen(qstatCommand)

        theOutput = child.read()
        theOutput = theOutput.rstrip ('\n')
        err = child.close()
        
        while theOutput == jobId:
            sleep (30)
            
            child = os.popen(qstatCommand)
            theOutput = child.read()
            theOutput = theOutput.rstrip ('\n')
            err = child.close()
        


    def transferFile (self, fileName, remoteSystem, remotePath):

        if len(fileName) == 0:
            raise self.constants.ERROR

        systemCommand = self.constants.SCPPATH + "scp " + fileName + \
                        " " + remoteSystem + ":" + remotePath

        if os.system (systemCommand) != self.constants.SYS_SUCCESS:
            raise self.constants.SSHERROR
        


    #---------------------------------------------------------------------------  
    # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
    #
    # DESCRIPTION: 
    # This routine takes an array in the following format:
    # INDENTIFIER value
    # The INDENTIFIER should be ALL CAPS and should be
    # seperated from the value by a ":"
    #---------------------------------------------------------------------------  
    
    def returnValue (self, list, id):

        if len(list) == 0:
            raise (self.constants.NOSUCHIDENTIFIER)
            return

        for item in list:
            splitList = re.split (':', item)
            if splitList[0] == id:
                return splitList[1]

        raise (self.constants.NOSUCHIDENTIFIER)
            

    #---------------------------------------------------------------------------  
    # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
    #
    # DESCRIPTION: 
    # This routine takes an array in the following format:
    # INDENTIFIER value
    # The INDENTIFIER should be ALL CAPS and should be
    # seperated from the value by a ":"
    # A name of an item in the list and the value it should be set to.
    #---------------------------------------------------------------------------  
    
    def updateValue (self, list, name, value):

        if len(list) == 0:
            raise (self.constants.NOSUCHIDENTIFIER)
            return

        newList = []
        for item in list:
            splitList = re.split (':', item)
            if splitList[0] == name:
                newList.append (name+":"+value)
            else:
                newList.append (item)
                            
        return newList


    def justQuit (self):
        sys.exit (0)

         
    def releaseLockFileAndQuit (self, lockFile):
        os.remove (lockFile)
        sys.exit (0)



