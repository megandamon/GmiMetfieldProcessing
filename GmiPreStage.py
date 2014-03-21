#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         January 16th 2007
#
# DESCRIPTION:
# This class contains routines for pre-staging both DAS and forecast data.
# WARNING: these routines may be heavily dependant on the implementation of
# GMI and/or GMAO GEOS-4 and may be quickly outdated.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants
from GmiMetFieldTask import GmiMetFieldTask
from GmiAutomationTools import GmiAutomationTools

# These are for GEOS4 task processing
from GmiDasFieldsInstantSurface import GmiDasFieldsInstantSurface
from GmiDasFieldsAveragedSurface import GmiDasFieldsAveragedSurface
from GmiDasFieldsAveragedEta import GmiDasFieldsAveragedEta

import os

class GmiPreStage:
   
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      
      self.constants = GmiAutomationConstants ()
  
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Destructor routine.
   #---------------------------------------------------------------------------    
   
   def __del__(self):
      pass

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine will prestage the task data by spawning a new process
   # for each type of data in a DAS task.
   # This task will verify that a task is complete, but the caller of this
   # routine should not attempt to pass an invalid array element
   #---------------------------------------------------------------------------   
   
   def preStageDasTask (self, task, defaultDasInputPath, instantSurfaceDas, \
                        averagedSurfaceDas, \
                        averagedEtaDas):
      
      # verify the task as complete
      returnCode = task.verifyCompleteness ()
      if returnCode != self.constants.NOERROR:
         return self.constants.INVALIDINPUT
      
      # prestage instant surface fields
      instantSurfaceDas = GmiDasFieldsInstantSurface (defaultDasInputPath, instantSurfaceDas)
      path = task.sourcePath + instantSurfaceDas.LLKDIRECTORY + "/Y" + task.year + "/M" + task.month 
      pattern = task.filePrefix + instantSurfaceDas.PREFIX + task.year + task.month + task.day 
      returnCode = self.preStageData (path, pattern)
      if returnCode != self.constants.NOERROR:
         print "\nError pre-staging instant surface fields\n"
         return returnCode
      
      # prestage averaged surface fields
      averagedSurfaceDas = GmiDasFieldsAveragedSurface (defaultDasInputPath, averagedSurfaceDas)
      path = task.sourcePath + averagedSurfaceDas.LLKDIRECTORY + "/Y" + task.year + "/M" + task.month 
      pattern = task.filePrefix + averagedSurfaceDas.PREFIX + task.year + task.month + task.day 
      returnCode = self.preStageData (path, pattern)
      if returnCode != self.constants.NOERROR:
         print "\nError pre-staging averaged surface fields\n"
         return returnCode
       
      # prestage averaged eta fields 
      averagedEtaDas = GmiDasFieldsAveragedEta (defaultDasInputPath, averagedEtaDas)
      path = task.sourcePath + averagedEtaDas.LLKDIRECTORY + "/Y" + task.year + "/M" + task.month 
      pattern = task.filePrefix + averagedEtaDas.PREFIX + task.year + task.month + task.day 
      returnCode = self.preStageData (path, pattern)
      if returnCode != self.constants.NOERROR:
         print "\n Error pre-staging averaged eta fields\n"
         return returnCode
       
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine will attempt to prestage the data relevant to the path
   # and file pattern given.
   #--------------------------------------------------------------------------- 
   
   def preStageData (self, path, pattern):
      
      # get a listing of the files 
      autoTool = GmiAutomationTools ()
      
      # add a "/" to the end of the path if it is not there
      if path [len(path)-1] != '/':
         path = path + "/"
      
      systemCommand = '/usr/bin/dmget ' + path + pattern + '*'
      print systemCommand
      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != 0:
         print "\n There was an error getting the files with pattern ", pattern, " from archive\n"
         return self.constants.BADSYSTEMRETURNCODE 

      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine will attempt to prestage the data relevant to the path
   # and file pattern given.
   #--------------------------------------------------------------------------- 
   
   def preStageRemoteData (self, path, pattern, remoteSystem, exitMutex):

      print path, pattern
      
      # get a listing of the files 
      autoTool = GmiAutomationTools ()
      
      # add a "/" to the end of the path if it is not there
      if path [len(path)-1] != '/':
         path = path + "/"

      systemCommand = self.constants.SSHPATH + 'ssh ' + \
                      remoteSystem + ' ' + self.constants.DMGETPATH \
                      + 'dmget ' + path + pattern + '*'
   
      print systemCommand
      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != self.constants.SYS_SUCCESS:
         print "\n There was an error getting the files with pattern ", pattern, " from archive\n"
         return self.constants.BADSYSTEMRETURNCODE 

      exitMutex.acquire ()
      return self.constants.NOERROR
