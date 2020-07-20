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
      print(systemCommand)
      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != 0:
         print("\n There was an error getting the files with pattern ", pattern, " from archive\n")
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

      print(path, pattern)
      
      # get a listing of the files 
      autoTool = GmiAutomationTools ()
      
      # add a "/" to the end of the path if it is not there
      if path [len(path)-1] != '/':
         path = path + "/"

      systemCommand = self.constants.SSHPATH + 'ssh ' + \
                      remoteSystem + ' ' + self.constants.DMGETPATH \
                      + 'dmget ' + path + pattern + '*'
   
      print(systemCommand)
      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != self.constants.SYS_SUCCESS:
         print("\n There was an error getting the files with pattern ", pattern, " from archive\n")
         return self.constants.BADSYSTEMRETURNCODE 

      exitMutex.acquire ()
      return self.constants.NOERROR
