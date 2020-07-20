#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         November 28th 2006
#
# DESCRIPTION:
# This class contains the routines for logging entries to file or 
# checking for a logged entry.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants

import os

class GmiLogUtilities:
   
   
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
   # This routine logs the given entry to the given fileName.
   #---------------------------------------------------------------------------       

   def logEntryToFile (self, entry, fileName):
      
      if len (entry) <= 0 or len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         print("log file: ", fileName, " does not exist - creating...", "\n")
         fileObject = open (fileName, 'a')
         fileObject.close ()
         
      fileObject = open (fileName, 'a')
      fileObject.write (entry)
      fileObject.close ()      

      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This reads in a log file and checks for the given entry.
   #---------------------------------------------------------------------------       

   def checkLogForEntry (self, entry, fileName):
      
      if len (entry) <= 0 or len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         return self.constants.NOSUCHFILE
      
      fileObject = open (fileName, 'r')
      fileContents = fileObject.read ()
      fileObject.close ()
      
      fileLines = fileContents.splitlines()
   
      for line in fileLines:
         if line == entry:
            return True

      return False