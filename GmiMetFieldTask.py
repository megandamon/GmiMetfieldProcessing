#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         November 7th 2006
#
# DESCRIPTION:
# This class contains the information for a MetField regridding task.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants
from CommonUtilities import CommonUtilities

class GmiMetFieldTask:
   
   LENGHTOFEXPECTEDDATESTRING = 8
   
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      
      self.numberOfDays = 0
      self.firstDay = 0
      self.destinationPath = ''
      self.sourcePath = ''
      self.filePrefix = ''
      self.year = ''
      self.month = ''
      self.day = ''
      self.complete = False
      self.archType = ''
      
      GmiMetFieldTask.constants = GmiAutomationConstants ()
      
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine verifies that all necessary task fields have been defined.
   #---------------------------------------------------------------------------   
 
   def verifyCompleteness (self):
      
      if self.numberOfDays == 0:
         return self.constants.INCOMPLETEDATA
      if self.firstDay == 0:
         return self.constants.INCOMPLETEDATA
      if len (self.destinationPath) == 0:
         return self.constants.INCOMPLETEDATA
      if len (self.sourcePath) == 0:
         return self.constants.INCOMPLETEDATA
      if len (self.filePrefix) == 0:
         return self.constants.INCOMPLETEDATA
      if len (self.year) != 4:
         return self.constants.INCOMPLETEDATA
      if len (self.month) != 2:
         return self.constants.INCOMPLETEDATA
      if len (self.day) != 2:
         return self.constants.INCOMPLETEDATA
      
      return self.constants.NOERROR
  
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
   # This routine sets the year, month and day variables in the class instance
   # to respective values in the date string passed in
   # the formation of the date string is expected to be in : YYYYMMDD
   #---------------------------------------------------------------------------   
   
   def setDate (self, theDate):
      
      if len (theDate) < GmiMetFieldTask.LENGHTOFEXPECTEDDATESTRING:
         return GmiMetFieldTask.constants.INVALIDINPUT  
      
      self.year = theDate [0:4]
      self.month = theDate [4:6]
      self.day = theDate [6:8]
      
      return GmiMetFieldTask.constants.NOERROR
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine adds the a path the corresponds to the date to the basePath
   # if no year, month, or day information is present in the self object
   # this routines raises an exception
   #---------------------------------------------------------------------------      
   
   def addDatePathToBasePath (self, basePath):
      
      if len (self.year) != 4 or len (self.month) != 2 or len (self.day) != 2:
         raise self.constants.INVALIDINPUT   
      
      returnPath = '/Y' + self.year + '/M' + self.month + '/D' + self.day
      return returnPath


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine returns a string that describes the task.
   #---------------------------------------------------------------------------      

   def returnDescriptorString (self):

      descriptor =              "Num days         : " + str (self.numberOfDays) + "\n"
      descriptor = descriptor + "First day        : " + str (self.firstDay) + "\n"
      descriptor = descriptor + "Destination path : " + self.destinationPath + "\n"
      descriptor = descriptor + "Source path      : " + self.sourcePath + "\n"
      descriptor = descriptor + "File prefix      : " + self.filePrefix + "\n"
      descriptor = descriptor + "Year             : " + self.year + "\n"
      descriptor = descriptor + "Month            : " + self.month + "\n"
      descriptor = descriptor + "Day              : " + self.day + "\n"
      descriptor = descriptor + "Complete         : " + str (self.complete) + "\n"

      return descriptor

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Creates a task from the input array.
   #---------------------------------------------------------------------------
   def createTaskFromData (self, data):

      utilities = CommonUtilities ()
      
      if len (data) == 0:
         raise self.constants.INVALIDINPUT

      try:
         self.firstDay = utilities.returnValue (data, "START_DATE")
         self.numberOfDays = int (utilities.returnValue (data, "NUM_DAYS"))
         self.sourcePath = utilities.returnValue (data, "SOURCE_PATH")
         self.destinationPath = utilities.returnValue (data, \
                                                       "DESTINATION_PATH")
         self.filePrefix = utilities.returnValue (data, "FILE_PREFIX")
         self.setDate (self.firstDay)
      except:
         raise self.constants.INVALIDINPUT
      
      
   
