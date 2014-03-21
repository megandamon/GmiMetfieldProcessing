#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         November 7th 2006
#
# DESCRIPTION:
# This class contains methods for GmiAutomationTools tasks.
#------------------------------------------------------------------------------

import os
import sys
import datetime
import re
from time import time, sleep
from GmiMetFieldTask import GmiMetFieldTask
from IoRoutines import IoRoutines
from GmiAutomationConstants import GmiAutomationConstants
from CommonUtilities import CommonUtilities

class GmiAutomationTools:
   
   
   LINESPERINPUTENTRY = 5.0
   LINESPERINPUTENTRYINTEGER = 5
   DEFAULTNUMBEROFDAYS = 1

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      
      GmiAutomationTools.constants = GmiAutomationConstants ()

      self.tasks = []
      self.numberOfEntries = 0
      self.ioRoutines = IoRoutines ()
      self.comUtilities = CommonUtilities ()
 
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
   # This routine checks for the existence of the fileName.
   #---------------------------------------------------------------------------  
   
   def checkForSpecialInputFile (self, fileName):
 
      if len (fileName) == 0: 
         return GmiAutomationTools.constants.ERROR   
      
      if not os.path.exists (fileName):
         return GmiAutomationTools.constants.NOSUCHFILE 
         
      return GmiAutomationTools.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine reads the input file and returns appropriate error messages.
   # The routine uses the class attribute, LINESPERINPUTENTRY, to determine
   # if file contains the correct number of lines.
   # The file may contains multiple entires with LINESPERINPUTENTRY lines.
   #---------------------------------------------------------------------------  
   
   def readInputFile (self, fileName):
      
      if self.checkForSpecialInputFile (fileName) != GmiAutomationTools.constants.NOERROR:
         return GmiAutomationTools.constants.READERROR

      try:
         fileObject = open (fileName, 'r')
         fileContents = fileObject.read ()
         fileObject.close ()
      except:
         return GmiAutomationTools.constants.READERROR   

      if len (fileContents) == 0:
        return GmiAutomationTools.constants.BADFILENAME
      
      fileLines = fileContents.splitlines()
      
      if len (fileLines) % GmiAutomationTools.LINESPERINPUTENTRY != 0:
         return GmiAutomationTools.constants.INCOMPLETEFILE     

      # there are LINESPERINPUTENTRY per entry
      self.numberOfEntries = len (fileLines) / GmiAutomationTools.LINESPERINPUTENTRY
      
      lineCounter = 0 

      if len (self.tasks) == 0:
         entryCounter = 0
      else:
         entryCounter = len (self.tasks)
         
      while lineCounter < len (fileLines):
         
         self.tasks.append (GmiMetFieldTask ())
         
         self.tasks[entryCounter].numberOfDays = int (fileLines [lineCounter])
         lineCounter = lineCounter + 1

         self.tasks[entryCounter].firstDay = fileLines [lineCounter]
         lineCounter = lineCounter + 1        

         self.tasks[entryCounter].destinationPath = fileLines [lineCounter]
         lineCounter = lineCounter + 1

         self.tasks[entryCounter].sourcePath = fileLines [lineCounter] 
         lineCounter = lineCounter + 1
         
         self.tasks[entryCounter].filePrefix = fileLines [lineCounter]
         lineCounter = lineCounter + 1
         
         returnCode = self.tasks[entryCounter].setDate (self.tasks[entryCounter].firstDay)
         if returnCode != self.constants.NOERROR:
            return self.constants.ERROR
         
         entryCounter = entryCounter + 1
         
      
      return GmiAutomationTools.constants.NOERROR
         
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine looks for the input file in the current
   # directory and fills the tasks object with the information
   # from file or the default information for the current day
   #--------------------------------------------------------- ------------------  
         
   def getGmiMetFieldTasks (self, inputFileName, destinationPath, defaultForecastSourcePath):
            
      returnCode = self.checkForSpecialInputFile (inputFileName)
      if returnCode == GmiAutomationTools.constants.ERROR:
         return returnCode
      
      if returnCode != self.constants.NOSUCHFILE:
         returnCode = self.readInputFile (inputFileName)
         if returnCode != GmiAutomationTools.constants.NOERROR:
            return returnCode
      else:
         returnCode = self.getDefaultForecastTaskConfiguration (destinationPath, \
                                                                defaultForecastSourcePath)
         if returnCode != GmiAutomationTools.constants.NOERROR:
            return returnCode
         
      return GmiAutomationTools.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine fills tasks[0] with the default configuration for a 
   # task
   #--------------------------------------------------------------------------- 
   
   def getDefaultForecastTaskConfiguration (self, destinationPath, defaultForecastSourcePath):
      
         self.tasks.append (GmiMetFieldTask ())
         self.tasks[0].numberOfDays = 1
         self.tasks[0].firstDay = self.getCurrentDate ()
         self.tasks[0].filePrefix = 'a_flk_04.'
         self.tasks[0].setDate (self.tasks[0].firstDay)
         self.tasks[0].sourcePath = defaultForecastSourcePath
         self.tasks[0].destinationPath = destinationPath
                  
         return GmiAutomationTools.constants.NOERROR 
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine returns the current date in the format: YYYYMMDD
   #---------------------------------------------------------------------------  
         
   def getCurrentDate (self):
      
      today = datetime.datetime.date(datetime.datetime.now())
      splitToday = re.split('-', str(today))
      newToday = splitToday[0] + splitToday[1] + splitToday[2]
      
      return newToday
       
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine prints the current tasks.
   #---------------------------------------------------------------------------  
   
   def printTasks (self):
      
      if len (self.tasks) == 0:
         print "There are no tasks."
         return self.constants.NOERROR
      
      print "There are", len (self.tasks), "task(s)."
      print "\n"
      
      taskCounter = 0
      while taskCounter < len (self.tasks):
         
         print "Task", taskCounter+1, ":"
         print "Number of days to process:", self.tasks[taskCounter].numberOfDays
         print "First day:", self.tasks[taskCounter].firstDay
         print "Destination path:", self.tasks[taskCounter].destinationPath
         print "Source path:", self.tasks[taskCounter].sourcePath
         print "File prefix:", self.tasks[taskCounter].filePrefix
         print "Year:", self.tasks[taskCounter].year
         print "Month:", self.tasks[taskCounter].month
         print "Day:", self.tasks[taskCounter].day
         print "\n"
         
         taskCounter = taskCounter + 1
      
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine returns a local file listing from the given file patter.
   # The file prefix is expected to include a path if outside the working
   # directory.  Wildcards are not expected as part of the filePattern!
   # DO NOT USE wildcards, such as *!
   #---------------------------------------------------------------------------    
   
   def getMatchingFiles (self, path, filePattern):
      
      if len (filePattern) == 0:
         raise self.constants.INVALIDPATTERN
      
      if not os.path.exists (path):
         raise self.constants.NOSUCHPATH
      
      fileList =  os.listdir (path) 
      
      if filePattern == '*': 
            return fileList
   
      returnedFileList = []
      
      for file in fileList:

         if re.search (filePattern, file):
            returnedFileList.append (file)
      
      returnedFileList.sort ()
         
      return returnedFileList
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine creates the directory structure if it does not exist.  It
   # will exit on error if it is unable to create a directory.
   #---------------------------------------------------------------------------  
   
   def createDirectoryStructure (self, path):
      
      if len (path) <= 0:
         return self.constants.INVALIDINPUT
      
      directories = re.split('/', str(path))
      growingPath = ''
      for directory in directories:
         
         growingPath = growingPath + directory + '/'
         
         if not os.path.exists (growingPath):

            systemCommand = self.constants.MKDIRPATH + 'mkdir ' + growingPath
            systemReturnCode = os.system (systemCommand)
            if systemReturnCode != 0:
               print "Error! returnCode is: ", systemReturnCode, " after attempting to create the directory: ", growingPath, "\n"
               return self.constants.BADSYSTEMRETURNCODE 
         
      return self.constants.NOERROR
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine adds the specified path to the list of file names given.
   #---------------------------------------------------------------------------    
   
   def addPathToFileNames (self, path, fileNames):
      
      if len (fileNames) <= 0:
         raise self.constants.INVALIDFILENAMES
      
      returnedFileNames = []
      for fileName in fileNames:
         
         if len (fileName) == 0:
             raise self.constants.INVALIDFILENAMES
         
         newFileName = path + fileName
         returnedFileNames.append (newFileName)
         
      return returnedFileNames

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine simply creates a new file from the fileName
   #---------------------------------------------------------------------------  
   
   def touchNewFile (self, fileName):     

      fileObject = open (fileName, 'w')
      fileObject.close ()
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine will read the given file, determine the last date processed,
   # and increment the date and return it.
   #--------------------------------------------------------------------------- 

   def getNextDateFromFile (self, fileName, processingDir):
      
      if len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         return self.constants.NOSUCHFILE
      
      # read the last date processed in from file
      try:
         fileObject = open (fileName, 'r')
         fileContents = fileObject.read ()
         fileObject.close ()
      except:
         print "Error! returnCode is: ", returnCode, " after attempting to read ", fileName, "\n"
         return self.constants.READERROR
      
      fileLines = fileContents.splitlines()
      numberOfEntries = len (fileLines)
      lastDateProcessed = fileLines [numberOfEntries -1]
      nextDate = self.incrementDate (lastDateProcessed, processingDir)
      
      return nextDate

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine simply returns the next day.
   #--------------------------------------------------------------------------- 
   
   def incrementDate (self, theDate, runPath):
      
      # call external program to increment the date
      tempFileName = "out.out" + str(time())
      systemCommand = runPath + './incrdate ' + theDate + ' >& ' + tempFileName
      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != 0:
         print "Error! returnCode is: ", systemReturnCode, " after attempting to use the incrdate program!", "\n"
         os.remove (tempFileName)
         return self.constants.BADSYSTEMRETURNCODE 
      
      # read the next day in from file
      try:
         fileObject = open (tempFileName, 'r')
         fileContents = fileObject.read ()
         fileObject.close ()
         os.remove (tempFileName)
      except:
         print "Error after reading the next day from file!"
         os.remove (tempFileName)
         return self.constants.READERROR
      
      fileLines = fileContents.splitlines()
      nextDate = fileLines [0]
      
      return nextDate
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine fills tasks[0] with the default configuration for a 
   # DAS task.
   #--------------------------------------------------------------------------- 
   
   def getDefaultDasTaskConfiguration (self, fileName, processDirectory, \
                                       dasDestinationPath, defaultDasSourcePath):


      if len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
      self.tasks.append (GmiMetFieldTask ())
      
      # get the next day in the file
      self.tasks[0].firstDay = self.getNextDateFromFile (fileName, processDirectory)
      if len (self.tasks[0].firstDay) != GmiMetFieldTask.LENGHTOFEXPECTEDDATESTRING:
         print "There was an error getting the NextDateFromFile !\n"
         return self.constants.ERROR
      
      self.tasks[0].numberOfDays = 1
      self.tasks[0].filePrefix = 'a_llk_04.'
      self.tasks[0].setDate (self.tasks[0].firstDay)
      self.tasks[0].sourcePath = defaultDasSourcePath
      self.tasks[0].destinationPath = dasDestinationPath
            
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine expects an GmiAutomationTools object with at least one task
   # in it's task list.  It will create more tasks if the numberOfDays to 
   # process is greater than one.  It will simple append tasks to the list
   # and increment each date by one day.
   #--------------------------------------------------------------------------- 
   
   def createAdditionalTasks (self, processingDir):
      
      if len (self.tasks) <= 0:
         return self.constants.INVALIDINPUT
      
      newTasks = []

      for task in self.tasks:

         # make sure the task info is complete
         returnCode = task.verifyCompleteness ()
         if returnCode != self.constants.NOERROR:
            return self.constants.INVALIDINPUT
         
         # add a new task
         # only add one, because the current
         # task will be the one that represents
         # the first day
         loopCounter = 1
         

         nextDate = task.firstDay
         while loopCounter < task.numberOfDays:

            # add a new blank task
            newTasks.append (GmiMetFieldTask ())

            # set the values of the new task
            # to the old one
            newTasks [len(newTasks)-1].sourcePath = task.sourcePath 
            newTasks [len(newTasks)-1].destinationPath = task.destinationPath
            newTasks [len(newTasks)-1].filePrefix = task.filePrefix
            
            # make each task just say one day 
            newTasks [len(newTasks)-1].numberOfDays = 1
            
            nextDate = self.incrementDate (nextDate, processingDir)

            # then, change the date   
            newTasks [len(newTasks)-1].setDate (nextDate)
            newTasks [len(newTasks)-1].firstDay = nextDate
            newTasks [len(newTasks)-1].sourcePath = task.sourcePath 
            newTasks [len(newTasks)-1].destinationPath = task.destinationPath

            loopCounter = loopCounter + 1

         # now that this task has been looked at, set it to 1 day
         task.numberOfDays = 1
         
      # append new tasks to tasks list
      self.tasks.extend (newTasks)
            
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine writes all tasks in the self object to the file.
   #--------------------------------------------------------------------------- 

   def writeAllTasksToFile (self, fileName):

      for task in self.tasks:
         returnCode = self.writeTaskToFile (task, fileName)
         if returnCode != self.constants.NOERROR:
            return returnCode

      return self.constants.NOERROR
   

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This will verify that the task is complete, create a file, or just
   # append to it the task information.
   #--------------------------------------------------------------------------- 
   
   def writeTaskToFile (self, task, fileName):
      
      if len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
       # make sure the task info is complete
      returnCode = task.verifyCompleteness ()
      if returnCode != self.constants.NOERROR:
         return self.constants.INVALIDINPUT

      if not os.path.exists (fileName):
         self.touchNewFile (fileName)
         
      try:
         fileObject = open (fileName, 'a')
         toWrite = str (task.numberOfDays) + '\n'
         fileObject.write (toWrite)
         toWrite = str (task.firstDay) + '\n'
         fileObject.write (toWrite)
         fileObject.write (task.destinationPath + '\n')
         fileObject.write (task.sourcePath  + '\n')
         fileObject.write (task.filePrefix + '\n')
         fileObject.close ()      
      except:
         print "There was an error appending to the file: ", fileName, "\n"
         return self.constants.WRITEERROR
      
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine removes the last task from the file.
   #---------------------------------------------------------------------------   
   
   def removeTaskFromFile (self, fileName, task):
      
      if len (fileName) <= 0:
         raise self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         raise self.constants.NOSUCHFILE
      
      # make sure the task info is complete
      returnCode = task.verifyCompleteness ()
      if returnCode != self.constants.NOERROR:
         raise self.constants.INVALIDINPUT
      
      try:
         fileObject = open (fileName, 'r')
         fileContents = fileObject.read ()
         fileObject.close
      except:
         raise self.constants.READERROR
      
      fileLines = fileContents.splitlines()
      
      if len (fileLines) <= 0:
         return self.constants.NOERROR
      
      os.remove (fileName)
      
      
      newFileLines = []
      
      lineCounter = 0
      while lineCounter < len (fileLines): 
         
         if fileLines [lineCounter] == task.firstDay:
            
            # pop the previous line, which belongs to this task
            if len (newFileLines) > 1:
               newFileLines.pop (lineCounter-1)
            else:
               newFileLines = []
               
            # skip the next 3 lines, which belong to this task
            lineCounter = lineCounter + 3
            
         else:
            newFileLines.append (fileLines [lineCounter])
         
         lineCounter = lineCounter + 1
      
      if len (newFileLines) <= 0:
         return self.constants.NOERROR
      
      try:
         fileObject = open (fileName, 'w')
         for line in newFileLines:
            fileObject.write (line + '\n')
         fileObject.close
      except:
         raise self.constants.WRITEERROR
      
      
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine removes duplicates from the tasks list.
   #---------------------------------------------------------------------------   
   
   def removeDuplicateTasks (self):
      

      # no tasks should not be an error
      # return because there is nothing to do 
      if len (self.tasks) <= 1:
         return self.constants.NOERROR
      
      newTasks = []
      loopCounter = 0
      while loopCounter < len (self.tasks):
         

         foundDuplicateTask = False
     
         innerCounter = loopCounter + 1    
         while innerCounter < len (self.tasks):
            
            if self.tasks[loopCounter].numberOfDays == self.tasks[innerCounter].numberOfDays and \
               self.tasks[loopCounter].firstDay == self.tasks[innerCounter].firstDay and \
               self.tasks[loopCounter].destinationPath == self.tasks[innerCounter].destinationPath and \
               self.tasks[loopCounter].sourcePath == self.tasks[innerCounter].sourcePath and \
               self.tasks[loopCounter].filePrefix == self.tasks[innerCounter].filePrefix and \
               self.tasks[loopCounter].year == self.tasks[innerCounter].year and \
               self.tasks[loopCounter].month == self.tasks[innerCounter].month and \
               self.tasks[loopCounter].day == self.tasks[innerCounter].day:

               foundDuplicateTask = True
               
            innerCounter = innerCounter + 1
            
         if foundDuplicateTask == False:
            newTasks.append(self.tasks[loopCounter])
         
         loopCounter = loopCounter + 1
      
      
      self.tasks = newTasks

      return self.constants.NOERROR


   #----------------------------------------------------------------
   # Write new tasks to file in case unexpected aborted
   # execution occurs.
   #----------------------------------------------------------------
   def writeNewTasksToFile (self, taskFile, mailTo):

      tempFile = taskFile + ".tmp"
      if self.writeAllTasksToFile (tempFile) \
             != GmiAutomationTools.constants.NOERROR:
         
         self.ioRoutines.mailMessage ("Problem writing tasks to file " + \
                                      tempFile, \
                                      GmiAutomationTools.constants.ERROR_SUBJECT, \
                                      mailTo)
         raise GmiAutomationTools.constants.ERROR 
      
      elif os.system (GmiAutomationTools.constants.MVPATH + "/mv " + \
                      tempFile + " " + taskFile) \
                      != GmiAutomationTools.constants.SYS_SUCCESS:
         self.ioRoutines.mailMessage ("Problem writing the file " + \
                                      taskFile, \
                                      GmiAutomationTools.constants.ERROR_SUBJECT, mailTo)
         raise GmiAutomationTools.constants.ERROR
      

   #----------------------------------------------------------------
   # Look for a file with attempted tasks that did complete due
   # unexpected problems.  Remove duplicate tasks.
   #----------------------------------------------------------------
   def readAttemptedTasks (self, taskFile, mailTo):

      if os.path.exists (taskFile):
         returnCode = self.readInputFile (taskFile)
         
         if returnCode != GmiAutomationTools.constants.NOERROR:
            self.ioRoutines.mailMessage ("Problem reading " + taskFile \
                                         , GmiAutomationTools.constants.ERROR_SUBJECT, mailTo)
            raise (returnCode)
         
         

   def createTasks (self, configOptions, processDirectory, mailTo, \
                 realTime, taskFile):

      
      #----------------------------------------------------------------
      # Create a single task
      #----------------------------------------------------------------
      self.tasks = []
      self.tasks.append (GmiMetFieldTask())
   
      #----------------------------------------------------------------
      # Set up tasks based on the START_DATE and NUM_DAYS
      #----------------------------------------------------------------
      self.tasks[0].createTaskFromData (configOptions)
      
      self.createAdditionalTasks (processDirectory)

      #----------------------------------------------------------------
      # Modifications for real-time mode support
      #----------------------------------------------------------------

      # If additional tasks were created and the mode is REAL_TIME
      # there is some conflict; real time is intended to process
      # one day at a time following operations
      if len(self.tasks) > 1 and realTime[0:4] == "true":
         self.ioRoutines.mailMessage ("If REAL_TIME mode is true then " + \
                                      "only one tasks at a time is allowed", \
                                      GmiAutomationTools.constants.ERROR_SUBJECT, \
                                      mailTo)
         raise (GmiAutomationTools.constants.ERROR)
      
      if realTime[0:4] == "true":
         realTimeDate = self.tasks[0].year + \
                        self.tasks[0].month + \
                        self.tasks[0].day 
         
         currentDirectory = os.getcwd()
         nextRealTimeDate = self.incrementDate (realTimeDate, currentDirectory + "/")
         
      #----------------------------------------------------------------
      # End modifications for real-time mode
      #----------------------------------------------------------------

      try:
         self.readAttemptedTasks (taskFile, mailTo)
         self.removeDuplicateTasks ()
         self.writeNewTasksToFile (taskFile, mailTo)
         #self.printTasks ()
      except:
         raise
      
