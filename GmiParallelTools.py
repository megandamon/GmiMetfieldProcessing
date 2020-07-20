#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         August 8th 2008
#
# DESCRIPTION:
# This class provides routines relating to serial, shared, and distributed
# architecture scenerios.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants
from CommonUtilities import CommonUtilities
from IoRoutines import IoRoutines

import _thread
from time import time
import os
import sys

class GmiParallelTools:
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self, archType):
      
      self.archType = archType
      self.constants = GmiAutomationConstants ()
      self.uniqueId = None
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine verifies that the architecture type is supported.
   #---------------------------------------------------------------------------   
 
   def verifyArchitectureType (self):

      if self.archType != 'serial' and \
         self.archType != 'shared_mem' and \
         self.archType != 'dist_mem':

         raise self.archType + " not supported!"
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine calls an independent task depending on architectur type
   #---------------------------------------------------------------------------   
 
   def executeOrGetIndependentTasks (self, sysCommands, theFunc, numArgs, *args):


      self.verifyArchitectureType ()

      print("numArgs:", numArgs)
      print("args:", args)

      # copy arguments into a new array
      newArgs = []
      for argument in args:
         newArgs.append(argument)

      if self.archType == 'serial':
         newArgs.append('yes')
         sysCommands.append (theFunc(newArgs))

      elif self.archType == 'shared_mem':
         newArgs.append('yes')
         _thread.start_new (theFunc, \
                           (newArgs,))
         
      elif self.archType == 'dist_mem':
         print("in dist_mem")
         newArgs.append('no')
         print("newArgs: ", newArgs)
         sysCommands.append (newArgs)


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This writes tasks to a independent tasks to a file and executes them.
   #---------------------------------------------------------------------------   
 
   def distributeIndependentTasksAndExec (self, sysCommands):

      self.verifyArchitectureType ()
      
      if self.archType != 'dist_mem':
         message = "The distributeIndependentTasksAndExec " + \
                   "routine is only intended for distributed architectures!"
         print(message)
         return

      execFileName = self.prepareExecFile (sysCommands)
      if os.path.exists(execFileName): print("it exists!")
      
      self.callPodsAndExec (execFileName)
      print("after calls PodsAndExec")
      self.removeFilesFromTemp ()
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Will remove everything from the TMPPATH with the self unique ID.
   #---------------------------------------------------------------------------   
 
   def removeFilesFromTemp (self):

      systemCommand = ""
      
      if self.uniqueID != None and len(self.uniqueID) >= 0:
         if os.path.exists (self.constants.TMPPATH):
            systemCommand = self.constants.RMPATH + "rm -rf " \
                            + self.constants.FINAL_TMPPATH + "*" + self.uniqueID
            os.system(systemCommand)

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Will call pods to run an exec file on distributed nodes.
   #---------------------------------------------------------------------------   
 
   def callPodsAndExec (self, execFileName):

      if os.path.exists (execFileName): print("stil exists!")
      else: print("where did it go?") 
      cmd = self.constants.PODSPATH + "pods.py " + execFileName + " 1"
      print(cmd)
      os.system (cmd)
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This writes tasks to a independent tasks to a file and executes them.
   #---------------------------------------------------------------------------   
 
   def prepareExecFile (self, sysCommands):
      ioRoutines = IoRoutines ()
      
      envFile = self.constants.ENVFILE

      # all task files have to include environment information
      taskFileLines = []
      taskFileLines.append ("#!" + self.constants.BASHPATH)
      taskFileLines.append (". " + envFile)

      count = 0
      self.uniqueID = str(time())
      execFile = [] # execution file contains all task scripts
      for cmd in sysCommands:

         # create a task file each independent task
         taskFile = self.constants.TMPPATH + \
                    'indp_task' + str(count) + '_' + self.uniqueID
         taskFileLines.append (cmd)
         ioRoutines.writeToFile(taskFileLines, taskFile)

         
         # pop the command in preperation for next task
         taskFileLines.pop ()

         execFile.append(self.constants.FINAL_TMPPATH + \
                         'indp_task' + str(count) + '_' + self.uniqueID)
         count = count + 1

      execFile.append ("")
      execFileName = self.constants.FINAL_TMPPATH + \
                   'run_tasks' + '_' + self.uniqueID

      ioRoutines.writeToFile(execFile, execFileName)

      # copy task files to FINAL_TMPPATH
      systemCommand = 'cp ' + self.constants.TMPPATH + 'indp_task*' + \
                      self.uniqueID + '* ' + \
                      self.constants.FINAL_TMPPATH

      os.system(systemCommand)
      
      os.system ("chmod 750 " + self.constants.FINAL_TMPPATH + \
                 "*" + self.uniqueID)

      return execFileName
