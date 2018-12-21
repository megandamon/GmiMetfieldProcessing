#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         July 2nd 2008
#
# DESCRIPTION:
# This class contains routines and data for dealing with GEOS5-DAS objects.
# Three types of GEOS5-DAS "objects"  are supported - avg3d, avg2d, int2d.
# Edit the GEOS-5 file types by altering PREFIXES, the hour records by RECORDS,
# the field variables as GEOS5FIELDS.
#------------------------------------------------------------------------------

from GmiGEOS5DasFields import GmiGEOS5DasFields
from GmiGEOS5DasAvg2D import GmiGEOS5DasAvg2D
from GmiGEOS5DasAvg3D import GmiGEOS5DasAvg3D
from GmiGEOS5DasInst2D import GmiGEOS5DasInst2D
from GmiAutomationConstants import GmiAutomationConstants
from GmiAutomationTools import GmiAutomationTools
from GmiLogUtilities import GmiLogUtilities
from GmiNetCdfFileTools import GmiNetCdfFileTools
from IoRoutines import IoRoutines
from CommonUtilities import CommonUtilities
from GmiCloud import GmiCloud


import random
import thread
from time import time, sleep
from numpy import *
from pynetcdf import *
import os
import re
import sys 

class GmiDasTasks:
   
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self, realTime):

      self.comUtilities = CommonUtilities ()
      self.autoConstants = GmiAutomationConstants ()
      self.gmiAutomationTool = GmiAutomationTools ()
      self.ioRoutines = IoRoutines ()
      self.netCdfTools = GmiNetCdfFileTools ()
      self.realTime = realTime
      self.logUtility = GmiLogUtilities ()
      self.geos5Fields = GmiGEOS5DasFields ()
      self.basePath = ''

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Destructor routine.
   #---------------------------------------------------------------------------       
   def __del__(self):
      pass   

      
   def processTask (self, task, dasObjects, destPath, processDirectory, \
                    archiveDirectory, remoteSystem, levsFile, constFieldsPath, \
                    mailTo, logFile, \
                    configFile, taskFile, transferFile):

      try:

         #---------------------------------------------------------------------------  
         #Only needed in debugging cases when removing steps below
         #---------------------------------------------------------------------------  
         for dasObject in dasObjects:
            dasObject.SOURCESTYLE = "GMI"
         #---------------------------------------------------------------------------  

         self.basePath = task.destinationPath + "/" + \
                         task.year + "/" + task.month + \
                         "/" + task.filePrefix + "."

         print "Creating necessary directories ..."
         self.createDirectories (task, remoteSystem, archiveDirectory, transferFile, \
                                 processDirectory)
         print "Done creating necessary directories ..."

         # By now, the data has been staged
         task.sourcePath = task.destinationPath
         
         print "Checking for DAS source files ..."
         self.checkSourceFiles (task, dasObjects, mailTo)
         print "Done checking for DAS source files ..."

         print "Distributed processing ..."
         self.doProcessing (task, dasObjects, destPath)
         print "Done with distributed processing ..."         

         print "Doing optical depth calculation"
         self.doOpticalDepth (task, dasObjects)
         print "Done calculating optical depth"   

         # NEED TO MAKE SURE that OPTDEPTH  gets carried along

         print "Merging across DAS types ..."
         self.mergeAcrossDataTypes (task, dasObjects, mailTo, levsFile)
         print "Done merging across DAS types ..."

         print "Adding lat,lev,and time file"
         self.add1DVars (task)
         print "Done adding lat,lev, and time"
         
         print "Remapping the lon coordinate"
         self.remapData (task, mailTo, levsFile)
         print "Done remapping the lon coordinate"

         print "rename Fields..."
         self.renameFields (task)
         print "Done renaming Fields..."

         print "add longtitude array to all files"
         self.addLongitudeToArray (task)
         print "done adding longitude array to all files"

         print "Checking output files ..."
         self.checkFiles (task)
         print "Done checking output files ..."

         print "Archiving the finished met fields and logging ..."
         self.archiveFilesAndLog (task, dasObjects, archiveDirectory, \
                                  remoteSystem, mailTo, logFile, \
                                  transferFile, processDirectory)
         print "Done archiving the finished met fields and logging ..."

         self.removeInterFiles (task, dasObjects)
         print "Done removing intermmediate files ..."

         print "Removing processed files ..."
         self.removeFiles (task)
         print "Done removing processed files ..."

         print "Updating the config file..."
         self.updateConfigFile (task, configFile)
         print "Done updating the config file..."
         
         print "Removing task from list..."
         self.removeTaskFromAttempts (task, taskFile, mailTo)
         print "Done removing task from list..."

         print "Removing processing prefixes and fields"
         self.removeExtraPrefixesAndFields (dasObjects)
         print "Done removing processing prefixes and fields"

         print "Mailing success message..."
         self.ioRoutines.mailMessage ("Succesful completion of the task: " +  \
                                         task.year + " " + task.month + " " + \
                                         task.day, "MERRA-2 Success", mailTo)
         print "Done mailing success message..."
         
      except Exception, error:
         print "GmiDasTasks error: ", error
         self.ioRoutines.mailMessage ("GmiDasTasks error: "+ str(error), \
                                         self.autoConstants.ERROR_SUBJECT, \
                                         mailTo)
                        
             

   def addLongitudeToArray (self, task):
      print "in addLongitudeToArray"

      for resolution in ["2x2.5", "1x1.25", "0.625x0.5"]:
         lonFile = "netcdf/lon." + resolution + ".nc"
         mainFile = self.basePath + task.year + task.month + \
             task.day + "." + resolution + ".nc"

         returnCode = self.netCdfTools.mergeFilesIntoNewFile([lonFile, mainFile], \
                                                                mainFile)
         print "returnCode: ", returnCode



   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Adds 1D vars (like lat, lon, time, etc.) to the files.
   # the 1D var files are created during processing of the avg3d objects
   #---------------------------------------------------------------------------  
   def add1DVars (self, task):            

      #MERRA2_400.20160101.1D.0.625x0.5.nc       
      #MERRA2_400.20160101.0.625x0.5.nc   

      for resolution in ["2x2.5", "1x1.25", "0.625x0.5"]:

         basePathWithDate = self.basePath + task.year + task.month + \
             task.day + "."

         oneDFile = basePathWithDate + "1D." + resolution + ".nc"
         sourceFile = basePathWithDate +  resolution + ".nc"

         fileList = []
         fileList.append (sourceFile)
         fileList.append (oneDFile)
         returnCode = self.netCdfTools.mergeFilesIntoNewFile(fileList, sourceFile)
         if returnCode != self.autoConstants.NOERROR:
            raise "Problem adding 1D variables!", returnCode
                         
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Optical depth is calculated from other fields. 
   #---------------------------------------------------------------------------  
   def doOpticalDepth (self, task, dasObjects):
      cloudObject = GmiCloud()
      returnCode = cloudObject.createCloudVars (task.destinationPath, \
                                                   task.year, task.month, \
                                                   task.day, task.filePrefix)
      if returnCode != self.autoConstants.NOERROR:
         raise "Problem calculating optical depth: ", returnCode


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # The GMI source code expects a certain set of field names
   #---------------------------------------------------------------------------  
   def renameFields (self,task):
      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         sourceFile = self.basePath + task.year + task.month + \
                      task.day + "." + resolution + ".nc"  
         returnCode = self.netCdfTools.renameFields(GmiGEOS5DasFields.oldFieldNames, \
                                                    GmiGEOS5DasFields.newFieldNames,\
                                                    '-v', sourceFile)
         if returnCode != self.autoConstants.NOERROR:
            raise "Problem renaming the ", resolution, " fields!"


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # If new year, a new directory might need to be created
   #---------------------------------------------------------------------------  
   def createDirectories (self, task, remoteSystem, archiveDirectory, \
                          transferFile, processDirectory):
      fullDestination = task.destinationPath + "/" + task.year + "/" \
                        + task.month


      returnCode = self.gmiAutomationTool.createDirectoryStructure \
                   (fullDestination)
      if returnCode != self.autoConstants.NOERROR:
         raise "Problem making the directory: ", fullDestination
      

      # create the three directories (one for each res) on the
      sysCommands = []
      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         sysCommands.append (self.comUtilities.getCreateRemoteDirCmd \
                      (remoteSystem, archiveDirectory + "/" + resolution \
                       + "/" + task.year + "/"))

      uniqueId = str(time()) + str(random.random())
      newTransferFile = transferFile + "." + uniqueId
      jobId = None


      try:
         print "does it exist: ", os.path.exists(newTransferFile)
         self.ioRoutines.appendLinesToFileAndRename (transferFile, \
                                                     sysCommands, \
                                                     newTransferFile)
         jobId = self.comUtilities.qsubFileAndWait (newTransferFile)
      except: raise "Problem creating directories"

      # remove the transfer and output file
      sleepTimes = 0
      transferOutputFile = processDirectory + "/slurm-" + jobId[0:7] + ".out"
      print transferOutputFile
      while not os.path.exists (transferOutputFile) and \
                sleepTimes < 12:
         sleep(30)
         sleepTimes = sleepTimes + 1
      
      if os.path.exists (newTransferFile):
         print "removing file: ", newTransferFile
         os.remove (newTransferFile)



   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # When a "task" (a single model day of processing) is completed
   # (processed and saved to disk), it is removed
   # from the hidden file on disk.
   #----------------------------------------------------------------
   def removeTaskFromAttempts (self, task, taskFile, mailTo):
      returnCode = self.gmiAutomationTool.removeTaskFromFile (taskFile, task)
      if returnCode != self.autoConstants.NOERROR:
         self.ioRoutines.mailMessage ("Problem removing a task from the file " \
                                 + taskFile, self.autoConstants.ERROR_SUBJECT, \
                                 mailTo)
         raise returnCode
      

   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # Make a change in the configuration file on disk.  The format
   # expected is NAME:VALUE.  No consideration for ":" in the
   # VALUE is given.
   #----------------------------------------------------------------
   def updateConfigFile (self, task, configFile):
      if self.realTime[0:4] != "true":
         return

      currentDirectory = os.getcwd ()
      realTimeDate = task.year + task.month + task.day
      nextRealTimeDate = self.gmiAutomationTool.incrementDate \
                         (realTimeDate, currentDirectory + "/")
      
      if task.year + task.month + task.day == realTimeDate:
         configOptions = self.netCdfTools.readFileAndReturnFileLines (configFile)
         newConfigOptions = self.comUtilities.updateValue \
                            (configOptions, "START_DATE", \
                             nextRealTimeDate)
         systemCommand = "cp " + configFile + " " + configFile + ".back"
         os.system (systemCommand)
         
         self.gmiAutomationTool.touchNewFile (configFile)
         self.ioRoutines.writeToFile (newConfigOptions, configFile)

         systemCommand = "rm " + configFile + ".back"
         os.system (systemCommand)

   
   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # This routine is responsible for archiving the finished
   # files and checking them.  The entry for this task is then
   # logged to a log file
   #----------------------------------------------------------------
   def archiveFilesAndLog (self, task, dasObjects, archiveDirectory, \
                           remoteSystem, mailTo, logFile, transferFile, \
                           processDirectory):
      entry = task.filePrefix + task.year + task.month + task.day
      print "entry: ", entry

      fullDestination = task.destinationPath + "/" + task.year + "/" + task.month

      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         fullArchive = archiveDirectory + "/" + resolution + "/" + task.year
         print "fullArchive: ", fullArchive
         systemCommand = "ssh discover01 cp " + fullDestination + "/" \
             + task.filePrefix + "." + task.year \
             + task.month + task.day + "." + resolution + ".nc " \
             + fullArchive
         print systemCommand
         os.system(systemCommand)


#      returnCode = dasObjects[0].archiveGEOSDataToGmiArchive \
#                   (task, archiveDirectory, remoteSystem, transferFile, \
#                    processDirectory)

#      if returnCode != self.autoConstants.NOERROR:
#         errorMsg = "There was a problem archiving " + entry
#         self.ioRoutines.printMessage (self.autoConstants.STD_OUT, \
#                                          errorMsg)
#         raise errorMsg

      returnCode = self.logUtility.logEntryToFile (entry + '\n', logFile)
      if returnCode != self.autoConstants.NOERROR:
         raise "There was a problem logging the entry: " + entry + " to file."
      
            
   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # Another GEOS-5 specific routine.  Simply checks is the
   # task files exist.  Maybe it belongs elsewhere.
   #----------------------------------------------------------------
   def checkFiles (self, task):
      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         thePath = self.basePath + task.year + \
                   task.month + task.day + \
                   "." + resolution + ".nc"
         if not os.path.exists (thePath):
            raise self.autoConstants.NOSUCHFILE + " " + thePath


   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # Another GEOS-5 specific routine.  Simply checks is the
   # task files exist.  Maybe it belongs elsewhere.
   #----------------------------------------------------------------
   def removeFiles (self, task):
      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         theFile = self.basePath + task.year + \
                   task.month + task.day + \
                   "." + resolution + ".nc"
         print "removing the file: ", theFile
         systemCommand = self.autoConstants.RMPATH + \
                         "rm -f " + theFile
         os.system (systemCommand)


   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # Some files for processing could be left in the destination
   # path.
   #----------------------------------------------------------------
   def removeInterFiles (self, task, dasObjects):
      self.basePath = task.destinationPath + "/" + \
          task.year + "/" + task.month + \
          "/" + task.filePrefix + "."
      
      sysCommand = self.autoConstants.RMPATH + "rm --force " \
                   + self.basePath \
                   + "*" + task.year + task.month \
                   + task.day + "*"
      print sysCommand
      os.system (sysCommand)



   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # Three resolutions are hard-coded: 4x5, 2x2.5, 1x1.25 !!
   #----------------------------------------------------------------
   def mergeAcrossDataTypes (self, task, dasObjects, mailTo, levsFile):
      errorFlag = 0
      fileNames = []
      
      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         fileNames = []
         for dasObject in dasObjects:
            fileNames.append (self.basePath + \
                              dasObject.GMIPREFIX + "." + \
                              task.year + task.month + task.day + \
                              "." + resolution + ".nc")
            
            
         sourceFile = self.basePath + task.year + task.month + \
                      task.day + "." + resolution + ".nc"
         returnCode = self.netCdfTools.mergeFilesIntoNewFile \
                      (fileNames, sourceFile)
         if returnCode != self.autoConstants.NOERROR:
            self.ioRoutines.mailMessage ("Problem merging the files: ", \
                                         self.autoConstants.ERROR_SUBJECT, \
                                         mailTo)
            raise "Problem merging across data types: ", returnCode
         
   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   # Three resolutions are hard-coded: 4x5, 2x2.5, 1x1.25 !!
   # Perhaps this class/module (currently GmiDasTasks) should
   # contain that information.
   #----------------------------------------------------------------
   def remapData (self, task, mailTo, levsFile):
      count = 0
      exitMutexes = []
      for resolution in ["2x2.5", "1x1.25", "0.625x0.5"]:
         sourceFile = self.basePath + task.year + task.month + \
             task.day + "." + resolution + ".nc"
         exitMutexes.append (thread.allocate_lock())
         thread.start_new (self.netCdfTools.remapDataOnLonCoordNew, \
                              (sourceFile, levsFile, task.filePrefix, \
                                  exitMutexes[count]))                           
         count = count + 1
         
      #----------------------------------------------------------------
      #  Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass


   
   def removeExtraPrefixesAndFields (self, dasObjects):

      for dasObject in dasObjects:
         if dasObject.TYPE == "avg3D":
            dasObject.PREFIXES.remove("tavg3_3d_cld_Nv_set2")
            print "Removed tavg3_3d_cld_Nv_set2"
            dasObject.PREFIXES.remove("tavg3_3d_mst_Ne_set2")
            print "Removed tavg3_3d_mst_Ne_set2"
            dasObject.PREFIXES.remove("tavg3_3d_asm_Nv_set2")
            print "Removed tavg3_3d_asm_Nv_set2"

         if dasObject.TYPE == "inst2D":
            dasObject.PREFIXES.remove("const_2d_asm_Nx")
            print "Removed const_2d_asm_Nx"
            dasObject.GEOS5FIELDS.remove ("FRLAKE")
            print "Removed FRLAKE"
            dasObject.GEOS5FIELDS.remove ("FRLAND")
            print "Removed FRLAND"
            dasObject.GEOS5FIELDS.remove ("FRLANDICE")
            print "Removed FRLANDICE"
            dasObject.GEOS5FIELDS.remove ("FROCEAN")
            print "Removed FROCEAN"

   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC#
   # Kicks off the three GEOS5-DAS objects in the order that seems
   # to be the most efficient.  There is a assumption regarding
   # the order of the object see C.* below.
   #----------------------------------------------------------------
   def doProcessing (self, task, dasObjects, destPath):
      exitMutexes = []
      threadCount = 0
      sourcePath = task.sourcePath

      for dasObject in dasObjects:
         print "processing dasObject: ", dasObject.TYPE

#         if dasObject.TYPE == "inst2D": # for testing
         if dasObject.TYPE == "avg3D" or dasObject.TYPE == "inst2D":

            print dasObject.TYPE, " started! "
            
            exitMutexes.append (thread.allocate_lock())

            thread.start_new (dasObject.doGEOSFields, \
                              (task, destPath, \
                               exitMutexes[threadCount]))
            
            threadCount = threadCount + 1
      
      
      #C.*
      #we know the first thread is inst2D, the
      # avg3D takes awhile, so start the avg2D
      while not exitMutexes[0].locked ():
         pass

      #C.* now start avg2D, it runs after
      # inst2D
      task.sourcePath = sourcePath
      for dasObject in dasObjects:
         if dasObject.TYPE == "avg2D":

            print dasObject.TYPE, " started! "
         
            exitMutexes.append (thread.allocate_lock())
            thread.start_new (dasObject.doGEOSFields, \
                             (task, destPath, \
                              exitMutexes[threadCount]))
            threadCount = threadCount + 1

      #C.*
      # Wait for "all" threads to complete
      # The avg3D takes the longest
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass


   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA SSAI
   #----------------------------------------------------------------
   def copyMerra2DataToWorkingDir (self, task, dasObjects, remoteSystem, \
                       transferFile, processDirectory, exitMutex, \
                       mailTo, ftpScript):


      print "Copying Merra2 Data"

      print task.addDatePathToBasePath(task.sourcePath)


      fullDestination = task.destinationPath + "/" + task.year + "/" + task.month
      returnCode = self.gmiAutomationTool.createDirectoryStructure (fullDestination)
      if returnCode != self.autoConstants.NOERROR:
         raise "Problem making the directory: ", fullDestination

      print fullDestination
      
      commands = self.constructCpCmdMerra2 (task, dasObjects)
      for command in commands:
         print command
         returnCode = os.system(command)
         if returnCode != 0:
            print "Current command failed!"
            sys.exit(0)

         
   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC#
   # GEOS5-specific.
   #----------------------------------------------------------------
   def constructCpCmdMerra2 (self, task, dasObjects):
      filesToTransfer = []
      scpCmd = "scp "
      fullDestination = task.destinationPath + "/" + task.year + "/" + task.month
      
      dasDir = dasObjects[0].DIR
      
      for dasObject in dasObjects:
         for prefix in dasObject.PREFIXES:
            filesToTransfer.append (scpCmd + task.sourcePath + \
                                       "/" + \
                                       "Y" + task.year + "/M" + \
                                       task.month + "/" + task.filePrefix \
                                       + "." + prefix + "." + \
                                       task.year + task.month + \
                                       task.day + "*nc4 " + fullDestination)
      
      return filesToTransfer

   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC#
   # Prestages DAS files from a remote system.Needs a transfer
   # file template and assumes bbscp as transfer protocol.
   #----------------------------------------------------------------
   def preStageAllDas (self, task, dasObjects, remoteSystem, \
                       transferFile, processDirectory, exitMutex, \
                       mailTo, ftpScript):
      print "in preStageAllDas: ", task.year, task.month, task.day
      listTasks = []
      listTasks.append(task)

      self.stageDataToDestination (task, dasObjects, remoteSystem, \
                                   transferFile, processDirectory, \
                                   ftpScript)
      task.sourcePath = task.destinationPath

      try:
         self.checkSourceFiles (task, dasObjects, mailTo)
      except:
         exitMutex.acquire()
         raise "Problem with source files after pre staging: ", \
             task.year + task.month + task.day
			
         
      exitMutex.acquire()

   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC#
   # Constructs a bbscp batch file, submits its, runs it and waits
   # for it.  Then sets the SOURCESTYLE to GMI.  Search .py
   # files for "SOURCESTYLE" for more information.
   #----------------------------------------------------------------
   def stageDataToDestination (self, task, dasObjects, remoteSystem, transferFile, \
                               processDirectory, ftpScript):
      jobId = None
      if ftpScript != None:
         transferFile = ftpScript
         
      fullDestination = task.destinationPath + "/" + task.year + "/" + task.month
      returnCode = self.gmiAutomationTool.createDirectoryStructure (fullDestination)
      if returnCode != self.autoConstants.NOERROR:
         raise "Problem making the directory: ", fullDestination

      if task.sourcePath == 'ftp':
         systemCommand = "ssh discover29 'bash " + transferFile + " " + task.year + " " \
                         + task.month + " " + task.day + " " \
                         + fullDestination + " '"
         print systemCommand
         os.system(systemCommand)
      else:
         newTransferFile = transferFile + "." + str (time())  + str(random.random())
         print "newTransferFile: ", newTransferFile, " " , task.year, task.month, \
               task.day      
         filesToTransfer = self.constructBbscpCommand (task, dasObjects, remoteSystem)
         sleep(30)

         jobId = None     
         try:
            self.ioRoutines.appendLinesToFileAndRename (transferFile, \
                                                        filesToTransfer, \
                                                        newTransferFile)
            jobId = self.comUtilities.qsubFileAndWait (newTransferFile)
            # remove the transfer and output file         
            if os.path.exists (newTransferFile):
               systemCommand = self.autoConstants.RMPATH + "rm -f " + \
                               newTransferFile
               os.system(systemCommand)
     
               if jobId != None:
                  systemCommand = self.autoConstants.RMPATH + "rm -f " + \
                                  processDirectory + "/*" \
                                  + jobId[0:6] + "*"
                  os.system(systemCommand)
         except: raise
      
      task.sourcePath = task.destinationPath
      for dasObject in dasObjects:
         dasObject.SOURCESTYLE = "GMI"

         
               
   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC#
   #----------------------------------------------------------------
   def checkSourceFiles (self, task, dasObjects, mailTo):      
      for dasObject in dasObjects:
         print "Checking source files for ", dasObject.TYPE
         dasObject.checkGEOSSourceFiles (task)


  
         
   #----------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC#
   # GEOS5-specific.
   #----------------------------------------------------------------
   def constructBbscpCommand (self, task, dasObjects, remoteSystem):
      filesToTransfer = []
      filesToTransfer.append ("\n")
#      bbscpCmd = self.autoConstants.BBSCPPATH + "bbscp " + remoteSystem + ":"
      bbscpCmd = self.autoConstants.BBSCPPATH + "cp " + remoteSystem + ":"
      dmgetCmd = self.autoConstants.SSHPATH + "ssh " + remoteSystem + " " + \
                 self.autoConstants.DMGETPATH + "dmget "
      fullDestination = task.destinationPath + "/" + task.year + "/" + task.month
      
      for dasObject in dasObjects:
         for prefix in dasObject.PREFIXES:
            if dasObject.TYPE == "inst2D":
               if re.search ("bkg", prefix):
                  dasDir = "ana"
               else:
                  dasDir = "diag"
            else:
                  dasDir = dasObject.DIR

            filesToTransfer.append (dmgetCmd + task.sourcePath + \
                                    "/" + dasDir + "/" + \
                                    "Y" + task.year + "/M" + \
                                    task.month + "/" + task.filePrefix \
                                    + "." + prefix + "." \
                                    + task.year + task.month + \
                                    task.day + "_*z.hdf ")

            filesToTransfer.append (bbscpCmd + task.sourcePath + \
                                    "/" + dasDir + "/" + \
                                    "Y" + task.year + "/M" + \
                                    task.month + "/" + task.filePrefix \
                                    + "." + prefix + "." + \
                                    task.year + task.month + \
                                    task.day + "*z.hdf " + fullDestination)
            
      return filesToTransfer
