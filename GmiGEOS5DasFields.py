#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         October 30th, 2007
#
# DESCRIPTION:
# This is the base class for all GEOS5DAS fields.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants
from GmiAutomationTools import GmiAutomationTools
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiLogUtilities import GmiLogUtilities
from GmiPreStage import GmiPreStage
from GmiGFIORemapTools import GmiGFIORemapTools
from GmiParallelTools import GmiParallelTools
from CommonUtilities import CommonUtilities
from IoRoutines import IoRoutines

import os
import re
import sys 
import thread
from numpy import *
from time import time

class GmiGEOS5DasFields:

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  


   RESOLUTIONS = ['2x2.5', '1x1.25']
   oldFieldNames = ['TROPPB', 'TS']
   newFieldNames = ['TROPP', 'TSKIN']
   longitude = zeros(144)
   longitude[:] = [0, 2.5, 5, 7.5, 10, 12.5, 15, 17.5, 20, 22.5, 25, 27.5, 30, \
                        32.5, 35, 37.5, 40, 42.5, 45, 47.5, 50, 52.5, 55, 57.5, 60, 62.5, 65, \
                        67.5, 70, 72.5, 75, 77.5, 80, 82.5, 85, 87.5, 90, 92.5, 95, 97.5, 100, \
                        102.5, 105, 107.5, 110, 112.5, 115, 117.5, 120, 122.5, 125, 127.5, 130, \
                        132.5, 135, 137.5, 140, 142.5, 145, 147.5, 150, 152.5, 155, 157.5, 160, \
                        162.5, 165, 167.5, 170, 172.5, 175, 177.5, 180, 182.5, 185, 187.5, 190, \
                        192.5, 195, 197.5, 200, 202.5, 205, 207.5, 210, 212.5, 215, 217.5, 220, \
                        222.5, 225, 227.5, 230, 232.5, 235, 237.5, 240, 242.5, 245, 247.5, 250, \
                        252.5, 255, 257.5, 260, 262.5, 265, 267.5, 270, 272.5, 275, 277.5, 280, \
                        282.5, 285, 287.5, 290, 292.5, 295, 297.5, 300, 302.5, 305, 307.5, 310, \
                        312.5, 315, 317.5, 320, 322.5, 325, 327.5, 330, 332.5, 335, 337.5, 340, \
                        342.5, 345, 347.5, 350, 352.5, 355, 357.5]
   pressureLevels = zeros(72)
   pressureLevels[:] = [0.015, 0.02635, 0.0401425, 0.0567925, 0.0776725, 0.104524, \
                             0.139599, 0.185422, 0.244938, 0.321784, 0.420424, 0.546293, \
                             0.705957, 0.907287, 1.15998, 1.47565, 1.86788, 2.35259, 2.94832, \
                             3.67650, 4.56169, 5.63180, 6.91832, 8.45639, 10.2849, 12.4602, \
                             15.0503, 18.1244, 21.7610, 26.0491, 31.0889, 36.9927, 43.9097, \
                             52.0159, 61.4956, 72.5579, 85.4390, 100.514, 118.250, 139.115, \
                             163.662, 192.558, 226.597, 266.698, 313.214, 356.894, 394.600, \
                             432.316, 470.041, 507.771, 545.506, 583.245, 620.988, 658.732, \
                             690.189, 715.354, 740.520, 765.687, 790.855,  813.507, 831.125, \
                             846.227, 861.328, 876.430, 891.532, 906.634, 921.736, 936.839, \
                             951.941, 967.043, 982.146, 997.234]

   
   def __init__(self):
      
      self.GEOS5FIELDS = []
      self.SOURCESTYLE = "GMAO"
      self.BIT = "32"
      self.NODENAMES = "borgmg"
      self.FIELDSFORUNITCONVERSION = []
      self.UNITCONVERSIONS = []
      self.TYPE = None
      self.DIR = None
      self.FINAL_RECORDS = 4
      self.constants = GmiAutomationConstants ()
      self.gmiAutomationTool = GmiAutomationTools ()
      self.UNITCONVERSIONS.append (1/100.)
      self.UNITCONVERSIONS.append (1/100.)
      self.UNITCONVERSIONS.append (1/100.)
      self.UNITCONVERSIONS.append (1/100.)
      self.UNITCONVERSIONS.append (1/100.)

 
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Destructor routine.
   #---------------------------------------------------------------------------    
   
   def __del__(self):
      pass

   def getFullSourcePath (self, task):

      fullSourcePath = None

      if self.SOURCESTYLE == "GMAO":
         fullSourcePath = task.sourcePath + self.DIR + "/Y" + \
                          task.year + "/M" + task.month + "/" 
      elif self.SOURCESTYLE == "GMI":
         fullSourcePath  = task.sourcePath + "/" + task.year + \
                           "/" + task.month + "/" 
      else:
         print "The SOURCESTYLE: ", self.SOURCESTYLE, " is not supported"
         raise self.constants.INVALIDINPUT
      
      return fullSourcePath
   
   def checkGEOSSourceFiles (self, task):

      try:
         fullSourcePath = self.getFullSourcePath (task)
      except: raise

      for prefix in self.PREFIXES:
         fileStringPre = task.filePrefix + "." + prefix + \
                         "." + task.year + task.month + task.day
         if len(self.RECORDS) != 0:
            print "Checking records"
            for record in self.RECORDS:
               fileString = fileStringPre + "_" + record + "z.hdf"
               theFile = fullSourcePath + fileString
               if not os.path.exists (theFile):
                  print "check file parent: ", theFile
                  raise self.constants.NOSUCHFILE
         else:
            fileString = fileStringPre + ".hdf"
            theFile = fullSourcePath + fileString
            if not os.path.exists (theFile):
               print "check file parent: ", theFile
               raise self.constants.NOSUCHFILE
            print "The file: ", fileString, " exists."

            


   def makeTimeDimRecordDim (self, task):
      netCdfObject = GmiNetCdfFileTools ()
      for resolution in ["2x2.5", "1x1.25"]:
         fileName = self.basePath + self.GMIPREFIX + self.endPath + "." + \
             resolution + ".nc"
         netCdfObject.makeDimensionRecordDimension (fileName, 'time')


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine checks the logs and calls the processInstantSurfaceFieldsDAS
   # and then logs an entry if processing is successful.
   #--------------------------------------------------------------------------- 
   
   def doGEOSFields (self, task, destinationPath, exitMutex):
      
      self.prepareGEOSFields (task)

      print "doGEOS5DASFields parent ACQURING MUTEX"
      exitMutex.acquire ()

   def extractTimeRecords (self, task, spec, min, max, stride):

      print "Extracting records ", min, "-", max, " stride ", stride
      print "For: ", self.RESOLUTIONS, self.PREFIXES

      basePath = task.destinationPath + "/" + \
                 task.year + "/" + task.month + \
                 "/"
      netCdfObject = GmiNetCdfFileTools ()
      
      for resolution in self.RESOLUTIONS: 
         for prefix in self.PREFIXES:
            inputFile = basePath + \
                      task.filePrefix + "." + \
                      prefix + "." + \
                      task.year + task.month + \
                      task.day + "." + \
                      resolution + ".nc"
            newFile = basePath + task.filePrefix + "." + \
                      prefix + "." + \
                      task.year + task.month + \
                      task.day + "." + \
                      resolution + ".3hr.nc"

            print inputFile
            print newFile
            netCdfObject.doVariableExtractionWithHyperSlabs ( \
               " ", [], spec, min, max, stride, inputFile, \
               newFile)
            os.rename(newFile, inputFile)




         
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine performs the tasks needed by all the GEOS5DAS fields.
   # finalize this list: regridding, hdf to netcdf, renaming?
   # this routine expects an exitMutex object; it is intended to be
   # reentrant
   #---------------------------------------------------------------------------

   def regridAndDumpHdfFiles (self, task):

      try:
         print "Regridding the files to required resolutions"
         self.regridFilesAllResolutions (task)
         print "Convert HDF5 to HDF4"
         self.convertHdf5ToHdf4 (task)
         print "Dumping hdf fields to netcdf"
         self.dumpFieldsToNetcdf (task)
         print "Done dumping to netcdf"
      except:
         raise

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Archives processed GEOS5 data to specified directory.
   # Uses the conventions of GMI directory structure as of June 2nd.
   #--------------------------------------------------------------------------- 
   def archiveGEOSDataToGmiArchive (self, task, archiveDirectory, remoteSystem, \
                                    transferFile, processDirectory):
      
      # construct the destination path
      destinationPath = task.destinationPath + "/" + \
                        task.year + "/" + task.month

      gmiAutoObject = GmiAutomationTools ()
      commonUtils = CommonUtilities ()
      ioRoutines = IoRoutines ()
      systemCommands = []

#      bbscpCmd = self.constants.BBSCPPATH + "bbscp "
      cpCmd = "cp "
      newTransferFile = transferFile + "." + str (time())

      for resolution in ["2x2.5", "1x1.25"]:

         fullArchivePath = archiveDirectory + "/" + resolution \
                           + "/" + task.year
         
         systemCommands.append (commonUtils.getCreateRemoteDirCmd \
                                (remoteSystem, fullArchivePath))
         
         systemCommands.append (cpCmd \
                                + destinationPath + "/" \
                                + task.filePrefix + "." \
                                + task.year + task.month + task.day \
                                + "." + resolution + ".nc " \
                                #+ remoteSystem + ":" + fullArchivePath)
                                + fullArchivePath)


      jobId = None
      try:
         ioRoutines.appendLinesToFileAndRename (transferFile, \
                                                systemCommands, \
                                                newTransferFile)
         print "Here is the transfer file: ", newTransferFile

         jobId = commonUtils.qsubFileAndWait (newTransferFile)
      except: raise "Problem submitting transfer job"
     
      # remove the transfer and output file
      os.remove (newTransferFile)
      if jobId != None:
         systemCommand = self.constants.RMPATH + "rm -f " + \
                         processDirectory + "/*" \
                         + jobId[0:6] + "*"
         os.system(systemCommand)

      return self.constants.NOERROR

      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This simply merges files define by the PREFIXES
   # into a single file.
   #---------------------------------------------------------------------------

   def mergeAllFilesIntoOne (self, task):


      print "merge all files"
      basePath = task.destinationPath + "/" + \
                 task.year + "/" + task.month + \
                 "/"
      gmiNetCdfObject = GmiNetCdfFileTools ()

      # merge time records
      resolution = "2x2.5"
      fileNames = []
      print self.AVERAGEDRECORDS
      for record in self.AVERAGEDRECORDS:
         timeRecordFile = basePath + \
                          task.filePrefix + "." + \
                          self.GMIPREFIX + "." + \
                          task.year + task.month + \
                          task.day + "_" + \
                          record + "z." + \
                          resolution + ".nc"
         print timeRecordFile
         fileNames.append (timeRecordFile)
         
         # add the correct time stamp

         systemCommand = "ncap -O -s time=time+"+record+". "+timeRecordFile+" "+timeRecordFile
         
         print systemCommand
         returnCode = os.system (systemCommand)
         if returnCode != 0:
            print "ERROR trying to do ncap: ", returnCode

      mergedTimeRecordFileName = basePath + task.filePrefix + "." + \
                                 self.GMIPREFIX + "." + task.year + \
                                 task.month + task.day + "." + \
                                 resolution + ".nc"

      blankFieldNames = []
      returnCode = gmiNetCdfObject.concatenateRecordVariables (blankFieldNames, fileNames, \
                                                               mergedTimeRecordFileName)
      if returnCode != self.constants.NOERROR:
         print "ERROR trying to concatenate record variables: ", returnCode

            
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine will dump all the fields in the task to a netcdf file.
   #---------------------------------------------------------------------------    

   def dumpFieldsToNetcdf (self, task):

      
      # construct the destination path
      gmiAutoObject = GmiAutomationTools ()
      gmiNetCdfObject = GmiNetCdfFileTools ()
      
      
      destinationPath = task.destinationPath + "/" + \
                        task.year + "/" + task.month

      # create the path if it does not exist
      if not os.path.exists (destinationPath):
         gmiAutoObject.createDirectoryStructure (destinationPath)   

      # create a remap object & parallel tools object
      parallelTools = GmiParallelTools (task.archType)
      remapObject = GmiGFIORemapTools ()

      print "About to start dumping threads..."

      exitMutexes = []
      systemCommands = []
      count = 0
      # process each time record
      for resolution in self.RESOLUTIONS:

         # process each file prefix
         for prefix in self.PREFIXES:
            # process each resolution
            fileString = task.filePrefix + "." + prefix + \
                         "." + task.year + task.month + task.day + \
                         "." + resolution + "."
            print fileString

            sourceFile = destinationPath + "/" + \
                         fileString + \
                         "hdf"
            destinationFile = destinationPath + "/" + \
                              fileString + \
                              "nc"


            print "starting new thread"
            print self.GEOS5FIELDS
            print sourceFile
            print destinationFile
            exitMutexes.append (thread.allocate_lock())

            thread.start_new(gmiNetCdfObject.doHdfDumpToNetCdf, \
                             (self.GEOS5FIELDS, \
                              sourceFile, \
                              destinationFile, \
                              0, exitMutexes[count]))
                             
            count = count + 1


      #----------------------------------------------------------------
      #  Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass

      print "All threads returned!"

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine will convert HDF5 files to HDF4.
   #---------------------------------------------------------------------------    

   def convertHdf5ToHdf4 (self, task):

      
      # construct the destination path
      gmiAutoObject = GmiAutomationTools ()
      gmiNetCdfObject = GmiNetCdfFileTools ()
      
      
      destinationPath = task.destinationPath + "/" + \
                        task.year + "/" + task.month

      # create the path if it does not exist
      if not os.path.exists (destinationPath):
         gmiAutoObject.createDirectoryStructure (destinationPath)   

      # create a remap object & parallel tools object
      parallelTools = GmiParallelTools (task.archType)
      remapObject = GmiGFIORemapTools ()


      exitMutexes = []
      systemCommands = []
      count = 0
      # process each time record
      for resolution in self.RESOLUTIONS:

         # process each file prefix
         for prefix in self.PREFIXES:
            # process each resolution
            fileString = task.filePrefix + "." + prefix + \
                         "." + task.year + task.month + task.day + \
                         "." + resolution + "."

            sourceFile = destinationPath + "/" + \
                         fileString + \
                         "hdf"
            exitMutexes.append (thread.allocate_lock())

            thread.start_new(gmiNetCdfObject.doHdf5ToHdf4,\
                              (sourceFile, \
                              exitMutexes[count]))
                             
            count = count + 1


      #----------------------------------------------------------------
      #  Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass



   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Regrids all the files in the task.
   #---------------------------------------------------------------------------    

   def regridFilesAllResolutions (self, task):

      # construct the destination path
      gmiAutoObject = GmiAutomationTools ()
      destinationPath = task.destinationPath + "/" + \
                        task.year + "/" + task.month

      # create the path if it does not exist
      if not os.path.exists (destinationPath):
         gmiAutoObject.createDirectoryStructure (destinationPath)   

      # create a remap object
      remapObject = GmiGFIORemapTools ()

      exitMutexes = []
      count = 0

      parallelTools = GmiParallelTools(task.archType)

      systemCommands = []

      print "PREFIXES: ", self.PREFIXES



      for prefix in self.PREFIXES:

         
         fileString = task.filePrefix + "." + prefix + \
                      "." + task.year + task.month + task.day + \
                      ".hdf"
         
         print fileString
             
         if self.SOURCESTYLE == "GMAO":
            sourceFile = task.sourcePath + self.DIR + "/Y" + \
                         task.year + "/M" + task.month + "/" + \
                         fileString
         else:
            sourceFile = task.sourcePath + "/" + task.year + \
                         "/" + task.month + "/" + fileString
         print sourceFile
               
         # process each resolution
         for resolution in self.RESOLUTIONS:

            destinationFile = destinationPath + "/" + \
                              fileString[0:len(fileString)-3] + \
                              resolution + ".hdf"
               
            exitMutexes.append (thread.allocate_lock())

            print destinationFile
            thread.start_new(remapObject.gfioRemap, \
                             (sourceFile, \
                              destinationFile, \
                              resolution, \
                              self.constants.REMAPEXEC, \
                              self.BIT, \
                              exitMutexes[count]))
               
            count = count + 1

      #----------------------------------------------------------------
      # WAIT:: Wait for all threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass

      print "done regridding"


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine extracts the set of variables that are required; the rest
   # will be purged from the file
   #--------------------------------------------------------------------------- 
   
   def doFieldExtraction (self, task):
      print "in Field Extraction"

      # this object contains the routine
      # for the extraction
      gmiNetCdfObject = GmiNetCdfFileTools ()

      for resolution in self.RESOLUTIONS:
         fileName = task.destinationPath + "/" + \
                    task.year + "/" + task.month + \
                    "/" + task.filePrefix + "." + \
                    self.GMIPREFIX + "." + task.year + \
                    task.month + task.day + "." +  \
                    resolution + ".nc"

         print self.GEOS5FIELDS
         print fileName
         returnCode = gmiNetCdfObject.extractSubsetOfVariables (self.GEOS5FIELDS, fileName, \
                                                                fileName)
         if returnCode != self.constants.NOERROR:
            print "There was a problem extracting the variables"
         
