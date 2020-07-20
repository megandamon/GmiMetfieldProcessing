#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         December 14th 2006
#
# DESCRIPTION:
# This is the base class for all DAS fields.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants
from GmiAutomationTools import GmiAutomationTools
from GmiLogUtilities import GmiLogUtilities
from GmiNetCdfFileTools import GmiNetCdfFileTools
from CommonUtilities import CommonUtilities
from IoRoutines import IoRoutines

import os
import re
import sys
import _thread
from time import time

class GmiDasFields:
   
   OLDFIELDNAMES = ['SURFP','ALBEDO', 'ALDIF', 'ALDIR', 'ASDIF','ASDIR', 'FLNS', 'FLNSC', 'FLNT', 'FLNTC', 'FSNS', 'FSNSC', 'GWETROOT', 'GWETTOP', 'LHFX', 'ORO','OSR', 'OSRCLR', 'PARDF','PARDR','PBLH', 'PHIS','PREACC','PRECC','PRECL','Q10M','Q2M','SHFX','SLP','SNOWH','SOLIN','T10M','T2M','TAUX','TAUY','TQ','TROPP','TROPQ','TROPT','TSKIN','U10M','U2M','USTAR','V10M','V2M','Z0H','Z0M','ZMPR','CLOUD','CMFDQR2','CMFDTR','CMFETR','CMFMC','CMFMC2','DQRL','DTCOND','DTRAIN','KVH','OMEGA','Q','T','TAUCLI','TAUCLW','U','V','ZMDQR','ZMDU','ZMED','ZMEU','ZMMD','ZMMU','ai_42','bi_42','am_42','bm_42','pt_42']
   NEWFIELDNAMES = ['psf','albd','saldif','saldir','sasdif','sasdir','radlwg','radlwgclr','radlwt','radlwtclr','radswg','radswgclr','gwetroot','gwettop','lhfx','lwi','osr','osrclr','pardif','pardir','pbl','phis','preacc','precon','prels','q10m','q2m','shfx','slp','snowh','solin','t10m','t2m','taux','tauy','totq','tropp','tropq','tropt','tg','u10m','u2m','ustar','v10m','v2m','z0h','z0','zmpr','cloud','cmfdqr2','cmfdtr','cmfetr','cldmas','cmfmc2','dqrl','dtcond','dtrain','kzz','omega','sphu','kel','taucli','tauclw','u','v','zmdqr','zmdu','zmed','zmeu','zmmd','zmmu','ai','bi','am','bm','pt']
   EXPECTEDNUMBEROFREHORIZONTALGRIDFILES = 3
   
   FIELDS = []
   FIELDSFORUNITCONVERSION = []
   UNITCONVERSION = []
   PREFIX = ''
   EXPECTEDNUMBEROFFILES = 0
   TIMEDELTA = 0
   LLKDIRECTORY = '' 
   FILETYPE = 'none'
   HORIZONTALREGRIDNAMELISTFILE = []
   HORIZONTALGRIDFILE = []
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      
      self.constants = GmiAutomationConstants ()
      self.gmiAutomationTool = GmiAutomationTools ()
      self.gmiNetCdfFileTool = GmiNetCdfFileTools ()
      self.regrid1x1x25FileName = ''
      self.regrid2x2x5FileName = ''
      self.regrid4x5FileName = ''
 
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
   # Will check the local path for DAS source files.
   #---------------------------------------------------------------------------    
   def checkGEOS4DasSourceFiles (self, task):

      try:
         fullSourcePath = self.getFullSourcePath (task)
      except: raise

      for record in self.RECORDS:
         fileString = task.filePrefix + "." + self.GMIPREFIX + \
                      "." + task.year + task.month + task.day + \
                      "_" + record + "z.hdf"
         theFile = fullSourcePath + fileString
         if not os.path.exists (theFile):
            print("File not found: ", theFile)
            raise self.constants.NOSUCHFILE

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Returns the full source path (including year and month directory)
   #---------------------------------------------------------------------------    
   def getFullSourcePath (self, task):

      fullSourcePath = None

      if self.SOURCESTYLE == "GMAO":
         fullSourcePath = task.sourcePath + self.DIR + "/Y" + \
                          task.year + "/M" + task.month + "/" 
      elif self.SOURCESTYLE == "GMI":
         fullSourcePath  = task.sourcePath + "/" + task.year + \
                           "/" + task.month + "/" 
      else:
         print("The SOURCESTYLE: ", self.SOURCESTYLE, " is not supported")
         raise 
      
      return fullSourcePath


      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine checks the logs and calls the processInstantSurfaceFieldsDAS
   # and then logs an entry if processing is successful.
   #--------------------------------------------------------------------------- 
   
   def doDasFieldsAndLog (self, task):
      
      
      # verify the completeness of the task object
      returnCode = task.verifyCompleteness ()
      if returnCode != self.constants.NOERROR:
         print("\nInvalid task for", self.FILETYPE, " returnCode = ", returnCode, "\n")
         return self.constants.INVALIDINPUT
      
      # create a log entry
      entry = task.filePrefix + self.PREFIX + task.year + task.month + task.day
      
      
      # check for this entry - it may already be processed
      # if not found, process it
      logUtility = GmiLogUtilities ()
      if logUtility.checkLogForEntry (entry, self.constants.DEFAULTDASLOGFILENAME) != True:
         
         print("\nThe entry: ", entry, " was not found and will be processed for", self.FILETYPE, "\n")
         
         returnCode = self.processDasFields (task)
         if returnCode != self.constants.NOERROR:
            print("Could not process the ", self.FILETYPE, "DAS fields \nreturnCode = ", returnCode, "\n")
            return returnCode
         
         
         logUtility.logEntryToFile (entry + '\n', self.constants.DEFAULTDASLOGFILENAME) 
           
           
      else:
         print("The entry: ", entry, " was found and will not be processed for ", self.FILETYPE, "\n")
         
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine fills in the necessary information for the object
   #---------------------------------------------------------------------------
   
   def fillInNecessaryInformation (self, task):

      filePattern = task.filePrefix + self.PREFIX + task.year + task.month + task.day
      
      # set the destination path
      destinationPath = task.destinationPath + task.year + "/" + task.month + '/'      
      
      # create a file name for all files to be concatenated to
      concatenatedFileName = destinationPath + filePattern + '.nc'

      
      self.regrid2x2x5FileName = concatenatedFileName \
                                 [0:len(concatenatedFileName)-3] + ".r2.nc"

      self.regrid4x5FileName = concatenatedFileName \
                                 [0:len(concatenatedFileName)-3] + ".r4.nc"

      self.regrid1x1x25FileName = concatenatedFileName \
                                  [0:len(concatenatedFileName)-3] + ".r1.nc"
      
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine performs the necessary tasks to pre-process the DAS 
   # fields.  It will : do the hdf to netcdf conversion,...
   #---------------------------------------------------------------------------      
   
   def processDasFields (self, task):

      #-------------------------------------------------
      # set up section 
   
      # set the source path
      sourcePath = task.sourcePath + "/" + task.year + "/" + task.month + "/"
      
      # set the destination path
      destinationPath = task.destinationPath + task.year + "/" + task.month + '/'

      # check if the files are there
      filePattern = task.filePrefix + "." + self.PREFIX + task.year + task.month + task.day
      
      try:
         files = self.gmiAutomationTool.getMatchingFiles (sourcePath, filePattern)
      except:         
         print("\nThe ", filePattern, " files in ", sourcePath, " for ", self.FILETYPE, \
               "are not available.\n")
         raise self.constants.NOSUCHPATH     
      
      if len (files) < self.EXPECTEDNUMBEROFFILES:
         print("len of ", self.FILETYPE, "  files: ", len (files), " not as expected!\n")
         raise self.constants.INCORRECTNUMBEROFFILES

      # add the source path to the file list
      filesWithPath = self.gmiAutomationTool.addPathToFileNames (sourcePath, files)

      # end setup section
      #-------------------------------------------------
            
      
      
      
      #-------------------------------------------------
      # begin processing the DAS fields
      
      exitMutexes = []
      mutexCount = 0
      while mutexCount < len (files):
         
         # prepare a new netcdf name with destination path
         newNcdfFileName = files [mutexCount]
         newNcdfFileName = newNcdfFileName [0:len(files[mutexCount]) - len ('.hdf')] + '.nc'
         newNcdfFileName = destinationPath + newNcdfFileName 
         
         # do the dump to netcdf
         print("Processing: ", newNcdfFileName, "\n")
         exitMutexes.append (_thread.allocate_lock())
         _thread.start_new (self.gmiNetCdfFileTool.doHdfDumpToNetCdf, \
                           (self.FIELDS, #
                            filesWithPath [mutexCount], #
                            newNcdfFileName,
                            exitMutexes[mutexCount]))

         mutexCount = mutexCount + 1
                           
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass

      # now, combine all the files that were just created...
      # get the new files
      try:
         allFiles = self.gmiAutomationTool.getMatchingFiles (destinationPath, filePattern)
      except:
         print("Exception raised when attempting to get matching files in : ", \
               destinationPath, "for ", self.FILETYPE, " \n")
         return self.constants.NOSUCHPATH

      # remove any non .nc files
      files = []
      for file in allFiles:
         if re.search ('.nc', file):
            files.append (file)

      # add the path for processing
      filesWithPath = self.gmiAutomationTool.addPathToFileNames (destinationPath, files)      
      
      
      # create a file name for all files to be concatenated to
      concatenatedFileName = destinationPath + filePattern + '.nc'
      
      # concatenate the files
      returnCode = self.gmiNetCdfFileTool.concatenateRecordVariables \
                   (self.FIELDS, filesWithPath, concatenatedFileName)
      if returnCode != self.constants.NOERROR:
         print("Problem concatenating record variables for ", self.FILETYPE, " fields\n")
         raise self.constants.ERROR

      # now remove the files
      for file in filesWithPath:
         systemCommand = self.constants.RMPATH + 'rm ' + file
         systemReturnCode = os.system (systemCommand)
         if systemReturnCode != 0:
            raise self.constants.BADSYSTEMRETURNCODE         

      # do the unit conversions
      fieldCounter = 0
      exitMutexes = []
      while fieldCounter < len (self.FIELDSFORUNITCONVERSION):

         exitMutexes.append (_thread.allocate_lock())
         _thread.start_new (self.gmiNetCdfFileTool.doUnitConversion, \
                           (self.FIELDSFORUNITCONVERSION [fieldCounter], \
                            self.UNITCONVERSION[fieldCounter], concatenatedFileName, \
                            exitMutexes[fieldCounter]))
         fieldCounter = fieldCounter + 1

      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass

      self.regrid2x2x5FileName = concatenatedFileName \
                                 [0:len(concatenatedFileName)-3] + ".r2.nc"

      self.regrid4x5FileName = concatenatedFileName \
                                 [0:len(concatenatedFileName)-3] + ".r4.nc"

      # regrid the new file to 2x2.5 resolution
      returnCode = self.gmiNetCdfFileTool.regridFile \
                   (concatenatedFileName, self.regrid2x2x5FileName, \
                    self.HORIZONTALREGRIDNAMELISTFILE [0], \
                    self.HORIZONTALGRIDFILE [0], "2x2.5")
      if returnCode != self.constants.NOERROR:
         print("Problem doing 2x2.25 horizontal regrid for ", self.FILETYPE)
         return returnCode    

      # now that the concatenatedFile is not needed to regrid anymore,
      # move it to a name to specifiy that it is the 1x1.25 file
      self.regrid1x1x25FileName = concatenatedFileName [0:len(concatenatedFileName)-3] + ".1x1.25.nc"      
      systemCommand = self.constants.MVPATH + 'mv ' + concatenatedFileName + " " + self.regrid1x1x25FileName
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE

      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Archives processed GEOS4 data to specified directory.
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

      bbscpCmd = self.constants.BBSCPPATH + "bbscp "
      newTransferFile = transferFile + "." + str (time())

      for resolution in ["4x5", "2x2.5"]:

         fullArchivePath = archiveDirectory + "/" + resolution \
                           + "/" + task.year
         
         systemCommands.append (commonUtils.getCreateRemoteDirCmd \
                                (remoteSystem, fullArchivePath))
         
         systemCommands.append (bbscpCmd \
                                + destinationPath + "/" \
                                + task.filePrefix + "." \
                                + task.year + task.month + task.day \
                                + "." + resolution + ".nc " \
                                + remoteSystem + ":" + fullArchivePath)

      jobId = None
      try:
         ioRoutines.appendLinesToFileAndRename (transferFile, \
                                                systemCommands, \
                                                newTransferFile)
         print("transfer file: ", newTransferFile)
         jobId = commonUtils.qsubFileAndWait (newTransferFile)
      except: raise
     
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
   # This routine merges the three files with the matching pattern
   # and adjust critical parameters common to all types of fields.
   # This routine will return NOERROR if successful.
   #--------------------------------------------------------------------------- 
   
   def mergeAndCorrectCriticalAttributes (self, task, filePattern, instantSurfacePrefix, \
                                          averagedSurfacePrefix, averagedEtaPrefix):
      

      # set the source path
      sourcePath = task.sourcePath + self.LLKDIRECTORY + 'Y' + task.year + "/M" + task.month + "/"
      
      # set the destination path
      destinationPath = task.destinationPath + task.year + "/" + task.month + '/'   
      
      #-------------------------------------------------
      # check if all the "regrid" files are now available
      # if they are merge them and fix other attributes
      
      filePatternWithDate = task.year + task.month + task.day + "." + filePattern
      print("\nMerging files with the pattern: ", filePatternWithDate)


      

      
      # remove any "G4" files with the filePattern that should not be there
      systemCommand = 'rm -f ' + destinationPath + 'G4LLK*' + \
                      task.year + task.month + task.day + '*' + filePattern + '*'

   
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         print("Error! returnCode is: ", systemReturnCode, " after attempting to remove the G4 files \n")
         return self.constants.BADSYSTEMRETURNCODE      



      fileList = self.gmiAutomationTool.getMatchingFiles (destinationPath, filePatternWithDate)
      
 
      if len (fileList) != self.EXPECTEDNUMBEROFREHORIZONTALGRIDFILES:
         return self.constants.INCORRECTNUMBEROFFILES
      
      if len (filePattern) <= 0:
         return self.constants.INVALIDINPUT
      
      
      
      #-------------------------------------------------
      # create the file listing
      # NOTE_doing it this way so that the files are in a certain order
      
      filesWithPath = []
      filesWithPath.append (destinationPath + task.filePrefix + "." + \
                            instantSurfacePrefix + \
                            task.year + task.month + task.day + "." + filePattern + ".nc")
      filesWithPath.append (destinationPath + task.filePrefix + "." + \
                            averagedSurfacePrefix + task.year + \
                            task.month + task.day + "." + filePattern + ".nc")
      filesWithPath.append (destinationPath + task.filePrefix + "." + \
                            averagedEtaPrefix + task.year + \
                            task.month + task.day + "." + filePattern + ".nc")
      newFileName = destinationPath + task.filePrefix + "." + task.year + \
                    task.month + task.day + "." + filePattern +".nc"

      #-------------------------------------------------
   
   
   
      #-------------------------------------------------
      # merge the files
      returnCode = self.gmiNetCdfFileTool.mergeFilesIntoNewFile (filesWithPath, newFileName)
      if returnCode != self.constants.NOERROR:
         
         print("\nThere was an error in mergeFilesIntoNewFile, return code is ", returnCode, "\n")
         return self.constants.ERROR
      
      #------------------------------------------------- 



      #-------------------------------------------------
      # remove the other files now
      
      systemCommand = self.constants.RMPATH + 'rm ' + filesWithPath [0] + " " + filesWithPath [1]
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         
         print("\nThere was an error removing the files: ", filesWithPath [0], " ", filesWithPath [1], "\n")
         return self.constants.ERROR 
   
      #-------------------------------------------------
   
   

   
      #-------------------------------------------------
      # rename the fields to GMI standards

      returnCode = self.gmiNetCdfFileTool.renameFields (GmiDasFields.OLDFIELDNAMES, \
                                                        GmiDasFields.NEWFIELDNAMES, newFileName)
      if returnCode != self.constants.NOERROR:
      
         print("\nThere was an error renaming fields in the file: ", newFileName, \
               " return code is : ", returnCode, "\n")
         return self.constants.ERROR
      
      #-------------------------------------------------
   

   
      #-------------------------------------------------
      # correct critical attributes
      
      systemCommand = self.constants.NCATTEDPATH + \
                      "ncatted -h -O -a Met_Data_Name,global,c,c,\"GMAO_GEOS4GCM_2%5x2x42\"" 
      systemCommand = systemCommand + " -a Conventions,global,c,c,COARDS"
      systemCommand = systemCommand + \
                      " -a source,global,c,c,\"GMAO GEOS-4-Forecast run - /output0/hiroo/ceres_tracep/netcdf/fcst/\""
      systemCommand = systemCommand + \
                      " -a title,global,c,c,\"GMAO GEOS-4-Forecast " + filePattern + \
                      " 42 level assimilation regridded\""
      systemCommand = systemCommand + " -a units,dtcond,o,c,\"degK/s\""
      systemCommand = systemCommand + " -a units,kel,o,c,\"degK\""
      systemCommand = systemCommand + " -a units,zmmu,o,c,\"Pa/s\""
      systemCommand = systemCommand + " -a units,pbl,o,c,\"m\""
      systemCommand = systemCommand + " -a units,snowh,o,c,\"m\""
      systemCommand = systemCommand + " -a units,t10m,o,c,\"degK\""
      systemCommand = systemCommand + " -a units,t2m,o,c,\"degK\""
      systemCommand = systemCommand + " -a units,tropt,o,c,\"degK\""
      systemCommand = systemCommand + " -a units,tg,o,c,\"degK\""
      systemCommand = systemCommand + " -a units,z0,o,c,\"m\""
      systemCommand = systemCommand + " -a units,z0h,o,c,\"m\""
      systemCommand = systemCommand + " -a units,tropp,o,c,\"hPa\""
      systemCommand = systemCommand + " -a units,slp,o,c,\"hPa\""
      systemCommand = systemCommand + " -a units,psf,o,c,\"hPa\""
      systemCommand = systemCommand + " " + newFileName
      
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         
         print("\nThere was an error correcting the critical attributes for the file: ", \
               newFileName, "systemReturnCode = ", systemReturnCode, "\n")
         return self.constants.ERROR
      
      #-------------------------------------------------


      
      return self.constants.NOERROR   



      
