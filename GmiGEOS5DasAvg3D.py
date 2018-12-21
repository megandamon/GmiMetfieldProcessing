
 #!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / SSAI
# DATE:         Feb 8 2016
#
# DESCRIPTION:
# This is the class for the tavg3_ fields (averaged)
#------------------------------------------------------------------------------
from GmiAutomationConstants import GmiAutomationConstants
from GmiAutomationTools import GmiAutomationTools
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiLogUtilities import GmiLogUtilities
from GmiMetFieldTask import GmiMetFieldTask
from GmiGEOS5DasFields import GmiGEOS5DasFields
from GmiPreStage import GmiPreStage
from GmiParallelTools import GmiParallelTools
from GmiGFIORemapTools import GmiGFIORemapTools
from IoRoutines import IoRoutines
import os
import re
import sys
import thread
from numpy import *
from pynetcdf import *
from datetime import datetime
from datetime import timedelta


class GmiGEOS5DasAvg3D (GmiGEOS5DasFields):
   def __init__(self):
      GmiGEOS5DasFields.__init__(self)
      self.netCdfObject = GmiNetCdfFileTools ()      
      self.SOURCESTYLE = "GMAO"
      self.TYPE = "avg3D"
      self.GMIPREFIX = "tavg3d"
      self.DIR = "diag"
      self.RECORDS = []
      self.AVERAGEDRECORDS = self.RECORDS
      self.resetPrefixesAndFields ()
      
   def __del__(self):
      pass

   # this routine adds all met fields and collection names
   # to the PREFIXES and GEOS5FIELDS lists
   def resetPrefixesAndFields (self):

      self.PREFIXES = []

      self.fieldMap = {}

      self.GEOS5FIELDS = []
      
      # 3-hr
      self.PREFIXES.append ("tavg3_3d_asm_Nv")
      self.GEOS5FIELDS.append ("QV")
      self.GEOS5FIELDS.append ("T")
      self.GEOS5FIELDS.append ("DELP")
      self.GEOS5FIELDS.append ("U")
      self.GEOS5FIELDS.append ("V")
      self.fieldMap["tavg3_3d_asm_Nv"] = ["QV", "T", "DELP"]
      self.fieldMap["tavg3_3d_asm_Nv_set2"] = ["U", "V"]


      # 3-hr
      self.PREFIXES.append ("tavg3_3d_cld_Nv")
      self.GEOS5FIELDS.append ("DTRAIN")
      self.GEOS5FIELDS.append ("TAUCLI")
      self.GEOS5FIELDS.append ("TAUCLW")
      self.GEOS5FIELDS.append ("CLOUD")
      self.GEOS5FIELDS.append ("QI")
      self.GEOS5FIELDS.append ("QL")
      self.GEOS5FIELDS.append ("time")
      self.GEOS5FIELDS.append ("lev")
      self.GEOS5FIELDS.append ("lon")
      self.GEOS5FIELDS.append ("lat")
      self.fieldMap["tavg3_3d_cld_Nv"] = ["DTRAIN", "TAUCLI", "TAUCLW"]
      self.fieldMap["tavg3_3d_cld_Nv_set2"] = ["CLOUD", "QI", "QL", "lev", "time", "lat", "lon"]

      # 3-hr edge
      self.PREFIXES.append ("tavg3_3d_mst_Ne")
      self.GEOS5FIELDS.append ("CMFMC")
      self.GEOS5FIELDS.append ("PFICU")
      self.GEOS5FIELDS.append ("PFLCU")
      self.GEOS5FIELDS.append ("PFILSAN")
      self.GEOS5FIELDS.append ("PFLLSAN")
      self.GEOS5FIELDS.append ("PLE")
      self.fieldMap["tavg3_3d_mst_Ne"] = ["CMFMC", "PFICU", "PFLCU"]
      self.fieldMap["tavg3_3d_mst_Ne_set2"] = ["PFILSAN", "PFLLSAN", "PLE"]


      # 3-hr edge
      self.PREFIXES.append ("tavg3_3d_trb_Ne")
      self.GEOS5FIELDS.append ("KH")
      self.fieldMap["tavg3_3d_trb_Ne"] = ["KH", "lev"]

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # The routine that drives the processing for the avg 3d fields.
   #---------------------------------------------------------------------------    
   def prepareGEOSFields (self, task):
      self.basePath = task.destinationPath + "/" + \
                      task.year + "/" + task.month + \
                      "/" + task.filePrefix + "."
      self.endPath = "." + task.year + task.month + \
                     task.day
                  
      self.PREFIXES.append ("tavg3_3d_cld_Nv_set2")      
      self.PREFIXES.append ("tavg3_3d_mst_Ne_set2")
      self.PREFIXES.append ("tavg3_3d_asm_Nv_set2")
   
      # extract only the needed variables first
      print "Extracting necessary Avg3D fields..."
      self.doFieldExtraction (task)
      print "done with field extraction"

      print "Avg3D is calling the parent regridAndDumpHdfFiles routine"
      self.regridAndDumpHdfFiles (task)
      print "done with regrid and dump to HDF"

      print "Extracting and saving lat, lev, and time dimensions"
      self.extract1DVariables (task)
      print "Done extracting and saving lat, lev, and time dimensions"

      print "Resolving fake dimensions"
      self.resolveFakeDimensions (task)
      print "Done resolving fake dimensions"   

      print "Extracting time records..."
      GmiGEOS5DasFields.extractTimeRecords (self, task, "time", "0", "7", "1")
      print "Done extracting time records"

      print "Calculating OPTDEPTH 0.625 degree only "
      self.extractCloudFieldsNativeGrid (task)
      print "Done with calculating optical depth 0.625 degree only"

      print "Merging the collections..."
      self.mergeAllFilesIntoOne (task)
      print "done with merging across 3D types"   

      print "Making time dimension a record dimension"
      GmiGEOS5DasFields.makeTimeDimRecordDim (self, task)
      print "Done Making time dimension a record dimension"

      print "Removing CLOUD fields from lower resolution files"
      self.removeCloudFieldsFromLowRes (task)
      print "Done removing CLOUD fields from lower resolution files"

      print "Avg3D exiting"


   # we do this because we will generate the lower res CLOUD fields
   # from an external program later in the doProcessing routine
   # see GmiDasTasks module
   def removeCloudFieldsFromLowRes (self, task):

      print "In removeCloudFieldsFromLowRes routine"

      basePath = task.destinationPath + "/" + \
                 task.year + "/" + task.month + \
                 "/"

      NOCLOUDFIELDS = self.GEOS5FIELDS
      NOCLOUDFIELDS.remove ("CLOUD")
      NOCLOUDFIELDS.remove ("TAUCLW")
      NOCLOUDFIELDS.remove ("TAUCLI")
      NOCLOUDFIELDS.remove ("time")
      NOCLOUDFIELDS.remove ("lat")
      NOCLOUDFIELDS.remove ("lon")
      NOCLOUDFIELDS.remove ("lev")
      NOCLOUDFIELDS.remove ("lev_edges")
      

      for resolution in [ "1x1.25", "2x2.5" ]:
         outFileName = basePath + task.filePrefix + "." + \
             self.GMIPREFIX + "." + task.year + \
             task.month + task.day + "." + \
             resolution + ".nc"

         print "outFileName: ", outFileName

         returnCode = self.netCdfObject.extractSubsetOfVariables (NOCLOUDFIELDS, \
                                                                     outFileName, outFileName)
         if returnCode != self.constants.NOERROR:
            print "There was a problem extracting the fields without the cloud vars!"
            return self.constants.ERROR

 

   def doFieldExtraction (self, task):
      print "in doFieldExtraction"

      # this object contains the routine
      # for the extraction
      gmiNetCdfObject = GmiNetCdfFileTools ()

      for prefix in self.PREFIXES:

         subfix = "_set2"
         if subfix in prefix: 
            print prefix
            fileString = task.filePrefix + "." + prefix.replace(subfix,"") + \
                "." + task.year + task.month + task.day + \
                ".nc4"
         else:
            fileString = task.filePrefix + "." + prefix + \
                "." + task.year + task.month + task.day + \
                ".nc4"
            
         fileName = task.destinationPath + "/" + \
             task.year + "/" + task.month + \
             "/" + fileString
                    

         outFileName = task.destinationPath + "/" + task.year + "/" + task.month + "/" + \
             task.filePrefix + "." + prefix + "." + task.year + task.month + task.day + \
             ".extracted.nc4"

         print fileName
         print outFileName
         print self.fieldMap[prefix]

         returnCode = gmiNetCdfObject.extractSubsetOfVariables (self.fieldMap[prefix], fileName, \
                                                                   outFileName)

         if returnCode != self.constants.NOERROR:
            print "There was a problem extracting the variables"
            

      for prefix in self.PREFIXES:

         fileString = task.filePrefix + "." + prefix + \
             "." + task.year + task.month + task.day + \
             ".extracted.nc4"

         fileName = task.destinationPath + "/" + \
             task.year + "/" + task.month + \
             "/" + fileString
                    
         outFileName = task.destinationPath + "/" + task.year + "/" + task.month + "/" + \
             task.filePrefix + "." + prefix + "." + task.year + task.month + task.day + \
             ".nc4"

         systemCommand = "cp " + fileName + " " + outFileName
         returnCode = os.system(systemCommand)
         
         print "returnCode: ", returnCode

         if prefix == "tavg3_3d_trb_Ne":
            print "about to rename lev lev_edges"
            gmiNetCdfObject.renameFields (["lev"], ["lev_edges"], "-v", outFileName)
            self.GEOS5FIELDS.append ("lev_edges")
            self.fieldMap["tavg3_3d_trb_Ne"] = ["KH", "lev_edges"]

         

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / SSAI
   #
   # DESCRIPTION: 
   #---------------------------------------------------------------------------  

   def regridAndDumpHdfFiles (self, task):
      print "Starting Avg3D regridAndDumpHdfFiles"
      try:
         self.regridFilesAllResolutions (task)
         GmiGEOS5DasFields.convertHdf5ToHdf4 (self, task)
         print "Dumping converting hdf5 to hdf4"
         GmiGEOS5DasFields.dumpFieldsToNetcdf (self, task)
         print "Done dumping to netcdf"
      except: 
         raise

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / SSAI
   #
   # DESCRIPTION: 
   #---------------------------------------------------------------------------  

   def regridFilesAllResolutions (self, task):
      print "Starting Avg3D regridFilesAllResolutions"
      
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
                      ".nc4"
         
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
         for resolution in ["1x1.25", "2x2.5", "0.625x0.5"]:

            destinationFile = destinationPath + "/" + \
                              fileString[0:len(fileString)-3] + \
                              resolution + ".hdf"
               


            print destinationFile

            if resolution == "0.625x0.5": 
               systemCommand = "cp " + sourceFile + " " + destinationFile
               print systemCommand
               sysOut = os.system(systemCommand)
               print "return Code: ", sysOut

            else: 
               exitMutexes.append (thread.allocate_lock())
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
   # The 3D collections contain the full range of spatial variables we need
   #---------------------------------------------------------------------------  
   def extract1DVariables (self, task):

      print "in extract1DVariables()"

      basePath = self.basePath
      basePathWithDate = self.basePath + task.year + task.month + task.day + "."
      print "basePath: ", basePath
      print "basePathWithDate: ", basePathWithDate

      for resolution in ["1x1.25", "2x2.5", "0.625x0.5"]:
         filesToMerge = []

         fileName = basePath + "tavg3_3d_cld_Nv"  + self.endPath + "." + resolution + ".nc"

         print "fileName: ", fileName

         for var in ['lat', 'lon', 'time', 'lev']:

            newFileName = basePathWithDate + var + "." + resolution + ".nc"
            print "newFileName: ", newFileName

            if os.path.exists (newFileName): os.remove(newFileName)

            self.netCdfObject.extractSubsetOfVariables ([var], fileName, newFileName)
            self.netCdfObject.resolveFieldDimensions (newFileName, 0, [var], [var], thread.allocate_lock())

            
            filesToMerge.append (newFileName)

         print "FILES TO MERGE: ", filesToMerge
         self.netCdfObject.mergeFilesIntoNewFile (filesToMerge, basePathWithDate + "1D." + resolution + ".nc")

         for file in filesToMerge:
            print "Removing this file: ", file
            if os.path.exists (file): os.remove(file)

                                                     
                
   def resolveFakeDimensions (self, task):      
      print "about to start threads for resolving dimensions..."

      netCdfObject = GmiNetCdfFileTools ()
      exitMutexes = []
      dimensions = ['time', 'lev', 'lat', 'lon']

      count = 0
      for prefix in self.PREFIXES:

         for resolution in ["2x2.5", "1x1.25", "0.625x0.5"]:

            fileName = self.basePath + prefix  + self.endPath + "." + resolution + ".nc"
            if not os.path.exists (fileName): raise fileName + " does not exist! ERROR"

            if prefix == "tavg3_3d_trb_Ne"  or prefix == "tavg3_3d_mst_Ne" \
                   or prefix == "tavg3_3d_mst_Ne_set2":
               dimensions = ['time', 'lev_edges', 'lat', 'lon']
            else:
               dimensions = ['time', 'lev', 'lat', 'lon']

            print "resolving dimensions for: ", fileName
            print self.fieldMap [prefix]

            # for each file type fix the fake dimensions
            exitMutexes.append (thread.allocate_lock())
            thread.start_new (netCdfObject.resolveFieldDimensions, \
                (fileName, count, self.fieldMap[prefix], dimensions, exitMutexes[count]))
            
            count = count + 1
         
      #----------------------------------------------------------------
      # Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass
    
      print "All threads returned from resolveFieldDimensions"

      
               
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine puts all cloud variables - TAUCLI, TAUCLW, CLOUD, and
   # and calculated OPTDEPTH (TAUCLI+TAUCLW) for 0.625 only
   # TAUCLI and TAUCLW are in tavg3_3d_cld_Nv
   # and CLOUD is in tavg3_3d_cld_Nv+set2
   #---------------------------------------------------------------------------
   def extractCloudFieldsNativeGrid (self, task):
      constants = GmiAutomationConstants()

      resolution = '0.625x0.5'
      
      tauInputFile = task.sourcePath + "/" + task.year \
          + "/" + task.month + "/" + task.filePrefix + \
          ".tavg3_3d_cld_Nv." + task.year + task.month + \
          task.day + "." + resolution + ".nc"
      cloudInputFile = task.sourcePath + "/" + task.year \
          + "/" + task.month + "/" + task.filePrefix + \
          ".tavg3_3d_cld_Nv_set2." + task.year + task.month + \
          task.day + "." + resolution + ".nc"

      cloudFileOut = task.sourcePath + "/" + task.year \
          + "/" + task.month + "/" + task.filePrefix + \
          ".CLOUD." + task.year + task.month + \
          task.day + "." + resolution + ".nc"

      cloudFileOut1 = task.sourcePath + "/" + task.year \
          + "/" + task.month + "/" + task.filePrefix + \
          ".CLOUD1." + task.year + task.month + \
          task.day + "." + resolution + ".nc"

      cloudFileOut2 = task.sourcePath + "/" + task.year \
          + "/" + task.month + "/" + task.filePrefix + \
          ".CLOUD2." + task.year + task.month + \
          task.day + "." + resolution + ".nc"

      returnCode = self.netCdfObject.extractSubsetOfVariables \
          (["TAUCLI", "TAUCLW"], tauInputFile, cloudFileOut1)

      print "returnCode after TAUCLI and TAUCLW extraction: ", returnCode

      returnCode = self.netCdfObject.extractSubsetOfVariables \
          (["CLOUD"], cloudInputFile, cloudFileOut2)

      print "returnCode after CLOUD extraction: ", returnCode 

      optDepthFile = task.sourcePath + "/" + task.year + "/" + task.month + \
          "/" + task.filePrefix + ".tavg3_3d_optDepth." + task.year + \
          task.month + task.day + "." + resolution + ".nc"
      systemCommand = constants.NCAPPATH + \
          "ncap -s \"OPTDEPTH=(TAUCLI+TAUCLW)\" " + \
          cloudFileOut1 + " " + optDepthFile
         
      print systemCommand
      returnCode = os.system (systemCommand)
      print "returnCode from ncap command which creates OPTDEPTH: ", returnCode


      # put OPTDEPTH into cloudFileOut
      returnCode = self.netCdfObject.mergeFilesIntoNewFile ([optDepthFile, cloudFileOut1, cloudFileOut2], \
                                                               cloudFileOut)
      print "returnCode from merging files: ", returnCode
      
      # this was probably nice, but then stopped working at some point
      #self.netCdfObject.addAttributesToFile(cloudFileOut, "visible_optical_depth", "1", "OPTDEPTH")
         
      # Since this file will not be part of the regular collections
      # make the time dimension a record dim here
      returnCode = self.netCdfObject.makeDimensionRecordDimension (cloudFileOut, 'time')
      print "returnCode from making time record dimension: ", returnCode
         
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Sets up the threads and calls the merge routine.
   #---------------------------------------------------------------------------    
   def mergeAllFilesIntoOne (self, task):    
      exitMutexes = []
      count = 0
      parallelTools = GmiParallelTools (task.archType)
      systemCommands = []

      for resolution in self.RESOLUTIONS:

         exitMutexes.append (thread.allocate_lock ())
         thread.start_new (self.mergePrefixes, \
                           (task, resolution, exitMutexes[count]))
         count = count + 1

      #----------------------------------------------------------------
      # Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # First merges all the files specified by the self.PREFIXES array
   # into a file with the self.GMIPREFIX.  
   #---------------------------------------------------------------------------    
   def mergePrefixes (self, task, resolution, exitMutex):
      basePath = task.destinationPath + "/" + \
                 task.year + "/" + task.month + \
                 "/"
      constants = GmiAutomationConstants ()

      fileNames = []
      for prefix in self.PREFIXES:
         print prefix
         metFile = ""
         newFile = basePath + \
                   task.filePrefix + "." + \
                   prefix + "." + \
                   task.year + task.month + \
                   task.day + "." + \
                   resolution + ".nc"
            
         fileNames.append (newFile)
            
      outFileName = basePath + task.filePrefix + "." + \
                    self.GMIPREFIX + "." + task.year + \
                    task.month + task.day + "." + \
                    resolution + ".nc"

      print "merging the files: ", fileNames

      self.netCdfObject.mergeFilesIntoNewFile ( \
               fileNames, outFileName)
         
      print "mergePrefixes ACQUIRING MUTEX"
      exitMutex.acquire ()



