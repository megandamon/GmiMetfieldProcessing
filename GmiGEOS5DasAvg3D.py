#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         October 30th, 2007
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

      # This collection was on the native grid and a pain to deal with
      # found the variables it had in other files 
      # self.PREFIXES.append ("tavg3_3d_chm_Ne")

      self.GEOS5FIELDS = []
      self.GEOS5FIELDS.append ("time")
      self.GEOS5FIELDS.append ("lev")
      self.GEOS5FIELDS.append ("lon")
      self.GEOS5FIELDS.append ("lat")

      self.PREFIXES.append ("tavg3_3d_chm_Fv")
      self.GEOS5FIELDS.append ("QV") 
      self.GEOS5FIELDS.append ("T") 
      self.GEOS5FIELDS.append ("DTRAIN") 
      self.GEOS5FIELDS.append ("TAUCLI") 
      self.GEOS5FIELDS.append ("TAUCLW")
      self.GEOS5FIELDS.append ("CFAN")
      self.GEOS5FIELDS.append ("CFCU")
      self.GEOS5FIELDS.append ("CFLS")
      self.GEOS5FIELDS.append ("QI")
      self.GEOS5FIELDS.append ("QL")
      self.GEOS5FIELDS.append ("U")
      self.GEOS5FIELDS.append ("V")
      self.GEOS5FIELDS.append ("DELP") 

      self.PREFIXES.append ("tavg3_3d_chm_Fe")
      self.GEOS5FIELDS.append ("CMFMC") 
      self.GEOS5FIELDS.append ("KH")
      self.GEOS5FIELDS.append ("MFZ")
      self.GEOS5FIELDS.append ("PFICU") 
      self.GEOS5FIELDS.append ("PFLCU")
      self.GEOS5FIELDS.append ("PFILSAN")
      self.GEOS5FIELDS.append ("PFLLSAN")



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


                     
      # call the parent routine for the collections
      # on the reduced grid (already in 1x1.25)

      self.RESOLUTIONS = ["2x2.5"]

      self.PREFIXES = []
      self.GEOS5FIELDS = []
      self.PREFIXES.append ("tavg3_3d_chm_Fv")
      self.GEOS5FIELDS.append ("QV") 
      self.GEOS5FIELDS.append ("T") 
      self.GEOS5FIELDS.append ("DTRAIN") 
      self.GEOS5FIELDS.append ("TAUCLI") 
      self.GEOS5FIELDS.append ("TAUCLW")
      self.GEOS5FIELDS.append ("CFAN")
      self.GEOS5FIELDS.append ("CFCU")
      self.GEOS5FIELDS.append ("CFLS")
      self.GEOS5FIELDS.append ("QI")
      self.GEOS5FIELDS.append ("QL")
      self.GEOS5FIELDS.append ("U")
      self.GEOS5FIELDS.append ("V")
      self.GEOS5FIELDS.append ("DELP") 
      self.GEOS5FIELDS.append ("lat")
      self.GEOS5FIELDS.append ("lon")
      self.GEOS5FIELDS.append ("lev")
      self.GEOS5FIELDS.append ("time")


      print "Avg3D is calling the parent routine for the chm_Fv fields..."
      GmiGEOS5DasFields.regridAndDumpHdfFiles (self, task)
      print "done with chem_Fv fields."


      self.PREFIXES = []
      self.GEOS5FIELDS = []
      self.PREFIXES.append ("tavg3_3d_chm_Fe")
      self.GEOS5FIELDS.append ("CMFMC") 
      self.GEOS5FIELDS.append ("KH")
      self.GEOS5FIELDS.append ("MFZ")
      self.GEOS5FIELDS.append ("PFICU") 
      self.GEOS5FIELDS.append ("PFLCU")
      self.GEOS5FIELDS.append ("PFILSAN")
      self.GEOS5FIELDS.append ("PFLLSAN")
      self.GEOS5FIELDS.append ("lat")
      self.GEOS5FIELDS.append ("lon")
      self.GEOS5FIELDS.append ("lev")
      self.GEOS5FIELDS.append ("time")


      print "Avg3D is calling the parent routine for the chm_Fe fields..."
      GmiGEOS5DasFields.regridAndDumpHdfFiles (self, task)
      print "done with chem_Fe fields."

      self.RESOLUTIONS.append ("1x1.25")
      print "Done with the 3D parent routine calls"
      
      self.resetPrefixesAndFields ()

      # dump HDF-EOS 1x1.25 original files to NETCDF  
      print "Convert HDF-EOS 1x1.25 files to Netcdf..."
      self.doDumpHdfEosToNetcdf (task)
      print "Done converting HDF-EOS files"

      # no longer used, but could be useful later
      # process the tavg3_3d_chm_Ne collection
      # print "Process the tavg3_3d_chm_Ne collection..."
      # self.processChmNeFields (task)
      # print "Done processing the tavg3_3d_chm_Ne collection"
      #----------------------------------------#
      # For testing only
      # self.PREFIXES.append ("tavg3_3d_chm_Ne")
      #----------------------------------------#

      print "Extracting and saving lat, lon, lev, and time dimensions"
      self.extract1DVariables (task)
      print "Done extracting and saving lat, lon, lev, and time dimensions"
   
      print "Resolving fake dimensions"
      self.resolveFakeDimensions (task)
      print "Done resolving fake dimensions"

      print "Extracting time records..."
      GmiGEOS5DasFields.extractTimeRecords (self, task, "time", "0", "7", "1")
      print "Done extracting time records"

      # new calculation
      print "Calculating OPTDEPTH, TAUCLI, TAUCLW, and CLOUD..."
      self.calculateOptDepthVarsAppendToCldV (task)
      print "Done with calculating optical depth"

      #----------------------------------------#
      #For testing only 
      #self.GEOS5FIELDS.append('CLOUD')
      #self.GEOS5FIELDS.append('OPTDEPTH')
      #self.PREFIXES.append ("tavg3_3d_cloud")
      #----------------------------------------#

      # for native collection Chm_Ne
      # print "Making time dimension the record dimension"
      # self.netCdfObject.makeDimensionRecordDimension \
      #    (self.basePath + "tavg3_3d_chm_Ne"  + self.endPath + ".1x1.25.nc", "time")
      # print "Done making time dimension the record dimension"

      print "Renaming lev to lev_edges in the ChmFe file"
      self.renameLevInChmFe ()
      print "Done enaming lev to lev_edges in the ChmFe file"

      print "Merging the collection..."
      self.mergeAllFilesIntoOne (task)
      print "done with merging across 3D types"


      # may no longer be needed b/c of new reset process (see above)
      # extract only the needed variables
      print "Extracting necessary Avg3D fields..."
      GmiGEOS5DasFields.doFieldExtraction (self,task)
      print "done with field extraction"

      print "Making time dimension a record dimension"
      self.netCdfObject.makeDimensionRecordDimension  (self.basePath + self.GMIPREFIX + self.endPath + ".2x2.5.nc", "time")
      print "Done Making time dimension a record dimension"

      print "Avg3D exiting"

 

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   #---------------------------------------------------------------------------  
   def renameLevInChmFe (self):
      print "in routine to rename lev in chemFe file"
      fileName = self.basePath + "tavg3_3d_chm_Fe" + self.endPath + ".1x1.25.nc"
      print fileName
      self.netCdfObject.renameFields (['lev'], ['lev_edges'], '-d', fileName)
      self.netCdfObject.renameFields (['lev'], ['lev_edges'], '-v', fileName)


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

      for resolution in ["1x1.25", "2x2.5"]:
         filesToMerge = []

         # extract each 1D var from chm_Fv file into it's own file and resolve fake dimensions
         fileName = basePath + "tavg3_3d_chm_Fv"  + self.endPath + "." + resolution + ".nc"
         print "fileName: ", fileName

         for var in ['lat', 'lon', 'time', 'lev']:

            newFileName = basePathWithDate + var + "." + resolution + ".nc"
            print "newFileName: ", newFileName

            if os.path.exists (newFileName): os.remove(newFileName)

            self.netCdfObject.extractSubsetOfVariables ([var], fileName, newFileName)
            self.netCdfObject.resolveFieldDimensions (newFileName, 0, [var], [var], thread.allocate_lock())

            
            filesToMerge.append (newFileName)

# native collection currently not being used
#         chmFileName = basePath + "tavg3_3d_chm_Ne"  + self.endPath + "." + resolution + ".nc"
#         newChmFileName = basePathWithDate + "lev73." + resolution + ".nc"
#         if os.path.exists (newChmFileName): os.remove(newChmFileName)
#        self.netCdfObject.extractSubsetOfVariables (['lev'], chmFileName, newChmFileName)
#        self.netCdfObject.resolveFieldDimensions (newChmFileName, 0, ['lev'], ['lev_edges'], thread.allocate_lock())
#        self.netCdfObject.renameFields (['lev'], ['lev_edges'], '-v', newChmFileName)
#        filesToMerge.append (newChmFileName)

         print "FILES TO MERGE: ", filesToMerge
         self.netCdfObject.mergeFilesIntoNewFile (filesToMerge, basePathWithDate + "1D." + resolution + ".nc")

         for file in filesToMerge:
            print "Removing this file: ", file
            if os.path.exists (file): os.remove(file)

                                                     
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Resolve the fake dimensions created during the GFIO remap operation
   #---------------------------------------------------------------------------  
   def resolveFakeDimensions (self, task):

      exitMutexes = []
      count = 0
      for prefix in self.PREFIXES:
         for resolution in ["2x2.5", "1x1.25"]:
            fileName = self.basePath + prefix  + self.endPath + "." + resolution + ".nc"
            if not os.path.exists (fileName): raise fileName + " does not exist! ERROR"

            print "Resolving dimensions for: ", fileName

            if prefix == "tavg3_3d_chm_Fv":
               outDims = ['time', 'lev', 'lat', 'lon']
            else:
               outDims = ['time', 'lev_edges', 'lat', 'lon']

            print "outDims: ", outDims

            # for each file type fix the fake dimensions
            exitMutexes.append (thread.allocate_lock())
            thread.start_new (self.netCdfObject.resolveFieldDimensions, \
                (fileName, count, self.GEOS5FIELDS, outDims, exitMutexes[count]))
            
            count = count + 1
         
      #----------------------------------------------------------------
      # Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass
    

      print "All threads returned from resolveFieldDimensions"

                
   #-----------------------------------------------------------------------------
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: Basically, this is a wrapper routine for regridAndDumpHdfFiles
   # routine.  The chm_Ne collection prefix is added to the list.
   #-----------------------------------------------------------------------------    
   def processChmNeFields (self, task):	
      self.PREFIXES.append ("tavg3_3d_chm_Ne")
      savePrefixes = self.PREFIXES # full list
      # only process this collection, both resolutions
      self.PREFIXES = ["tavg3_3d_chm_Ne"]  
      print "Calling the parent routine..."
      GmiGEOS5DasFields.regridAndDumpHdfFiles (self, task)
      print "Done with the parent routine"
      self.PREFIXES = savePrefixes
      
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: There are collections that are already on the reduced FV 
   # grid; these collections do not need to be regridded.
   # This routine also renames the dimension variables (GFIO_remap does this)
   #---------------------------------------------------------------------------  
   def doDumpHdfEosToNetcdf (self, task):
      
      destinationPath = task.destinationPath +  "/" + task.year + "/" + task.month + "/"   
      
      for prefix in self.PREFIXES:
      
         fileName = self.basePath + prefix + self.endPath + ".hdf"
         newFileName = self.basePath + prefix + self.endPath + ".1x1.25.hdf"
         if not os.path.exists (newFileName): os.rename (fileName, newFileName)

         returnCode = self.netCdfObject.dumpHdfEosToNetcdf (newFileName, destinationPath)
         if returnCode != GmiAutomationConstants.SYS_SUCCESS:
            raise "There was a problem converting to: ", ncFileName

         ncFileName = self.basePath + prefix + self.endPath + ".1x1.25.nc"
         
         oldNames = ["TIME_EOSGRID", "XDim_EOSGRID", "YDim_EOSGRID", "Height_EOSGRID"]
         newNames = ["time", "lon", "lat", "lev"]
         for arg in ["-d", "-v"]:
             self.netCdfObject.renameFields (oldNames, newNames, arg, ncFileName)

         self.netCdfObject.changeTimeDimToUnlim (8, ncFileName, thread.allocate_lock(), "yes")


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine calls an external program to calculate TAUCLI, TAUCLW,
   # OPTDEPTH, and CLOUD at each resolution.
   #---------------------------------------------------------------------------
   def calculateOptDepthVarsAppendToCldV (self, task):
      constants = GmiAutomationConstants()
      print "resolutions: ", self.RESOLUTIONS
      for resolution in self.RESOLUTIONS:
         inputFileString = task.sourcePath + "/" + task.year \
                           + "/" + task.month + "/" + task.filePrefix + \
                           ".tavg3_3d_chm_Fv." + task.year + task.month + \
                           task.day + "." + resolution 
         inputFile = inputFileString + ".nc"
         cloudFile = inputFileString + ".cloud.nc"
         tauFile = inputFileString + ".tau.nc"

         systemCommand = constants.NCAPPATH + \
                         "ncap -s \"CLOUD=(CFAN+CFCU+CFLS)\" " + \
                         inputFile + " " + cloudFile
         print systemCommand
         returnCode = os.system (systemCommand)
         if returnCode != constants.SYS_SUCCESS:
            raise "There was a prolem calculating CLOUD"
         
         returnCode = self.netCdfObject.extractSubsetOfVariables \
                      (["TAUCLI", "TAUCLW"], inputFile, tauFile)
         returnCode = self.netCdfObject.extractSubsetOfVariables \
                      (["CLOUD"], cloudFile, cloudFile)
         returnCode = self.netCdfObject.mergeFilesIntoNewFile([cloudFile, tauFile], cloudFile)
         if returnCode != self.constants.NOERROR:
            raise "There was a problem merging CLOUD with the other 3D fields"

         optDepthFile = task.sourcePath + "/" + task.year + "/" + task.month + \
                        "/" + task.filePrefix + ".tavg3_3d_cloud." + task.year + \
                        task.month + task.day + "." + resolution + ".nc"
         systemCommand = constants.NCAPPATH + \
                         "ncap -s \"OPTDEPTH=(TAUCLI+TAUCLW)*CLOUD\" " + \
                         cloudFile + " " + optDepthFile
         
         returnCode = os.system (systemCommand)
         if returnCode != constants.SYS_SUCCESS:
            raise "There was a problem calculating OPTDEPTH using ncap"

         os.remove(cloudFile)  

         # this was probably nice, but then stopped working at some point
         #self.netCdfObject.addAttributesToFile(optDepthFile, "visible_optical_depth", "1", "OPTDEPTH")
         #self.netCdfObject.addAttributesToFile(optDepthFile, "cloud_area_fraction", "1", "CLOUD")
            
      self.GEOS5FIELDS.append('CLOUD')
      self.GEOS5FIELDS.append('OPTDEPTH')
      self.PREFIXES.append ("tavg3_3d_cloud")
         
         
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
