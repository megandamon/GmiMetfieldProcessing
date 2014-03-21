#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         October 30th, 2007
#
# DESCRIPTION:
# This is the class for averaged 2D fields.
#------------------------------------------------------------------------------
from GmiAutomationConstants import GmiAutomationConstants
from GmiAutomationTools import GmiAutomationTools
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiLogUtilities import GmiLogUtilities
from GmiMetFieldTask import GmiMetFieldTask
from GmiGEOS5DasFields import GmiGEOS5DasFields
from GmiPreStage import GmiPreStage
from CommonUtilities import CommonUtilities
from GmiGFIORemapTools import GmiGFIORemapTools
from GmiParallelTools import GmiParallelTools
import os
import re
import sys
import thread
from time import sleep
from datetime import datetime
from datetime import timedelta

class GmiGEOS5DasAvg2D (GmiGEOS5DasFields):
   def __init__(self):
      GmiGEOS5DasFields.__init__(self)
      self.netCdfObject = GmiNetCdfFileTools ()      
      self.TYPE = "avg2D"
      self.SOURCESTYLE = "GMAO"
      self.GMIPREFIX = "tavg3"
      self.DIR = "diag"
      self.RECORDS = []
      self.AVERAGEDRECORDS = self.RECORDS
      self.constants = GmiAutomationConstants ()
                   
      self.resetPrefixesAndFields ()

   def resetPrefixesAndFields (self):
      self.PREFIXES = []
      self.GEOS5FIELDS = []


      self.PREFIXES.append ("tavg1_2d_rad_Nx")
      self.GEOS5FIELDS.append ("EMIS") 
      self.GEOS5FIELDS.append ("ALBEDO")
      self.GEOS5FIELDS.append ("ALBNIRDF")
      self.GEOS5FIELDS.append ("ALBNIRDR")
      self.GEOS5FIELDS.append ("ALBVISDF")
      self.GEOS5FIELDS.append ("ALBVISDR")

      self.PREFIXES.append ("tavg1_2d_lnd_Nx")
      self.GEOS5FIELDS.append ("GRN") #tavg1_2d_lnd_Nx
      self.GEOS5FIELDS.append ("LAI") #tavg1_2d_lnd_Nx
      self.GEOS5FIELDS.append ("SNODP") #tavg1_2d_lnd_Nx 540,361

      self.PREFIXES.append ("tavg1_2d_slv_Nx")
      self.GEOS5FIELDS.append ("QV10M") 
      self.GEOS5FIELDS.append ("SLP")
      self.GEOS5FIELDS.append ("T10M") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("TROPPB") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("TROPQ")
      self.GEOS5FIELDS.append ("TROPT")
      self.GEOS5FIELDS.append ("U2M") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("U50M") 
      self.GEOS5FIELDS.append ("V2M") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("V50M")#tavg1_2d_slv_Nx

      self.PREFIXES.append ("tavg1_2d_flx_Nx")
      self.GEOS5FIELDS.append ("RHOA")
      self.GEOS5FIELDS.append ("BSTAR") 
      self.GEOS5FIELDS.append ("FRSEAICE")
      self.GEOS5FIELDS.append ("USTAR") 
      self.GEOS5FIELDS.append ("Z0M") 

      #self.PREFIXES.append ("tavg3_2d_chm_Fx") # add later (it is on the reduced grid)
      #self.GEOS5FIELDS.append ("CLDTOT")
      #self.GEOS5FIELDS.append ("GWETROOT")
      #self.GEOS5FIELDS.append ("GWETTOP")
      #self.GEOS5FIELDS.append ("PARDF") 
      #self.GEOS5FIELDS.append ("PARDR")
      #self.GEOS5FIELDS.append ("PBLH") 
      #self.GEOS5FIELDS.append ("PRECANV") 
      #self.GEOS5FIELDS.append ("PRECCON")
      #self.GEOS5FIELDS.append ("PRECLSC")
      #self.GEOS5FIELDS.append ("PRECSNO")
      #self.GEOS5FIELDS.append ("QV2M")
      #self.GEOS5FIELDS.append ("SWGNET") #tavg3_2d_chm_Fx
      #self.GEOS5FIELDS.append ("T2M") 
      #self.GEOS5FIELDS.append ("TS")
      #self.GEOS5FIELDS.append ("U10M") #tavg3_2d_chm_Fx
      #self.GEOS5FIELDS.append ("V10M") #tavg3_2d_chm_Fx


 











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
   # The routine that drives the processing for the avg 2d fields.
   #---------------------------------------------------------------------------    
   def prepareGEOSFields (self, task):
      self.basePath = task.destinationPath + "/" + \
                      task.year + "/" + task.month + \
                      "/" + task.filePrefix + "."
      self.endPath = "." + task.year + task.month + \
                     task.day

      print "in Avg 2D processing routine ..."      



      self.PREFIXES = []
      self.GEOS5FIELDS = []
      self.PREFIXES.append ("tavg1_2d_rad_Nx")
      self.GEOS5FIELDS.append ("EMIS") 
      self.GEOS5FIELDS.append ("ALBEDO")
      self.GEOS5FIELDS.append ("ALBNIRDF")
      self.GEOS5FIELDS.append ("ALBNIRDR")
      self.GEOS5FIELDS.append ("ALBVISDF")
      self.GEOS5FIELDS.append ("ALBVISDR")

      print "Avg2D is calling its own routine for rad_Nx fields ..."
      self.regridAndDumpHdfFiles (task)
      print "done with rad_Nx fields"

      self.PREFIXES = []
      self.GEOS5FIELDS = []
      self.PREFIXES.append ("tavg1_2d_lnd_Nx")
      self.GEOS5FIELDS.append ("GRN") 
      self.GEOS5FIELDS.append ("LAI") 
      self.GEOS5FIELDS.append ("SNODP") 

      print "Avg2D is calling its own routine for lnd_Nx fields ..."
      self.regridAndDumpHdfFiles (task)
      print "done with lnd_Nx fields"

      self.PREFIXES = []
      self.GEOS5FIELDS = []
      self.PREFIXES.append ("tavg1_2d_slv_Nx")
      self.GEOS5FIELDS.append ("QV10M") 
      self.GEOS5FIELDS.append ("SLP")
      self.GEOS5FIELDS.append ("T10M") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("TROPPB") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("TROPQ")
      self.GEOS5FIELDS.append ("TROPT")
      self.GEOS5FIELDS.append ("U2M") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("U50M") 
      self.GEOS5FIELDS.append ("V2M") #tavg1_2d_slv_Nx
      self.GEOS5FIELDS.append ("V50M")#tavg1_2d_slv_Nx

      print "Avg2D is calling its own routine for slv_Nx fields ..."
      self.regridAndDumpHdfFiles (task)
      print "done with slv_Nx fields"

      self.PREFIXES = []
      self.GEOS5FIELDS = []
      self.PREFIXES.append ("tavg1_2d_flx_Nx")
      self.GEOS5FIELDS.append ("RHOA")
      self.GEOS5FIELDS.append ("BSTAR") 
      self.GEOS5FIELDS.append ("FRSEAICE")
      self.GEOS5FIELDS.append ("USTAR") 
      self.GEOS5FIELDS.append ("Z0M") 

      print "Avg2D is calling its own routine for flx_Nx fields ..."
      self.regridAndDumpHdfFiles (task)
      print "done with flx_Nx fields"


      self.resetPrefixesAndFields ()
      

      print "Resolving fake dimensions"
      self.resolveFakeDimensions (task)
      print "Done resolving fake dimensions"      

      # extract the 1:30, 4:30, 7:30, 10:30
      # 13:30, 16:30, 19:30, and 22:30 records
      print "Extract 3-hour records"
      GmiGEOS5DasFields.extractTimeRecords (self, task, "time", "0", "22", "3")
      print "Done extracting time records"


      # now that the time records have been extracted from the rest of
      # the collections, we can add the 3 hour chm_Fx collection to the set
      print "Regriding the chm_Fx collection..."
      self.processChmFxFiles (task)
      print "Done processing chm_Fx collection"

      #For testing only      
      #self.PREFIXES.append ("tavg3_2d_chm_Fx")
      print "current resolutions: ", self.RESOLUTIONS

      print "Resolving fake dimensions for tavg3_3d_chm_Fx only"
      self.resolveFakeDimensionsChmFx (task)
      print "done resolving fake dimensions for tavg3_3d_chm_Fx only"

      # we do we call this again exactly?
      print "Extracting time records...."
      GmiGEOS5DasFields.extractTimeRecords (self, task, "time", "0", "7", "1")
      print "Done getting extracting time records"
      
      # call the merge routine
      print "Merging Avg2D files"
      self.mergeAllFilesIntoOne (task)
      print "done merging across records"

      print "Extracting necessary Avg2D fields"
      # extract only the needed variables
      GmiGEOS5DasFields.doFieldExtraction (self, task)
      print "done with field extraction"
 
      print "Making time dimension the record dimension"
      self.makeTimeDimRecordDim (task)
      print "Done making time dimension the record dimension"

      print "Avg2D exiting"

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

      print "New regridAndDumpHdfFiles!"

      try:
         print "Regridding the files to required resolutions"
         GmiGEOS5DasFields.regridFilesAllResolutions (self, task)
         print "Convert HDF5 to HDF4"
         GmiGEOS5DasFields.convertHdf5ToHdf4 (self, task)
         print "Dumping hdf fields to netcdf"
         self.dumpFieldsToNetcdf (task)
         print "Done dumping to netcdf"
      except:
         raise


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


      
   def makeTimeDimRecordDim (self, task):
      netCdfObject = GmiNetCdfFileTools ()
      fileName = self.basePath + self.GMIPREFIX + self.endPath + "." + "2x2.5.nc"
      netCdfObject.makeDimensionRecordDimension (fileName, 'time')


   def resolve1DFakeDimensions (self, task):
      print "Resolving 1D fake dimensions"
      netCdfObject = GmiNetCdfFileTools ()
      
      for resolution in ['2x2.5', '1x1.25']:
         fileName = self.basePath + "tavg1_2d_flx_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim108"], ["fakeLat"], "-d", fileName)
         netCdfObject.renameFields(["fakeDim109"], ["fakeLon"], "-d", fileName)

         fileName = self.basePath + "tavg1_2d_lnd_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim114"], ["fakeLat"], "-d", fileName)
         netCdfObject.renameFields(["fakeDim115"], ["fakeLon"], "-d", fileName)

         fileName = self.basePath + "tavg1_2d_rad_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim108"], ["fakeLat"], "-d", fileName)
         netCdfObject.renameFields(["fakeDim109"], ["fakeLon"], "-d", fileName)

         fileName = self.basePath + "tavg1_2d_slv_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim114"], ["fakeLat"], "-d", fileName)
         netCdfObject.renameFields(["fakeDim115"], ["fakeLon"], "-d", fileName)

         fileName = self.basePath + "tavg3_2d_chm_Fx" + self.endPath + "." + resolution + ".nc"
         print fileName

         if resolution == "2x2.5":
            netCdfObject.renameFields(["fakeDim93"], ["fakeLat"], "-d", fileName)
            netCdfObject.renameFields(["fakeDim94"], ["fakeLon"], "-d", fileName)
         if resolution == "1x1.25":
            netCdfObject.renameFields(["lat"], ["fakeLat"], "-d", fileName)
            netCdfObject.renameFields(["lon"], ["fakeLon"], "-d", fileName)


      

   def renameRemainingTimeDims (self, task):
      netCdfObject = GmiNetCdfFileTools ()
      
      for resolution in ['2x2.5', '1x1.25']:
         fileName = self.basePath + "tavg1_2d_flx_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim110"], ["fakeTimeDim"], "-d", fileName)

         fileName = self.basePath + "tavg1_2d_lnd_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim116"], ["fakeTimeDim"], "-d", fileName)

         fileName = self.basePath + "tavg1_2d_rad_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim110"], ["fakeTimeDim"], "-d", fileName)

         fileName = self.basePath + "tavg1_2d_slv_Nx" + self.endPath + "." + resolution + ".nc"
         print fileName
         netCdfObject.renameFields(["fakeDim116"], ["fakeTimeDim"], "-d", fileName)

         fileName = self.basePath + "tavg3_2d_chm_Fx" + self.endPath + "." + resolution + ".nc"
         print fileName

         if resolution == "2x2.5":
            netCdfObject.renameFields(["fakeDim95"], ["fakeTimeDim"], "-d", fileName)
            netCdfObject.renameFields(["time"], ["fakeTimeDim"], "-v", fileName)
         if resolution == "1x1.25":
            netCdfObject.renameFields(["time"], ["fakeTimeDim"], "-d", fileName)
            netCdfObject.renameFields(["time"], ["fakeTimeDim"], "-v", fileName)
            



   def resolveFakeDimensionsChmFx (self, task):
      netCdfObject = GmiNetCdfFileTools ()

      resolution = "2x2.5"
      fileName = self.basePath + "tavg3_2d_chm_Fx"  + self.endPath + "." + resolution + ".nc"
      if not os.path.exists (fileName): raise fileName + " does not exist! ERROR"
            # for each file type fix the fake dimensions

      netCdfObject.resolveFieldDimensions (fileName, 0, self.GEOS5FIELDS, ['time', 'lat', 'lon'], thread.allocate_lock())
         
      print "Returned from resolve field dimensions for chm_Fx"


      
   def resolveFakeDimensions (self, task):      
      netCdfObject = GmiNetCdfFileTools ()

      print "about to start threads for resolving dimensions..."

      exitMutexes = []
      count = 0
      for prefix in self.PREFIXES:
         for resolution in ["2x2.5", "1x1.25"]:
            fileName = self.basePath + prefix  + self.endPath + "." + resolution + ".nc"
            if not os.path.exists (fileName): raise fileName + " does not exist! ERROR"

            # for each file type fix the fake dimensions
            exitMutexes.append (thread.allocate_lock())
            thread.start_new (netCdfObject.resolveFieldDimensions, \
                (fileName, count, self.GEOS5FIELDS, ['time', 'lat', 'lon'], exitMutexes[count]))
            
            count = count + 1
         
      #----------------------------------------------------------------
      # Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass
    

      print "All threads returned from resolveFieldDimensions"

      
   def processChmFxFiles (self, task):
      destinationPath = task.destinationPath +  "/" + task.year + "/" + task.month + "/" 
      self.PREFIXES.append ("tavg3_2d_chm_Fx") 
      self.GEOS5FIELDS.append ("CLDTOT")
      self.GEOS5FIELDS.append ("GWETROOT")
      self.GEOS5FIELDS.append ("GWETTOP")
      self.GEOS5FIELDS.append ("PARDF") 
      self.GEOS5FIELDS.append ("PARDR")
      self.GEOS5FIELDS.append ("PBLH") 
      self.GEOS5FIELDS.append ("PRECANV") 
      self.GEOS5FIELDS.append ("PRECCON")
      self.GEOS5FIELDS.append ("PRECLSC")
      self.GEOS5FIELDS.append ("PRECSNO")
      self.GEOS5FIELDS.append ("QV2M")
      self.GEOS5FIELDS.append ("SWGNET") #tavg3_2d_chm_Fx
      self.GEOS5FIELDS.append ("T2M") 
      self.GEOS5FIELDS.append ("TS")
      self.GEOS5FIELDS.append ("U10M") #tavg3_2d_chm_Fx
      self.GEOS5FIELDS.append ("V10M") #tavg3_2d_chm_Fx

      savePrefixes = self.PREFIXES
      self.PREFIXES = ["tavg3_2d_chm_Fx"] 
      self.RESOLUTIONS = ["2x2.5"]
    
      GmiGEOS5DasFields.regridAndDumpHdfFiles (self, task)

      # rename original file to 1x1.25 
      basePath = self.basePath + "tavg3_2d_chm_Fx." + task.year + \
                 task.month + task.day
      print basePath + ".hdf"
      returnCode = os.rename (basePath + ".hdf", basePath + ".1x1.25.hdf")
     

      # dump original to netcdf since it is already 1x1.25
      desintationPath = task.destinationPath + "/" + task.year + "/" + task.month + "/"
      returnCode = self.netCdfObject.dumpHdfEosToNetcdf (basePath + ".1x1.25.hdf", destinationPath)
      if returnCode != self.constants.SYS_SUCCESS:
        raise "There was a problem related to dumping HDF-EOS to Netcdf!"

   
      oldNames = ["TIME_EOSGRID", "XDim_EOSGRID", "YDim_EOSGRID"]
      newNames = ["time", "lon", "lat"]
      for arg in ["-d", "-v"]:
          self.netCdfObject.renameFields (oldNames, newNames, arg, basePath + ".1x1.25.nc") 
       
 
      self.netCdfObject.changeTimeDimToUnlim (8, basePath + ".1x1.25.nc", thread.allocate_lock(), "yes")   
      self.PREFIXES = savePrefixes
      self.RESOLUTIONS.append ("1x1.25")

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
      print self.RESOLUTIONS
      for resolution in self.RESOLUTIONS:
         exitMutexes.append (thread.allocate_lock ())
         thread.start_new (self.mergeAveragedRecords, \
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
   def mergeAveragedRecords (self, task, resolution, exitMutex):
      basePath = task.destinationPath + "/" + \
                 task.year + "/" + task.month + \
                 "/"
      netCdfObject = GmiNetCdfFileTools ()

      
      fileNames = []
      for prefix in self.PREFIXES:
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
      netCdfObject.mergeFilesIntoNewFile ( \
               fileNames, outFileName)

      print "mergeAveragedRecords ACQUIRING MUTEX"
      exitMutex.acquire ()

