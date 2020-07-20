#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / SSAI
# DATE:         Feb 8 2016
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
import _thread
from time import sleep
from datetime import datetime
from datetime import timedelta



class GmiGEOS5DasAvg2D (GmiGEOS5DasFields):
   def __init__(self):
      
      GmiGEOS5DasFields.__init__(self)
      self.netCdfObject = GmiNetCdfFileTools ()      
      self.TYPE = "avg2D"
      self.SOURCESTYLE = "GMAO"
      self.GMIPREFIX = "tavg2d"
      self.DIR = "diag"
      self.RECORDS = []
      self.AVERAGEDRECORDS = self.RECORDS
      self.constants = GmiAutomationConstants ()
                   
      self.resetPrefixesAndFields ()

   def resetPrefixesAndFields (self):
      self.PREFIXES = []
      self.GEOS5FIELDS = []

      #MERRA-2 
      # 1-hr
      self.PREFIXES.append ("tavg1_2d_slv_Nx")
      self.GEOS5FIELDS.append ("QV10M") 
      self.GEOS5FIELDS.append ("SLP")
      self.GEOS5FIELDS.append ("T10M") 
      self.GEOS5FIELDS.append ("TROPPB") 
      self.GEOS5FIELDS.append ("TROPQ")
      self.GEOS5FIELDS.append ("TROPT")
      self.GEOS5FIELDS.append ("U2M") 
      self.GEOS5FIELDS.append ("U50M") 
      self.GEOS5FIELDS.append ("V2M") 
      self.GEOS5FIELDS.append ("V50M")
      self.GEOS5FIELDS.append ("QV2M")
      self.GEOS5FIELDS.append ("T2M")
      self.GEOS5FIELDS.append ("TS")
      self.GEOS5FIELDS.append ("U10M")
      self.GEOS5FIELDS.append ("V10M")


      # 1-hr
      self.PREFIXES.append ("tavg1_2d_flx_Nx")
      self.GEOS5FIELDS.append ("RHOA")
      self.GEOS5FIELDS.append ("BSTAR") 
      self.GEOS5FIELDS.append ("FRSEAICE")
      self.GEOS5FIELDS.append ("USTAR") 
      self.GEOS5FIELDS.append ("Z0M") 
      self.GEOS5FIELDS.append ("PBLH") 
      self.GEOS5FIELDS.append ("PRECANV")
      self.GEOS5FIELDS.append ("PRECCON")
      self.GEOS5FIELDS.append ("PRECLSC")
      self.GEOS5FIELDS.append ("PRECSNO")

      # 1-hr
      self.PREFIXES.append ("tavg1_2d_rad_Nx")
      self.GEOS5FIELDS.append ("EMIS") 
      self.GEOS5FIELDS.append ("ALBEDO")
      self.GEOS5FIELDS.append ("ALBNIRDF")
      self.GEOS5FIELDS.append ("ALBNIRDR")
      self.GEOS5FIELDS.append ("ALBVISDF")
      self.GEOS5FIELDS.append ("ALBVISDR")
      self.GEOS5FIELDS.append ("CLDTOT")
      self.GEOS5FIELDS.append ("SWGNT")

      # 1-hr
      self.PREFIXES.append ("tavg1_2d_lnd_Nx")
      self.GEOS5FIELDS.append ("GRN") 
      self.GEOS5FIELDS.append ("LAI") 
      self.GEOS5FIELDS.append ("SNODP")
      self.GEOS5FIELDS.append ("GWETROOT")
      self.GEOS5FIELDS.append ("GWETTOP")
      self.GEOS5FIELDS.append ("PARDFLAND")
      self.GEOS5FIELDS.append ("PARDRLAND")





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


      # call the parent routine
      print("\nFirst stage processing")
      GmiGEOS5DasFields.regridAndDumpHdfFiles (self, task)
      
      # print("Resolving fake dimensions")
      # self.resolveFakeDimensions (task)
      # print("Done resolving fake dimensions") 

      print("\nExtract 3-hour records")
      GmiGEOS5DasFields.extractTimeRecords (self, task, "time", "0", "23", "3")
      
      # call the merge routine
      print("\nMerging Avg2D files")
      self.mergeAllFilesIntoOne (task)

      print("\nExtracting necessary Avg2D fields")
      # extract only the needed variables
      GmiGEOS5DasFields.doFieldExtraction (self, task)

      print("\nMaking time dimension a record dimension")
      GmiGEOS5DasFields.makeTimeDimRecordDim (self, task)

      print("\nAvg2D exiting")



      
   def resolveFakeDimensions (self, task):      
      netCdfObject = GmiNetCdfFileTools ()

      print("\n About to start threads for resolving dimensions...")

      exitMutexes = []
      count = 0
      for prefix in self.PREFIXES:
         for resolution in ["2x2.5", "1x1.25", "0.625x0.5"]:
            fileName = self.basePath + prefix  + self.endPath + "." + resolution + ".nc"
            if not os.path.exists (fileName): raise fileName + " does not exist! ERROR"

            # for each file type fix the fake dimensions
            exitMutexes.append (_thread.allocate_lock())
            _thread.start_new (netCdfObject.resolveFieldDimensions, \
                (fileName, count, self.GEOS5FIELDS, ['time', 'lat', 'lon'], exitMutexes[count]))
            
            count = count + 1
         
      #----------------------------------------------------------------
      # Wait for all three threads before proceeding 
      #----------------------------------------------------------------
      for mutex in exitMutexes:
         while not mutex.locked (): 
            pass
    

      print("All threads returned from resolveFieldDimensions")

      
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
      print(self.RESOLUTIONS)
      for resolution in self.RESOLUTIONS:
         exitMutexes.append (_thread.allocate_lock ())
         _thread.start_new (self.mergeAveragedRecords, \
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

      print("\nmergeAveragedRecords ACQUIRING MUTEX for: ", resolution)
      exitMutex.acquire ()

