#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         October 30th, 2007
#
# DESCRIPTION:
# This is the class for the inst2D (instantaneous).
# GEOS5DAS produces these fields 8 times per at the hours (GMT):
# 00, 03, 06, 09, 12, 15, 18, 21
# Only the 00, 06, 12, 18 records are needed.
#------------------------------------------------------------------------------

from GmiAutomationConstants import GmiAutomationConstants
from GmiAutomationTools import GmiAutomationTools
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiLogUtilities import GmiLogUtilities
from GmiMetFieldTask import GmiMetFieldTask
from GmiGEOS5DasFields import GmiGEOS5DasFields
from GmiParallelTools import GmiParallelTools
from GmiGFIORemapTools import GmiGFIORemapTools

import os
import re
import sys
import thread
from datetime import datetime
from datetime import timedelta
from numpy import *
from pynetcdf import *

class GmiGEOS5DasInst2D (GmiGEOS5DasFields):
   def __init__(self):
      GmiGEOS5DasFields.__init__(self)
      self.TYPE = "inst2D"
      self.SOURCESTYLE = "GMAO"
      self.GMIPREFIX = "inst3_3d_chm_Ne"
      self.DIR = "diag"
      self.RECORDS = []
      self.AVERAGEDRECORDS = self.RECORDS
      self.gmiNetCdfObject = GmiNetCdfFileTools ()
      self.resetPrefixesAndFields ()


   def resetPrefixesAndFields (self):
      self.PREFIXES = []
      self.PREFIXES.append ("inst3_3d_chm_Ne")
      self.GEOS5FIELDS = []
      self.GEOS5FIELDS.append ("PLE") #inst3_3d_chm_Ne 540, 361
      self.constants = GmiAutomationConstants ()                  

   def __del__(self):
      pass

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # The routine that drives the processing for the inst 2d fields.
   #---------------------------------------------------------------------------    
   def prepareGEOSFields (self, task):

      self.basePath = task.destinationPath + "/" + \
          task.year + "/" + task.month + \
          "/" + task.filePrefix + "."
      self.endPath = "." + task.year + task.month + \
          task.day
      self.resetPrefixesAndFields ()
      
      # call the parent routine
      print "First stage processing"
      GmiGEOS5DasFields.regridAndDumpHdfFiles (self, task)
      print "Completed first stage processing"

      print "Resolving fake dimensions"
      self.resolveFakeDimensions (task)
      print "Done resolving fake dimensions"

      print "Extracting surface pressure"
      self.extractSurfacePressure (task)
      print "Done extracting surface pressure"

      print "Extracting time records"
      GmiGEOS5DasFields.extractTimeRecords (self, task, "time", "0", "7", "1")
      print "Done extracting time records"
      
      print "Extracting necessary Inst2D fields"
      GmiGEOS5DasFields.doFieldExtraction (self, task)
      print "Done with field extraction"

      print "Making time dimension a record dimension"
      GmiGEOS5DasFields.makeTimeDimRecordDim (self, task)
      print "Done Making time dimension a record dimension"


   def resolveFakeDimensions (self, task):

      exitMutexes = []
      count = 0
      for prefix in self.PREFIXES:
         for resolution in ["2x2.5", "1x1.25"]:
            fileName = self.basePath + prefix  + self.endPath + "." + resolution + ".nc"
            if not os.path.exists (fileName): raise fileName + " does not exist! ERROR"

            # for each file type fix the fake dimensions
            exitMutexes.append (thread.allocate_lock())
            thread.start_new (self.gmiNetCdfObject.resolveFieldDimensions, \
                (fileName, count, self.GEOS5FIELDS, ['time', 'lev_edges', 'lat', 'lon'], exitMutexes[count]))
            
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
   # This routine extracts the air pressure from the inst2D file, then
   # extracts just the surface layer.
   #--------------------------------------------------------------------------- 
   def extractSurfacePressure (self, task):

      print "in extract surface press"
      self.GEOS5FIELDS.append('PS')
      
      for resolution in GmiGEOS5DasFields.RESOLUTIONS:
         print "extracting surface pressure for: ", resolution
         fileName = task.destinationPath + "/" + \
                    task.year + "/" + task.month + \
                    "/" + task.filePrefix + "." + \
                    self.GMIPREFIX + "." + task.year + \
                    task.month + task.day + "." +  \
                    resolution + ".nc"
         surfPressFileName = task.destinationPath + "/" + \
                             task.year + "/" + task.month + \
                             "/" + task.filePrefix + "." + \
                             self.GMIPREFIX + "." + task.year + \
                             task.month + task.day + "." +  \
                             resolution + "PS.nc"
         returnCode = self.gmiNetCdfObject.doVariableExtractionWithHyperSlabs ("-v", ['PLE'], \
                                                                          'lev_edges', \
                                                                          72, 72, 1, \
                                                                          fileName,\
                                                                          surfPressFileName)

         if returnCode != self.constants.NOERROR:
            raise "There was a problem extracting the surface pressure for: ", resolution

      
         # Get PLE from the file and put it in physical 2D, which is
         # time, lat, lon 
         press2D = self.getPressureIn2D (surfPressFileName, 'PLE')
      
         # put the 2D pressure into the file
         returnCode = self.create2DPressureVariable (fileName, \
                                                     ('time', 'lat', 'lon'), \
                                                     'PS', float32, press2D, \
                                                     'PLE')
         if returnCode != self.constants.NOERROR:
            raise "There was a problem creating the 2D pressure variable"
      
         # extract the necessary variables
         self.gmiNetCdfObject.extractSubsetOfVariables(self.GEOS5FIELDS, fileName, fileName)



   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Returns the pressure variable in 2D physical space
   #--------------------------------------------------------------------------- 
   def getPressureIn2D (self, fileName, pressName):

      psFile = NetCDFFile (fileName, 'r')
      psValues = psFile.variables[pressName]
      oldShape = psValues.shape
      timeRecs = oldShape[0]
      latRecs = oldShape[2]
      lonRecs = oldShape[3]
      reserveValues = zeros(oldShape)
      reserveValues = psValues.getValue()
      reserveValues.ravel()
      reserveValues.shape = (timeRecs, latRecs, lonRecs)
      psFile.close()
      return reserveValues
 
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Puts the values2D into the fileName provided.
   #--------------------------------------------------------------------------- 
   def create2DPressureVariable (self, fileName, dimensions, pressName, \
                                 floatType, values2D, oldVar):
      
      psFile = NetCDFFile (fileName, 'as')
      newPressure = psFile.createVariable(pressName, 'f', dimensions)
      localPress = zeros(newPressure.shape, floatType)
      localPress[:,:,:] = values2D[:,:,:]
      newPressure.assignValue(localPress)
      psFile.close()
      return self.constants.NOERROR
