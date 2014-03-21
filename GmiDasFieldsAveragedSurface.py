#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         December 14th 2006
#
# DESCRIPTION:
# This is the class for averaged surface DAS fields.
#------------------------------------------------------------------------------

from GmiDasFields import GmiDasFields
from GmiAutomationConstants import GmiAutomationConstants

import thread
import os

class GmiDasFieldsAveragedSurface (GmiDasFields):
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):

      GmiDasFields.__init__(self)

      self.SOURCESTYLE = "GMAO"
      self.TYPE = "avg2D"
      self.FIELDS = ['ALBEDO','ALDIF','ALDIR','ASDIF','ASDIR','FLNS','FLNSC','FLNT','FLNTC','FSNS','FSNSC','GWETROOT','GWETTOP','PHIS','LHFX','ORO','OSR','OSRCLR','PARDF','PARDR','PBLH','PREACC','PRECC','PRECL','Q10M','Q2M','SHFX','SLP','SNOWH','SOLIN','T10M','T2M','TAUX','TAUY','TQ','TROPP','TROPQ','TROPT','TSKIN','U10M','U2M','USTAR','V10M','V2M','Z0H','Z0M','ZMPR', 'lat', 'lon', 'time']
      self.FIELDSFORUNITCONVERSION = ['SLP', 'TROPP']
      self.UNITCONVERSION = [1/100., 1/100.]
      self.PREFIX = 'diag.sfc.'
      self.GMIPREFIX = 'diag.sfc'
      self.EXPECTEDNUMBEROFFILES = 8
      self.RECORDS = []
      self.RECORDS.append ("0130")
      self.RECORDS.append ("0430")
      self.RECORDS.append ("0730")
      self.RECORDS.append ("1030")
      self.RECORDS.append ("1330")
      self.RECORDS.append ("1630")
      self.RECORDS.append ("1930")
      self.RECORDS.append ("2230")      
      self.TIMEDELTA = 3
      self.LLKDIRECTORY = 'diag/'
      self.DIR = 'diag'
      self.FILETYPE = GmiAutomationConstants.AVERAGEDSURFACEDAS
      self.HORIZONTALREGRIDNAMELISTFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                           "LLK_H_AS-2x2%5.namelist", \
                                           GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                           "LLK_H_AS-4x5.namelist"]
      self.HORIZONTALGRIDFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                 "LLK_Horizontal2x2%5.grid", \
                                 GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                 "LLK_Horizontal4x5.grid"]
 
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
   # This routine calls the parent processDasFields routine and does
   # the horizantal regrid for the 4x5 files
   #---------------------------------------------------------------------------    
   
   def processDasFields (self, task, exitMutex):
      
      
      #------------------------------------------------------------------------
      # call the parent routine
      
      returnCode = GmiDasFields.processDasFields (self, task)
      if returnCode != self.constants.NOERROR:
         
         print "\nThere was a problem processing the das fields in the parent routine!\n"
         exitMutex.acquire()
         return self.constants.ERROR  
      
      #------------------------------------------------------------------------
         
         
         
         
         
      #------------------------------------------------------------------------
      # regrid the new file to 4x5 resolution
      
      returnCode = self.gmiNetCdfFileTool.regridFile (self.regrid2x2x5FileName, \
                                                      self.regrid4x5FileName, \
                                                      self.HORIZONTALREGRIDNAMELISTFILE [1], \
                                                      self.HORIZONTALGRIDFILE [1], "4x5")
      if returnCode != self.constants.NOERROR:
         print "Problem doing 4x5 horizontal regrid for ", self.FILETYPE
         exitMutex.acquire()
         return returnCode              
      
      #------------------------------------------------------------------------


      systemCommand = "mv " + self.regrid2x2x5FileName + " " + task.destinationPath + "/" + \
                      task.year + "/" + task.month + "/" + task.filePrefix + \
                      "." + self.PREFIX + task.year + \
                      task.month + task.day + ".2x2.5.nc"
      systemReturnCode = os.system (systemCommand)

      systemCommand = "mv " + self.regrid4x5FileName + " " + task.destinationPath + "/" + \
                      task.year + "/" + task.month + "/" + task.filePrefix + \
                      "." + self.PREFIX + task.year + \
                      task.month + task.day + ".4x5.nc"
      systemReturnCode = os.system (systemCommand)


      exitMutex.acquire()
      return self.constants.NOERROR
