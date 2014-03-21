#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         December 14th 2006
#
# DESCRIPTION:
# This is the class for instant surface DAS fields.
#------------------------------------------------------------------------------

from GmiDasFields import GmiDasFields
from GmiAutomationConstants import GmiAutomationConstants
from GmiNetCdfFileTools import GmiNetCdfFileTools

import sys
import os
import thread

class GmiDasFieldsInstantSurface (GmiDasFields):
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      
      GmiDasFields.__init__(self)
      self.gmiNetCdfFileTool = GmiNetCdfFileTools ()
      self.SOURCESTYLE = "GMAO"
      self.TYPE = "inst2D"
      self.FIELDS = ['SURFP', 'lat', 'lon', 'time']
      self.FIELDSFORUNITCONVERSION = ['SURFP']
      self.UNITCONVERSION = [1/100.]
      self.PREFIX = 'bkg.sfc.'
      self.GMIPREFIX = 'bkg.sfc'
      self.EXPECTEDNUMBEROFFILES = 8
      self.RECORDS = []
      self.RECORDS.append ("00")
      self.RECORDS.append ("03")
      self.RECORDS.append ("06")
      self.RECORDS.append ("09")
      self.RECORDS.append ("12")
      self.RECORDS.append ("15")
      self.RECORDS.append ("18")
      self.RECORDS.append ("21")
      self.TIMEDELTA = 3
      self.LLKDIRECTORY = 'ana/'
      self.DIR = 'ana'
      self.FILETYPE = GmiAutomationConstants.INSTANTSURFACEDAS
      self.HORIZONTALREGRIDNAMELISTFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                           "LLK_H_IS-2x2%5.namelist", \
                                           GmiAutomationConstants.DEFAULTDASINPUTPATH  + \
                                           "LLK_H_IS-4x5.namelist"]
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
      
      print "calling parent"
      
      #------------------------------------------------------------------------
      # call the parent routine
      returnCode = GmiDasFields.processDasFields (self, task)
      if returnCode != self.constants.NOERROR:
         
         print "\nThere was a problem processing the das fields in the parent routine!\n"
         exitMutex.acquire ()
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
         exitMutex.acquire ()
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

      exitMutex.acquire ()
      return self.constants.NOERROR
