#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         December 14th 2006
#
# DESCRIPTION:
# This is the class for averaged eta DAS fields.
#------------------------------------------------------------------------------

from GmiDasFields import GmiDasFields
from GmiAutomationConstants import GmiAutomationConstants
from GmiNetCdfFileTools import GmiNetCdfFileTools

import os
import sys
import thread

class GmiDasFieldsAveragedEta (GmiDasFields):
   
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
      self.noParent = 0
      self.TYPE = "avg3D"
      self.FIELDS = ['SURFP','CLOUD','CMFDQR2','CMFDTR','CMFETR','CMFMC','CMFMC2','DQRL','DTCOND','DTRAIN','KVH','OMEGA','Q','T','TAUCLI','TAUCLW','U','V','ZMDQR','ZMDU','ZMED','ZMEU','ZMMD','ZMMU', 'lat', 'lon', 'time']
      self.FIELDSTOEXTRACT = ['am_42','ai_42','bm_42','bi_42','pt_42','CLOUD','CMFDQR2','CMFDTR','CMFETR','CMFMC','CMFMC2','DQRL','DTCOND','DTRAIN','KVH','OMEGA','Q','T','TAUCLI','TAUCLW','U','V','ZMDQR','ZMDU','ZMED','ZMEU','ZMMD','ZMMU']
      self.FIELDSFORUNITCONVERSION = ['SURFP']
      self.UNITCONVERSION = [1/100.]
      self.PREFIX = 'diag.eta.'
      self.GMIPREFIX = 'diag.eta'
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
      self.FILETYPE = GmiAutomationConstants.AVERAGEDETADAS
      self.HORIZONTALREGRIDNAMELISTFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                           "LLK_H_AE-2x2%5.namelist", #
                                           GmiAutomationConstants.DEFAULTDASINPUTPATH  + \
                                           "LLK_H_AE-4x5.namelist", \
                                           GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                           "LLK_V_AE-1x1%25.namelist"]

      self.HORIZONTALGRIDFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                 "LLK_Horizontal2x2%5.grid", \
                                 GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                 "LLK_Horizontal4x5.grid"]

      self.VERTICALREGRIDNAMELISTFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                         "LLK_V_AE-2x2%5.namelist", \
                                         GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                                         "LLK_V_AE-4x5.namelist", # \
                                         GmiAutomationConstants.DEFAULTDASINPUTPATH + "LLK_V_AE-1x1%25.namelist"]

      self.VERTICALGRIDFILE = [GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                               "LLK_Vertical2x2%5.nc", GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                               "LLK_Vertical4x5.nc", GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                               "LLK_Vertical1x1%25x55.nc", GmiAutomationConstants.DEFAULTDASINPUTPATH + \
                               "LLK_Vertical1x1%25x42.nc"]
    
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
   # This routine calls the parent processDasFields routine and then also
   # does the vertically regridding of the files.
   #---------------------------------------------------------------------------    
   
   def processDasFields (self, task, exitMutex):

      #------------------------------------------------------------------------
      # check if the parent routine should be called

      if self.noParent == 0:
         
         #------------------------------------------------------------------------
         # call the parent routine
         # the parent routine performs basic dumping, concatenating
         # and the horizontal the regrid for the 2x2.5 resolution files
         returnCode = GmiDasFields.processDasFields (self, task)
         if returnCode != self.constants.NOERROR:
            print "\nThere was a problem processing the das fields in the parent routine!\n"
            exitMutex.acquire ()
            return self.constants.ERROR  
      
         #------------------------------------------------------------------------
      
      else:
         returnCode = GmiDasFields.fillInNecessaryInformation (self, task)
         if returnCode != self.constants.NOERROR:
            print "\nThere was a problem processing the das fields in the parent routine!\n"
            exitMutex.acquire ()
            return self.constants.ERROR  

      #------------------------------------------------------------------------
      # do the vertical regrid on the 2x2.5 files, since this is an eta
      # field
      
      vertRegrid2x2x5FileName = self.regrid2x2x5FileName \
                                 [0:len(self.regrid2x2x5FileName)-3] + "v.nc"

      print "vertRegrid2x2 filename: ", vertRegrid2x2x5FileName

      returnCode = self.regridEtaFieldsVertically (self.regrid2x2x5FileName, \
                                                   vertRegrid2x2x5FileName, \
                                                   self.HORIZONTALGRIDFILE[0], \
                                                   self.VERTICALGRIDFILE[0], \
                                                   self.VERTICALREGRIDNAMELISTFILE[0])
      if returnCode != self.constants.NOERROR:
         print "\nThere was an error doing the vertical regrid of the 2x2.5 files!\n"
         exitMutex.acquire ()
         return returnCode

      self.regrid2x2x5FileName = vertRegrid2x2x5FileName
      print "new 2x2 file name: ", self.regrid2x2x5FileName
      #------------------------------------------------------------------------
      
      
      #------------------------------------------------------------------------      
      # do the horizontal regrid on the 4x5 files
      # here, we start from the newly created, 2x2.5 file, which has
      # already been vertically regridded
      vertRegrid4x5FileName = self.regrid4x5FileName \
                                [0:len(self.regrid4x5FileName)-3] + "v.nc"
      returnCode = self.gmiNetCdfFileTool.regridFile (self.regrid2x2x5FileName, \
                                                      vertRegrid4x5FileName, \
                                                      self.HORIZONTALREGRIDNAMELISTFILE [1], \
                                                      self.HORIZONTALGRIDFILE [1], "4x5.v-42")
      if returnCode != self.constants.NOERROR:
         print "Problem doing 4x5 horizontal regrid for ", self.FILETYPE
         exitMutex.acquire ()
         return returnCode

      self.regrid4x5FileName = vertRegrid4x5FileName
      print "new 4x5 file name: ", self.regrid4x5FileName
      #------------------------------------------------------------------------

      
      
      
      #------------------------------------------------------------------------
      # append the vertical grid information to the new 4x5 file
      # this is for variables am_42, bm_42, ai_42, and bi_42
      # a.k.a am, bm, ai, bi

      systemCommand = self.constants.NCKSPATH + "ncks --append " + \
                      self.HORIZONTALGRIDFILE [1] + " " + self.regrid4x5FileName
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         print "Problem appending ", self.HORIZONTALGRIDFILE [1] + " to " + self.regrid4x5FileName
         exitMutex.acquire ()
         return self.constants.BADSYSTEMRETURNCODE     
      #------------------------------------------------------------------------




      #------------------------------------------------------------------------
      # do the vertical regrid for the 1x1.25 files
      # this is not ready yet
      
#      returnCode = self.regridEtaFieldsVertically (self.regrid1x1x25FileName, \
#      self.VERTICALGRIDFILE[3], self.VERTICALGRIDFILE[2], self.VERTICALREGRIDNAMELISTFILE[2])  
#      if returnCode != self.constants.NOERROR:
#         return returnCode

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
   
   
   
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine vertically regrids the file.  It is intended for Eta fields
   # only.
   #--------------------------------------------------------------------------- 
   
      
   def regridEtaFieldsVertically (self, averagedEtaFileName, newFileName, \
                                  horizontalGridFile, verticalGridFile, verticalNameListFile):
      

      # append the vertical grid information to the new regridded file
      systemCommand = self.constants.NCKSPATH + "ncks --append " + verticalGridFile + " " + averagedEtaFileName
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         print "Problem appending ", self.verticalGridFile, " to " + averagedEtaFileName
         return self.constants.BADSYSTEMRETURNCODE      

   
      # regrid the file vertically
      returnCode = self.gmiNetCdfFileTool.regridFile (averagedEtaFileName, newFileName, \
                                                      verticalNameListFile, horizontalGridFile, "v-42")
      if returnCode != self.constants.NOERROR:
         print "Problem doing vertical regrid for ", self.FILETYPE, " return Code is : ", returnCode
         return returnCode   

         
      # remove the old averaged eta regridded file
      systemCommand = self.constants.RMPATH + 'rm ' + averagedEtaFileName
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE
      
      # TODO_move me
      # extract the subset of variables 
      returnCode = self.gmiNetCdfFileTool.extractSubsetOfVariables \
                   (self.FIELDSTOEXTRACT, newFileName)
      if returnCode != self.constants.NOERROR:
         print "Problem doing field extraction for ", self.FILETYPE
         return returnCode   
      
      return self.constants.NOERROR
         
