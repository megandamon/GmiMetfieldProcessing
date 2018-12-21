#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         December 5th 2006
#
# DESCRIPTION:
# This class contains methods for tasks such as doing a hdf to netcdf file
# conversion and other various netcdf operations.
#------------------------------------------------------------------------------

import os
import sys
import datetime
import re
from numpy import *
from pynetcdf import *
import subprocess


from GmiAutomationConstants import GmiAutomationConstants
from IoRoutines import IoRoutines
from CommonUtilities import CommonUtilities

class GmiNetCdfFileTools:
   

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      GmiNetCdfFileTools.constants = GmiAutomationConstants ()
      self.ioRoutines = IoRoutines ()
      self.utilities = CommonUtilities ()

      self.shape576 = arange(576).reshape(576)
      self.shape288 = arange(288).reshape(288)
      self.shape144 = arange(144).reshape(144)
      self.shape72 = arange(72).reshape(72)


      self.ai = zeros(73)
      self.ai[:] =  [0.010000000, 0.02000000, 0.03270000, 0.04758500, 0.06600000, 0.08934500,  \
                     0.1197030, 0.1594950, 0.2113490, 0.2785260, 0.3650410, 0.4758060,         \
                     0.6167790, 0.7951340, 1.019440, 1.300510, 1.650790, 2.084970,             \
                     2.620210, 3.276430, 4.076570, 5.046800, 6.216800, 7.619840,               \
                     9.292940, 11.27690, 13.64340, 16.45710, 19.79160, 23.73040,               \
                     28.36780, 33.81000, 40.17540, 47.64390, 56.38790, 66.60340,               \
                     78.51230, 92.36570, 108.6630, 127.8370, 150.3930, 176.9300,               \
                     201.1920, 216.8650, 224.3630, 223.8980, 218.7760, 212.1500,               \
                     203.2590, 193.0970, 181.6190, 169.6090, 156.2600, 142.9100,               \
                     128.6960, 118.9590, 109.1820, 99.36520, 89.09990, 78.83420,               \
                     70.62200, 64.36260, 58.05320, 51.69610, 45.33900, 38.98200,               \
                     32.57080, 26.09200, 19.61310, 13.13480, 6.593750, 0.04804800,             \
                     0.000000]

      self.bi = zeros(73)
      self.bi[:] =  [0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  \
                     0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,       \
                     0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,       \
                     0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,       \
                     0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,       \
                     0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,       \
                     0.000000,  0.000000,  0.000000,  0.000000,  0.000000,  0.000000,       \
                     0.007000000, 0.02800000, 0.06370000, 0.1136000, 0.1562000, 0.2004000,  \
                     0.2467000, 0.2944000, 0.3434000, 0.3929000, 0.4437000, 0.4946000,      \
                     0.5463000, 0.5810000, 0.6158000, 0.6506000, 0.6859000, 0.7212000,      \
                     0.7494000, 0.7706000, 0.7919000, 0.8133000, 0.8347000, 0.8560000,      \
                     0.8774000, 0.8989000, 0.9204000, 0.9419000, 0.9634000, 0.9850000,      \
                     1.000000]

      self.am = zeros(72)
      self.am[:] =  [0.01500000, 0.02635000, 0.04014250, 0.05679250, 0.07767250, 0.1045240,  \
                     0.1395990, 0.1854220, 0.2449380, 0.3217830, 0.4204240, 0.5462930,       \
                     0.7059570, 0.9072870,  1.159980,  1.475650,  1.867880,  2.352590,       \
                     2.948320,  3.676500,  4.561690,  5.631800,  6.918320,  8.456390,        \
                     10.28490,  12.46020,  15.05030,  18.12440,  21.76100,  26.04910,        \
                     31.08890,  36.99270,  43.90970,  52.01590,  61.49570,  72.55790,        \
                     85.43900,  100.5140,  118.2500,  139.1150,  163.6620,  189.0610,        \
                     209.0290,  220.6140,  224.1310,  221.3370,  215.4630,  207.7040,        \
                     198.1780,  187.3580,  175.6140,  162.9340,  149.5850,  135.8030,        \
                     123.8270,  114.0700,  104.2730,  94.23260,  83.96710,  74.72810,        \
                     67.49230,  61.20790,  54.87470,  48.51760,  42.16050,  35.77640,        \
                     29.33140,  22.85260,  16.37400,  9.864280,  3.320900, 0.02402400]


      self.bm = zeros(72)
      self.bm[:] =  [0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  \
                     0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  \
                     0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  \
                     0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  \
                     0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  \
                     0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.000000,  \
                     0.000000, 0.000000, 0.000000, 0.000000, 0.000000, 0.003500000,  \
                     0.01750000, 0.0458500, 0.0886500, 0.1349000, 0.1783000, 0.2235500,  \
                     0.2705500,  0.3189000, 0.3681500, 0.4183000, 0.4691500, 0.5204500,  \
                     0.5636500,  0.5984000, 0.6332000, 0.6682500, 0.7035500, 0.7353000,  \
                     0.7600000,  0.7812500, 0.8026000, 0.8240000, 0.8453500, 0.8667000,  \
                     0.8881500,  0.9096500, 0.9311500, 0.9526500, 0.9742000, 0.9925000]

      
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
   # Gets a list of variables names from a netcdf file and checks those against
   # the names in the fieldArray provided. If the variable dimensions are the 
   # same size as the outDims provided and the field is in the fieldArray,
   # the routine resolves the fake dimension names.
   #---------------------------------------------------------------------------    
   def resolveFieldDimensions (self, fileName, seed, fieldArray, outDims, exitMutex):

      print "In resolveFieldDimensions!"
      varsFromFile = self.returnNetCdfFieldsAndDims (fileName)
      print "Thread ", seed, " found :",  varsFromFile  
      
      filesToMerge = []


      # resolve dimensions on fields we care about
      for var in varsFromFile:
         print "var: ", var
         print "fieldArray: ", fieldArray

         if var == self.utilities.isValueInList (var, fieldArray) and len(varsFromFile[var]) == len(outDims):
            print "Thread ", seed, " is resolving : ", var

            # extract field
            newFileName = fileName + "." + var + ".nc"
            print newFileName
            filesToMerge.append(newFileName)
            self.extractSubsetOfVariables ([var], fileName, newFileName)

            # rename dimensions in new file
            self.renameFields (varsFromFile[var], outDims, '-d', newFileName)

      print "files to merge: ", filesToMerge
      if len(filesToMerge) > 1: 
         print "MERGING"
         systemCommand = "mv " + fileName + " " + fileName + ".orig.nc"
         returnCode = os.system(systemCommand)
         self.mergeFilesIntoNewFile (filesToMerge, fileName)
      else:
         print "NOT MERGING"
         systemCommand = "mv " + newFileName + " " + fileName
         returnCode = os.system(systemCommand)


      print "resolveFieldDimensions acquiring mutex..."
      exitMutex.acquire ()

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Changes the dimension names from inDims to outDims by way of 
   # extracting the field into a new file, and then putting in back
   # in the original file. The original field will be renamed to fieldName_old
   #---------------------------------------------------------------------------    
   def renameDimensions (self, fileName, field, inDims, outDims):

      if not os.path.exists(fileName):
         raise fileName, " does not exit!"

      # extract field
      newFileName = fileName + "." + field + ".nc"
      print newFileName

      self.extractSubsetOfVariables ([field], fileName, newFileName)

      # rename dimensions in new file
      self.renameFields (inDims, outDims, '-d', newFileName)

      # rename old field in fileName to something else
      self.renameFields ([field], [field + "_old"], '-v', fileName)

      # put new variable from new file 
      self.mergeFilesIntoNewFile ([fileName, newFileName], fileName)


      

   def createNewVarWithNcap(self, fileName, expression, variableName):
      if not os.path.exists(fileName):
         raise fileName, " does not exist!"
        
      sysCommand = self.constants.NCAPPATH + \
                   "ncap -s \"" + expression + \
                   "\" " + fileName + " " + fileName + "." + \
                   variableName + ".nc"        
      print sysCommand
      returnCode = os.system(sysCommand)
      if returnCode != self.constants.SYS_SUCCESS:
         raise "Problem creating " + variableName + \
               " with ncap"
        
      returnCode = self.mergeFilesIntoNewFile([fileName + "." + \
                                                       variableName + ".nc", \
                                                       fileName], fileName)
      if returnCode != "NOERROR":
         raise "Problem merging files into: ", fileName

      os.remove(fileName + "." + variableName + ".nc")

   def dumpHdfEosToNetcdf (self, fileName, destinationPath):
      print "in dumpHdfEosToNetcdf..."
      print "NCLPATH: ", self.constants.NCLPATH
      print "NCLLDPATH: ", self.constants.NCLPATH
      print "NCLCONVERTPATH: ",  self.constants.NCLCONVERTPATH 
      print "fileName: ", fileName
      print "destinationPath: ", destinationPath
      systemCommand = "export PATH=$PATH:" + self.constants.NCLPATH + ";" + \
                      " export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:" + \
                      self.constants.NCLPATH + ";" + self.constants.NCLCONVERTPATH + \
                      "ncl_convert2nc " + fileName + " -o " + \
                      destinationPath
      print systemCommand
      returnCode = os.system(systemCommand)
      print "returncode: ", returnCode
      
      return returnCode
      
   

   def addAttributesToFile (self, sourceFile, longName, units, variableName):
      sysCmdBase = self.constants.NCATTEDPATH + "ncatted -a "
      
      systemCommands = []
      systemCommands.append (sysCmdBase + "long_name," + variableName + ",c,c,\"" + longName + "\" " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "units," + variableName + ",c,c,\"" + units + "\" " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "missing_value," + variableName + ",c,f,1.e+15f " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "fmissing_value," + variableName + ",c,f,1.e+15f " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "scale_factor," + variableName + ",c,f,1.f " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "add_offset," + variableName + ",c,f,0.f " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "standard_name," + variableName + ",c,c,\"" + longName + "\" " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "vmin," + variableName + ",c,f,\"-1.e+30f\" " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "vmax," + variableName + ",c,f,\"1.e+30f\" " + \
                             sourceFile)
      systemCommands.append (sysCmdBase + "valid_range," + variableName + ",c,f,\"-1.e+30f, 1.e+30f\" " + \
                             sourceFile)

      for systemCommand in systemCommands:
         print systemCommand
         os.system (systemCommand)


   
   def createNewNetCDFVar (self, ncFile, varName, type, dimensions, values):
      newVar = ncFile.createVariable (varName, type, dimensions)
      newVar.assignValue(values)


   def returnNetCDFVarFromFile (self, fileName, varName):
      print "Getting variable from netcdf file: ", fileName
      print fileName
      ncFile = NetCDFFile (fileName, 'r')
      ncValues = None
      print "Getting : ", varName
      ncValues = ncFile.variables[varName]
      dimensions = ncValues.shape
      returnValues = zeros(dimensions)
      returnValues = ncValues.getValue()
      ncFile.close()
      return returnValues

   def returnNetCdfFieldsAndDims (self, fileName):
      print "Getting fields and dimensions from: ", fileName
      ncFile = NetCDFFile (fileName, 'r')
      varsFromFile = {}
      for var in ncFile.variables:
         ncValues = ncFile.variables[var]
         varsFromFile[var] = ncValues.dimensions
         
      ncFile.close()

      return varsFromFile
      

   def doNetcdfToHdfDump (self, fieldNames, netCdfFileName, hdfFileName):

      if len (fieldNames) <= 0:
         return self.constants.INVALIDINPUT
      
      if len (netCdfFileName) <= 0:
         return self.constants.INVALIDINPUT
      
      if len (hdfFileName) <= 0:
         return self.constants.INVALIDINPUT

      if not os.path.exists (netCdfFileName):
         return self.constants.NOSUCHFILE

      systemCommand = self.constants.NCDUMPPATH + "/ncdump -v "

      loopCounter = 0
      while loopCounter < len (fieldNames):
         systemCommand = systemCommand + fieldNames[loopCounter]
         if loopCounter != len (fieldNames) - 1:
            systemCommand = systemCommand + ','
         
         loopCounter = loopCounter + 1

      systemCommand = systemCommand + " " + netCdfFileName + " | " \
                      + self.constants.HDFGENPATH + "/" + "hdfgen -o " \
                      + hdfFileName
      print systemCommand
      returnCode = os.system (systemCommand)

      if returnCode != self.constants.SYS_SUCCESS:
         returnCode = self.constants.ERROR
      else:
         returnCode = self.constants.NOERROR

      return returnCode

   def makeDimensionRecordDimension (self, fileName, dimName):

      if not os.path.exists (fileName):
         raise self.constants.NOSUCHFILE 

      systemCommand = self.constants.NCECATPATH + "ncecat -O " + fileName + " " + \
          fileName + ".ncecat.nc"
      print systemCommand
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         print "Something went wrong with ncecat command!"
         raise self.constants.BADSYSTEMRETURNCODE

      print "Successfully completed ncecat command"

      systemCommand = "mv " + fileName + ".ncecat.nc " + fileName
      print systemCommand
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         print "Something went wrong with file mv"
         raise self.constants.BADSYSTEMRETURNCODE


      systemCommand = self.constants.NCPDQPATH + "ncpdq -U -O -a " + dimName + ",record " \
          + fileName + " " + fileName + ".ncpdq.nc"
      print systemCommand
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         print "Something went wrong with ncpdq command"
         raise self.constants.BADSYSTEMRETURNCODE

      print "Successfully completed ncpdq command"

      systemCommand = "mv " + fileName + ".ncpdq.nc " + fileName
      print systemCommand
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         print "Something went wrong with mv command 2"
         raise self.constants.BADSYSTEMRETURNCODE


      systemCommand = self.constants.NCWAPATH + "ncwa -O -a record " \
          + fileName + " " + fileName + ".ncwa.nc"
      print systemCommand
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         print "Something went wrong with ncwa command"
         raise self.constants.BADSYSTEMRETURNCODE

      print "Successfully completed ncwa commmand"

      systemCommand = "mv " + fileName + ".ncwa.nc " + fileName
      print systemCommand
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         print "Something went wrong with file mv 3"
         raise self.constants.BADSYSTEMRETURNCODE






   def changeTimeDimToUnlim (self, numTimes, netCdfFile, exitMutex, toExecute):

      print "in changeTimeDimToUnlim: ", netCdfFile
      if numTimes <= 0:
         raise self.constants.INVALIDINPUT
      
      if len (netCdfFile) <= 0:
         raise self.constants.INVALIDINPUT

      if not os.path.exists (netCdfFile):
         raise self.constants.NOSUCHFILE 

      temporaryFileName = netCdfFile + '.tmp'

      print temporaryFileName
      print numTimes
      print str(numTimes)
      print netCdfFile
      
      systemCommand = self.constants.NCDUMPPATHSAFE + 'ncdump ' + \
                      netCdfFile + " | " + self.constants.SED_PATH + \
                      'sed -e "s/time = ' + str(numTimes) + ' ;' \
                      '/time = UNLIMITED ;\/\/ (' + str(numTimes) + \
                      ' currently)/g" | ' + self.constants.NCGENPATH + \
                      'ncgen -o ' + temporaryFileName
      print systemCommand

      if toExecute == 'yes':
         if os.system (systemCommand) != self.constants.SYS_SUCCESS:
            raise self.constants.BADSYSTEMRETURNCODE

      os.rename (temporaryFileName, netCdfFile)
      print "changeTimeDimToUnlim ACQURING MUTEX"
      exitMutex.acquire ()



   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Adds a global attribute name with description to the netcdf file
   #---------------------------------------------------------------------------      
   
   def addGlobalAttribute (self, ncAttribute, description, netCdfFile):
      
      if len (ncAttribute) <= 0:
         raise self.constants.INVALIDINPUT
      
      if len (description) <= 0:
         raise self.constants.INVALIDINPUT

      if len (netCdfFile) <= 0:
         raise self.constants.INVALIDINPUT

      if not os.path.exists (netCdfFile):
         raise self.constants.NOSUCHFILE 

      temporaryFileName = netCdfFile + '.tmp'
      systemCommand = 'cp ' + netCdfFile + ' ' + temporaryFileName

      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         raise self.constants.BADSYSTEMRETURNCODE

   
      systemCommand = self.constants.NCATTEDPATH + 'ncatted -a ' + \
                      ncAttribute + ',global,a,c,"' + description + \
                      '" ' + temporaryFileName

      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         raise self.constants.BADSYSTEMRETURNCODE
         
      systemCommand = self.constants.MVPATH + 'mv ' + temporaryFileName + ' ' + netCdfFile
      if os.system (systemCommand) != self.constants.SYS_SUCCESS:
         raise self.constants.BADSYSTEMRETURNCODE

      return self.constants.NOERROR


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine assumes a fileName of a netcdf file.
   # The fileName will be split into 1 file for every record in the orignal
   # file.  A unit conversion can also be performed.  If a unit conversion
   # is not necessary, a value of 1 can be passed.
   #---------------------------------------------------------------------------    

   def splitFileInto1RecordPerFileByTime (self, startHourString, timeDelta, #
                                    numberOfRecords, fileName, listOfFields, #
                                    listOfFieldsForUnitConversion, listOfUnitConversions):
      
      if numberOfRecords < 0 or len (fileName) == 0:
         return self.constants.INVALIDINPUT
      
      if len (startHourString) != 4 or startHourString [3] != 'z':
         return self.constants.INVALIDINPUT

      if not os.path.exists (fileName):
         return self.constants.NOSUCHFILE      
      
      startHour = int (startHourString [1:3])
      recordCounter = 0
      
      while recordCounter < numberOfRecords:
         
         newHourString = self.getNewForecastHourString (startHour + timeDelta*recordCounter, timeDelta)
         newFileName = fileName.replace ('.nc', newHourString + '.nc')
         
         returnCode = self.doVariableExtractionWithHyperSlabs ("-v", listOfFields, 'time', #
                                           recordCounter, recordCounter, 1, fileName, newFileName)
         if returnCode != self.constants.NOERROR:
            print "Problem extracting hyper slabs"
            return returnCode
         
         recordCounter = recordCounter + 1

         # the -1 could cause problems - it is for acquiring the lock on threads 
         returnCode = self.doUnitConversion (listOfFieldsForUnitConversion [0], \
                                             listOfUnitConversions[0], newFileName, -1)
         if returnCode != self.constants.NOERROR:
            print "Problem doing unit conversion"
            return returnCode                 
      
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine returns the new forecast hour in the form of a string.
   #---------------------------------------------------------------------------        

   def getNewForecastHourString (self, currentHour, timeDelta):
      
      if currentHour < 0:
         raise self.constants.INVALIDINPUT
   
      newHour = currentHour + timeDelta
   
      while newHour >= 24:
         newHour = newHour - 24
         
      if newHour < 10:
         newHourString =  '_0' + str (newHour) + 'z'
      else:
         newHourString = '_' + str (newHour) + 'z'
         
      return newHourString
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine performs the given unit conversion.  Will return
   # INVALIDINPUT if input is not valid.
   #---------------------------------------------------------------------------      
   
   def doUnitConversion (self, field, unitConversion, fileName, exitMutex):
      
      if len (field) <= 0:
         print "doUnitConversion ACQUIRING MUTEX"
         exitMutex.acquire () 
         return self.constants.INVALIDINPUT
      
      if len (fileName) <= 0:
         print "doUnitConversion ACQUIRING MUTEX"
         exitMutex.acquire () 
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         print "doUnitConversion ACQUIRING MUTEX"
         exitMutex.acquire () 
         return self.constants.NOSUCHFILE 
      
      temporaryFileName = fileName + 'tmp'

      systemCommand = self.constants.NCAPPATH + 'ncap -O -s ' + \
                      field + '=' + field + '*' + str (unitConversion) \
                      + '. ' + fileName + " " + temporaryFileName

      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         print "doUnitConversion ACQUIRING MUTEX"
         exitMutex.acquire () 
         return self.constants.BADSYSTEMRETURNCODE
         
      systemCommand = self.constants.MVPATH + 'mv ' + temporaryFileName + ' ' + fileName
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         print "doUnitConversion ACQUIRING MUTEX"
         exitMutex.acquire () 
         return self.constants.BADSYSTEMRETURNCODE
  
      print "doUnitConversion ACQUIRING MUTEX"
      exitMutex.acquire () 

      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine extracts hyper slabs from the given fileName and puts
   # the data into the newFileName
   #---------------------------------------------------------------------------      
   
   def doVariableExtractionWithHyperSlabs (self, args, fieldNames, hyperSlabSpecification, #
                                           minSpec, maxSpec, stride, fileName, newFileName):

      print "in doVariableExtraction"
      
      if len (hyperSlabSpecification) <= 0:
         print "hyper slab spec invalid"
         return self.constants.INVALIDINPUT
      
      if len (fileName) <= 0:
         print "fileName invalid"
         return self.constants.INVALIDINPUT
      
      if len (newFileName) <= 0:
         print "newFileName invalid"
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         print "File does not exist; ", fileName
         return self.constants.NOSUCHFILE
      
      systemCommand = self.constants.NCKSPATH + 'ncks '
      if len(fieldNames) > 0 and args == "-v":
         systemCommand = systemCommand + args + " "
      
         loopCounter = 0
         while loopCounter < len (fieldNames):
            systemCommand = systemCommand + fieldNames[loopCounter]
            if loopCounter != len (fieldNames) - 1:
               systemCommand = systemCommand + ','
         
            loopCounter = loopCounter + 1

      else: print "No field names specified - will ncks all fields"

      systemCommand = systemCommand + ' -d ' + hyperSlabSpecification + ',' 
      
      if type (minSpec) != type ('string'):
         minSpec = str (minSpec)
      if type (maxSpec) != type ('string'):
         maxSpec = str (maxSpec)
      if type (stride) != type ('string'):
         stride = str (stride)
      
      systemCommand = systemCommand + minSpec + ',' + maxSpec + ',' \
                      + stride + ' ' + fileName + " " + newFileName

      print systemCommand
      systemReturnCode = os.system (systemCommand)
      print "Sys return code: ", systemReturnCode
      
      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE
            
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine does an ncdump-hdf4 on a hdf file and converts it to ncdf format.
   #---------------------------------------------------------------------------
   def doHdfDumpToNetCdf (self, fieldNames, hdfFileName, newNcdfFileName, \
                             doSed, \
                             exitMutex):  

      print "doHdfDumpToNetCdf!!!"
      
      if len (fieldNames) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      if len (hdfFileName) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      print "here333"

      if len (newNcdfFileName) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      print "here5555"
      print hdfFileName
      if not os.path.exists (hdfFileName):
         print hdfFileName, " does not exist!"
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.NOSUCHFILE
         
      systemCommand = self.constants.HDFDUMPPATH + 'ncdump -v '
      
      loopCounter = 0
      while loopCounter < len (fieldNames):
         systemCommand = systemCommand + fieldNames[loopCounter]
         if loopCounter != len (fieldNames) - 1:
            systemCommand = systemCommand + ','
         
         loopCounter = loopCounter + 1

      # add the input file name to the command
      systemCommand = systemCommand + ' ' + hdfFileName + ' | '

      # the GEOS5DAS fields have a little bug regarding
      # pressure field informaiton
      # this is a work around and should be removed
      # from the routine eventually
      if re.search ('met_e', hdfFileName):
         systemCommand = systemCommand + \
                         self.constants.SEDPATH + \
                         'sed /ak/d | ' + \
                         self.constants.SEDPATH + \
                         'sed /bk/d | '

      if doSed == 1: 
         systemCommand = systemCommand + self.constants.SEDPATH + \
             'sed /_FillValue/d | sed /ak/d | '
         
      systemCommand = systemCommand + self.constants.NCGENPATH  + \
          'ncgen -v2 -o ' + newNcdfFileName


      print systemCommand
      systemReturnCode = os.system (systemCommand)
      
      if systemReturnCode != 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.BADSYSTEMRETURNCODE

      print "doHdfDumpToNetCdf ACQUIRING MUTEX"
      exitMutex.acquire ()
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine does an ncdump-hdf4 on a hdf file and converts it to ncdf format.
   #---------------------------------------------------------------------------
   def doHdfDumpToNetCdfRelaxMaxDims (self, fieldNames, hdfFileName, newNcdfFileName, \
                             doSed, \
                             exitMutex):  

      print "doHdfDumpToNetCdf!!!"
      
      if len (fieldNames) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      if len (hdfFileName) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      print "here333"

      if len (newNcdfFileName) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      print "here5555"
      print hdfFileName
      if not os.path.exists (hdfFileName):
         print hdfFileName, " does not exist!"
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.NOSUCHFILE
         
      systemCommand = self.constants.HDFDUMPPATH + 'ncdump -v '
      
      loopCounter = 0
      while loopCounter < len (fieldNames):
         systemCommand = systemCommand + fieldNames[loopCounter]
         if loopCounter != len (fieldNames) - 1:
            systemCommand = systemCommand + ','
         
         loopCounter = loopCounter + 1

      # add the input file name to the command
      systemCommand = systemCommand + ' ' + hdfFileName + ' | '

      # the GEOS5DAS fields have a little bug regarding
      # pressure field informaiton
      # this is a work around and should be removed
      # from the routine eventually
      if re.search ('met_e', hdfFileName):
         systemCommand = systemCommand + \
                         self.constants.SEDPATH + \
                         'sed /ak/d | ' + \
                         self.constants.SEDPATH + \
                         'sed /bk/d | '

      if doSed == 1: 
         systemCommand = systemCommand + self.constants.SEDPATH + \
             'sed /_FillValue/d | sed /ak/d | '
         
      systemCommand = systemCommand + self.constants.NCGENPATHRELAX + \
          'ncgen -o ' + newNcdfFileName


      print systemCommand
      systemReturnCode = os.system (systemCommand)
      
      if systemReturnCode != 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.BADSYSTEMRETURNCODE

      print "doHdfDumpToNetCdf ACQUIRING MUTEX"
      exitMutex.acquire ()
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine converts a hdf5 file to hdf4.
   #---------------------------------------------------------------------------
   def doHdf5ToHdf4 (self, hdf5FileName, exitMutex):
      
      print "doHdf5toHdf4 "
      if len (hdf5FileName) <= 0 or not os.path.exists (hdf5FileName):
         print "doHdf5ToHdf4 INPUT ERROR!!!! Now acquiring mutex and exiting"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      systemCommand = self.constants.H5TOH4 + " " + hdf5FileName + " " + \
          hdf5FileName + "4"

      print systemCommand
      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != 0:
         print "doHdf5ToHdf4 CONVERT ERROR!!!! Now acquiring mutex and exiting"
         exitMutex.acquire ()
         return self.constants.ERROR

      systemCommand = self.constants.MVPATH + 'mv ' + hdf5FileName + "4 " + \
          hdf5FileName
      print systemCommand

      systemReturnCode = os.system (systemCommand)

      if systemReturnCode != 0:
         print "doHdf5ToHdf4 MOVE ERROR!!!! Now acquiring mutex and exiting"
         exitMutex.acquire ()
         return self.constants.ERROR

      exitMutex.acquire ()
      return self.constants.NOERROR




   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine does an ncdump-hdf4 on a hdf file and converts it to ncdf format.
   #---------------------------------------------------------------------------
   def doHdfDumpToNetCdfWithFillValue (self, fieldNames, hdfFileName, newNcdfFileName, \
                          exitMutex):  

      print "doHdfDumpToNetCdf (with fill value)"
      
      if len (fieldNames) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      if len (hdfFileName) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      if len (newNcdfFileName) <= 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      if not os.path.exists (hdfFileName):
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.NOSUCHFILE

      systemCommand = self.constants.HDFDUMPPATH + 'ncdump-hdf4 -v '

      loopCounter = 0
      while loopCounter < len (fieldNames):
         systemCommand = systemCommand + fieldNames[loopCounter]
         if loopCounter != len (fieldNames) - 1:
            systemCommand = systemCommand + ','
         
         loopCounter = loopCounter + 1

      # add the input file name to the command
      systemCommand = systemCommand + ' ' + hdfFileName + ' | '

      # the GEOS5DAS fields have a little bug regarding
      # pressure field informaiton
      # this is a work around and should be removed
      # from the routine eventually
      if re.search ('met_e', hdfFileName):
         systemCommand = systemCommand + \
                         self.constants.SEDPATH + \
                         'sed -e \"s/1e+15/9.9999999e+14f/g\" | sed /ak/d | ' + \
                         self.constants.SEDPATH + \
                         'sed /bk/d | '

      systemCommand = systemCommand + self.constants.SEDPATH + \
                      'sed -e \"s/1e+15/9.999999e+14f/g\" | sed /ak/d | ' + \
                      'sed /_FillValue/d | '
      systemCommand = systemCommand + self.constants.NCGENPATH  + \
                      'ncgen -o ' + newNcdfFileName

      print systemCommand
      systemReturnCode = os.system (systemCommand)
      
      if systemReturnCode != 0:
         print "doHdfDumpToNetCdf ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.BADSYSTEMRETURNCODE

      print "doHdfDumpToNetCdf ACQUIRING MUTEX"
      exitMutex.acquire ()
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine does an ncdump-hdf4 on a hdf file and converts it to ncdf format.
   #---------------------------------------------------------------------------
   def doHdfDumpToNetCdf2 (self, args):

      fieldNames = args[0]
      hdfFileName = args[1]
      newNcdfFileName = args[2]
      exitMutex = args[3]
      toExecute = args[4]
      
      if len (fieldNames) <= 0:
         print "doHdfDumpToNetCdf2 ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT
      
      if len (hdfFileName) <= 0:
         print "doHdfDumpToNetCdf2 ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT
      
      if len (newNcdfFileName) <= 0:
         print "doHdfDumpToNetCdf2 ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.INVALIDINPUT

      if not os.path.exists (hdfFileName):
         print "doHdfDumpToNetCdf2 ACQUIRING MUTEX"
         exitMutex.acquire ()
         return self.constants.NOSUCHFILE    
      
      systemCommand = self.constants.HDFDUMPPATH + 'ncdump-hdf4 -v '

      loopCounter = 0
      while loopCounter < len (fieldNames):
         systemCommand = systemCommand + fieldNames[loopCounter]
         if loopCounter != len (fieldNames) - 1:
            systemCommand = systemCommand + ','
         
         loopCounter = loopCounter + 1

      # add the input file name to the command
      systemCommand = systemCommand + ' ' + hdfFileName + ' | '

      # the GEOS5DAS fields have a little bug regarding
      # pressure field informaiton
      # this is a work around and should be removed
      # from the routine eventually
      if re.search ('met_e', hdfFileName):
         systemCommand = systemCommand + \
                         self.constants.SEDPATH + \
                         'sed /ak/d | ' + \
                         self.constants.SEDPATH + \
                         'sed /bk/d | '

      systemCommand = systemCommand + self.constants.SEDPATH + \
                      'sed /_FillValue/d | sed /ak/d | '
      systemCommand = systemCommand + self.constants.NCGENPATH  + \
                      'ncgen -o ' + newNcdfFileName

      if toExecute == 'yes':
         systemReturnCode = os.system (systemCommand)
      
         if systemReturnCode != 0:
            print "doHdfDumpToNetCdf2 ACQUIRING MUTEX"
            exitMutex.acquire ()
            return self.constants.BADSYSTEMRETURNCODE

      print "doHdfDumpToNetCdf2 ACQUIRING MUTEX"
      exitMutex.acquire ()

      return systemCommand

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine renames all the fields in the two-dimensional array
   # fieldNames using ncrename.  The old names are expected in 
   # first column, the new in the second.
   #---------------------------------------------------------------------------    
      
   def renameFields (self, oldNames, newNames, arg, fileName):
      
      if len (oldNames) <= 0 or len (newNames) <= 0:
         return self.constants.INVALIDINPUT
      
      if len (oldNames) != len (newNames):
         return self.constants.INVALIDINPUT
      
      if len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName):
         return self.constants.NOSUCHFILE
      
      systemCommand = self.constants.NCRENAMEPATH + 'ncrename ' 
      #systemCommand = 'ncrename ' 
      
      loopCounter = 0
      while loopCounter < len (oldNames):
         systemCommand = systemCommand + ' ' + arg + ' ' + oldNames [loopCounter] + ',' + newNames [loopCounter]
         loopCounter = loopCounter + 1
      
      systemCommand = systemCommand + ' ' + fileName
      print systemCommand
      systemReturnCode = os.system (systemCommand)
      
      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE
      
      return self.constants.NOERROR
  
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This takes the list of input file names and calls ncea to do grid point 
   # averages and writes the output to the newFileName.
   #---------------------------------------------------------------------------    
   
   def doGridPointAverages (self, listOfInputFileNames, newFileName):
      
      if len (listOfInputFileNames) <= 0:
         return self.constants.INVALIDINPUT
      
      if len (newFileName) <= 0:
         return self.constants.INVALIDINPUT
      
      systemCommand = self.constants.NCEAPATH + 'ncea -O'
      
      for fileName in listOfInputFileNames:
         
         if not os.path.exists (fileName):
            print fileName, " does not exist!"
            return self.constants.NOSUCHFILE
         
         systemCommand = systemCommand + ' ' + fileName
         
      systemCommand = systemCommand + ' ' + newFileName

      systemReturnCode = os.system (systemCommand)
      
      if systemReturnCode != 0:
         print "Bad sys return code: ", systemReturnCode
         return self.constants.BADSYSTEMRETURNCODE
      
      return self.constants.NOERROR

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This takes the list of input file names and calls ncea to do grid point 
   # averages and writes the output to the newFileName.
   #---------------------------------------------------------------------------    
   
   def doGridPointAverages2 (self, args):

      listOfInputFileNames = args[0]
      newFileName = args[1]
      exitMutex = args[2]
      toExecute = args[3]

      if len (listOfInputFileNames) <= 0:
         return self.constants.INVALIDINPUT
      
      if len (newFileName) <= 0:
         return self.constants.INVALIDINPUT
      
      systemCommand = self.constants.NCEAPATH + 'ncea -O'
      
      for fileName in listOfInputFileNames:
         
         if not os.path.exists (fileName):
            print fileName, " does not exist!"
            return self.constants.NOSUCHFILE
         
         systemCommand = systemCommand + ' ' + fileName
         
      systemCommand = systemCommand + ' ' + newFileName

      if toExecute == 'yes':
         systemReturnCode = os.system (systemCommand)
         if systemReturnCode != 0:
            print "Bad sys return code: ", systemReturnCode
            print "doGridPointAverages2 ACQURING MUTEX"
            exitMutex.acquire()
            return self.constants.BADSYSTEMRETURNCODE

      print "doGridPointAverages2 ACQURING MUTEX"
      exitMutex.acquire()
      return systemCommand

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This takes the list of input file names and calls ncrcat to concatenate
   # record variables from the input file names.  The files will be concatenated
   # into a file specified by the newFileName variable.
   #---------------------------------------------------------------------------  

   def concatenateRecordVariables (self, fieldNames, fileNames, newFileName):
      
      if len (fileNames) <= 0:
         print "in valid file names: ", fileNames
         return self.constants.INVALIDINPUT
      
      if len (newFileName) <= 0:
         print "invalid new file name: ", newFileName
         return self.constants.INVALIDINPUT

      
      for fileName in fileNames:
         if not os.path.exists (fileName):
            print "The file ", fileName, " does not exist!!"
            return self.constants.NOSUCHFILE
        
      systemCommand = self.constants.NCRCATPATH + 'ncrcat '

      if len (fieldNames) > 0 :
         systemCommand = systemCommand + '-O -v '

         loopCounter = 0
         while loopCounter < len (fieldNames):
            systemCommand = systemCommand + fieldNames[loopCounter]
            if loopCounter != len (fieldNames) - 1:
               systemCommand = systemCommand + ','
               loopCounter = loopCounter + 1 

      systemCommand = systemCommand + ' '
      loopCounter = 0
      while loopCounter < len (fileNames):
         systemCommand = systemCommand + fileNames[loopCounter] + ' '
         loopCounter = loopCounter + 1       
      
      systemCommand = systemCommand + newFileName
      systemReturnCode = os.system (systemCommand)
      
      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE
      
      
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine regrids the given file.  The regridded file name will be
   # the input fileName (assumed ending in a .nc) renamed to fileName (no .nc)
   # + regrid + -resolution + .nc
   # Precondition: fileName must end with ".nc"
   #---------------------------------------------------------------------------        
   def regridFile (self, fileName, regridNameList, gridFileName, resolution):
      
      if len (fileName) <= 0 or len (regridNameList) <= 0 or len (gridFileName) <= 0 or len(resolution) <= 0:
         return self.constants.INVALIDINPUT
      
      if not os.path.exists (fileName) or not os.path.exists (regridNameList) or not os.path.exists (gridFileName):
         return self.constants.NOSUCHFILE
      
      try:
         fileLines = self.readFileAndReturnFileLines (regridNameList)
      except:
         return self.constants.READERROR
      
      if resolution == '2x2.5':
         fileNameNoExtension = fileName [0: len (fileName) - len ('.nc')]
      
      elif resolution == '4x5':
         fileNameNoExtension = fileName [0: len (fileName) - len ('.regrid-2x2.5.nc')]
      
      elif resolution == 'v-42':
         fileNameNoExtension = fileName [0: len (fileName) - len ('.nc')]
         
      elif resolution == '4x5.v-42':
         fileNameNoExtension = fileName [0: len (fileName) - len ('.regrid-2x2.5.v-42.nc')]
      
      else:
         print "Error! The resolution is : ", resolution, "! \n"
      
      loopCounter = 0
      while loopCounter < len (fileLines):
      
         if re.search ('infile', fileLines [loopCounter]):
            fileLines [loopCounter] = "infile = '" + fileName + "',"
            
         if re.search ('outfile', fileLines [loopCounter]):
            
            if resolution == 'v-42':
               fileLines [loopCounter] = "outfile = '" + fileNameNoExtension + "." + resolution + ".nc',"
            else:
               fileLines [loopCounter] = "outfile = '" + fileNameNoExtension + ".regrid-" + resolution + ".nc',"
            
         if re.search ('grdfile', fileLines [loopCounter]):
            fileLines [loopCounter] = "grdfile = '" + gridFileName    + "',"
         
         loopCounter = loopCounter + 1
            
      # after the replacements have been made, write the contents to disk
      os.remove (regridNameList)
      returnCode = self.createNewFileAndDump(regridNameList, fileLines)
      if returnCode != self.constants.NOERROR:
         return returnCode
      
      # do the regrid of fileName
      systemCommand = self.constants.NCREGRIDPATH + "ncregrid.exe " + regridNameList + " > /dev/null"
      systemReturnCode = os.system (systemCommand)
      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE
      
      return self.constants.NOERROR
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine returns the lines in the file or an error if necessary.
   #---------------------------------------------------------------------------  
   
   def readFileAndReturnFileLines (self, fileName):
      
      try:
         fileObject = open (fileName, 'r')
         fileContents = fileObject.read ()
         fileObject.close ()
      except:
         raise self.constants.READERROR   
      
      if len (fileContents) == 0:
        raise self.constants.BADFILENAME
      
      fileLines = fileContents.splitlines()
      
      return fileLines
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine creates a new file with the contents provided from the calling
   # routine.  It will return a FILEALREADYEXISTS code if the file already
   # exists on disk.
   #---------------------------------------------------------------------------  
   def createNewFileAndDump (self, fileName, fileContents):
      
      if len (fileName) <= 0:
         return self.constants.INVALIDINPUT
      
      if os.path.exists (fileName):
         return self.constants.FILEALREADYEXISTS
      
      try:
         fileObject = open (fileName, 'w')
         
         for line in fileContents:
            fileObject.write (line + '\n')

         fileObject.close ()
      except:
         return self.constants.WRITEERROR
      
      return self.constants.NOERROR
   
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine extracts subsets of variables from the file 
   #---------------------------------------------------------------------------     
   def extractSubsetOfVariables (self, variablesToExtract, fileName, newFileName):
      print "In extractSubsetOfVariables"
 
      if len (variablesToExtract) <= 0 or len (fileName) <= 0:
         return self.constants.INVALIDINPUT

      print "file name exists?"
      print fileName
      if not os.path.exists (fileName):
         return self.constants.NOSUCHFILE

      print "after file name exists"

      # command to extract from netcdf file
      systemCommand = self.constants.NCKSPATH + 'ncks -v ' 
      
      # add each variable to the system command
      loopCounter = 0
      while loopCounter < len (variablesToExtract):
         systemCommand = systemCommand + variablesToExtract[loopCounter]
         if loopCounter != len (variablesToExtract) - 1:
            systemCommand = systemCommand + ','
         
         loopCounter = loopCounter + 1 
      
      # temporary file name to write new variable set to
      temporaryFileName = fileName + ".extract.tmp"
      
      # do the extraction
      systemCommand = systemCommand +" " + fileName + " " + temporaryFileName

 
      print systemCommand
      systemReturnCode = os.system (systemCommand)
      print "system rtn code: ", systemReturnCode

      print "checking systemREturnCode"

      if systemReturnCode != 0:
         return self.constants.BADSYSTEMRETURNCODE

      # move the temporary file back to the orginal one or
      # move it to the new file name
      if fileName == newFileName:
         returnCode = os.rename (temporaryFileName, fileName)
      else:
         print "Creating new file for: ", variablesToExtract
         returnCode = os.rename (temporaryFileName, newFileName)
      if systemReturnCode != 0:
         print "There was a problem renaming ", temporaryFileName
         return self.constants.BADSYSTEMRETURNCODE

      print "returning from extractSubsetOfVariables"
      return self.constants.NOERROR
    
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # This routine merges the list of files into one
   #---------------------------------------------------------------------------     
   def mergeFilesIntoNewFile (self, fileList, newFileName):
      
      if len (newFileName) <= 0:
         return self.constants.INVALIDINPUT
      
      loopCounter = 0
      while loopCounter < len (fileList) - 1:
         
         if not os.path.exists (fileList [loopCounter]) \
            or not os.path.exists (fileList [loopCounter+1]):
            print "one of these files does not exist!: "
            print fileList[loopCounter]
            print fileList[loopCounter+1]
            return self.constants.NOSUCHFILE

         # command to merge netcdf files
         systemCommand = self.constants.NCKSPATH + "ncks --append " + \
                         fileList [loopCounter] \
                         + " " + fileList [loopCounter + 1]
         print ""
         print ""
         print systemCommand
         
         systemReturnCode = os.system (systemCommand)
         if systemReturnCode != 0:
            print "BAD return code in mergeFilesIntoNewFile"
            return self.constants.BADSYSTEMRETURNCODE  


         loopCounter = loopCounter + 1

      print "Last file: ", fileList[loopCounter]
      print "newFileName: ", newFileName
 
      # rename the last file into the newFileName if they are not the same file
      if fileList [loopCounter] != newFileName:
         print "Renaming ", fileList [loopCounter], " to " , newFileName
         systemCommand = self.constants.MVPATH + 'mv ' + fileList [loopCounter] \
                         + ' ' + newFileName
         print "before os.system"
         systemReturnCode = os.system (systemCommand)
         print "after os.system"
         if systemReturnCode != 0:
            return self.constants.BADSYSTEMRETURNCODE  
      print "before return"
      return self.constants.NOERROR 


   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Opens the netdcdf and returns a list of variables.
   #---------------------------------------------------------------------------     
   def openNetCdfFileAndReturnVariables (self, fileName):

      if not os.path.exists (fileName):
         raise self.constants.NOSUCHFILE
         return

      variables = []

      file = NetCDFFile (fileName, 'r')

      for variable in file.variables:
         variables.append (variable)

      return variables

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Returns the netcdf variable in array form or scalar.
   #---------------------------------------------------------------------------     
   def returnVariableFromNc (self, fileName, variable):

      if not os.path.exists (fileName):
         raise self.constants.NOSUCHFILE
         return
   
      ncfile = NetCDFFile (fileName, 'r')
      values = ncfile.variables[variable]
      return values

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Returns the netcdf object
   #---------------------------------------------------------------------------     
   def returnNcObject (self, fileName):

      if not os.path.exists (fileName):
         raise self.constants.NOSUCHFILE
            
      ncFile = NetCDFFile (fileName, 'a')
      return ncFile

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Remaps the longitude coordinate by 180 degrees.
   #---------------------------------------------------------------------------     
   def remapLonCoord (self, fileName):
      if not os.path.exists (fileName):
         raise self.constants.NOSUCHFILE

      ncObject = NetCDFFile (fileName, 'as')
      lonValues = ncObject.variables["lon"]

      print lonValues.shape
      print lonValues.getValue()

      # reserve the orginal values of the variable
      reserveValues = zeros(lonValues.shape)
      reserveValues[:] = lonValues.getValue()

      newValuesLocal = zeros (lonValues.shape, float32)
      newValuesLocal[:] = reserveValues[:] + 180
      
      # the new variable will have swapped coordinates
      lonValues.assignValue (newValuesLocal)
      
      ncObject.sync()
      return


   def flipDataOnCoord5D (self, fileName, variableName, exitMutex):

      if not os.path.exists (fileName):
         print "flipDataOnCoord5D... ACQURING MUTEX"
         exitMutex.acquire()
         raise self.constants.NOSUCHFILE

      print "Found file"
      ncObject = NetCDFFile (fileName, 'as')
      fieldValues = ncObject.variables[variableName]
      fieldShape = shape(fieldValues)
      print type(fieldValues)

      print fieldShape[2]
      levArray = zeros(fieldShape[2], int)
      levArray = arange(0,fieldShape[2])
      print levArray

      midPoint = len(levArray)/2
      lastIndex = len(levArray)
      count = 1

      newShape = (fieldShape[0], 1, 1, fieldShape[3], fieldShape[4])
      saveLast = zeros(newShape, float32)
      theValues = fieldValues.getValue()
      print shape(saveLast)
      print shape(theValues)
      
      for lev in levArray[0:midPoint]:
         saveLast [:,0,0,:,:] = theValues[:,0,lastIndex-count,:,:]
         print "swapping: ", lastIndex-count, " and ", count-1
         theValues[:,0,lastIndex-count,:,:] = theValues[:,0,count-1,:,:]
         theValues[:,0,count-1,:,:] = saveLast [:,0,0,:,:]
         count = count + 1

      fieldValues.assignValue (theValues)
      ncObject.sync()



   def remapDataOnLonCoordNew (self, fileName, levFileName, filePrefix, \
                               exitMutex):

      print "in remapDataOnLonCoordNew"

      shape576 = self.shape576
      shape288 = self.shape288
      shape144 = self.shape144
      
      if not os.path.exists (fileName):
         print "NO SUCH FILE: ", fileName
         print "remapDataOnLonCorrd... ACQURING MUTEX"
         exitMutex.acquire()

      print "Opening: ", fileName


        
      ncObject = NetCDFFile (fileName, 'as')
      lonValues = ncObject.variables["lon"]
      lonSize = len (lonValues)

      print "Done opening"

      
      # calculate the beg, middle and end of lon coord
      beg = 0
      mid = len (lonValues) / 2
      end = lonSize
         
      oldCoords1 = [mid,beg]
      oldCoords2 = [end,mid]
      newCoords1 = [beg,mid]
      newCoords2 = [mid,end]


      print oldCoords1
      print newCoords1

      print " "

      print oldCoords2
      print newCoords2



      print "before for"
      for variable in ncObject.variables:
         print "Reading: ", variable
         returnValues = ncObject.variables[variable]
         sizeOfVar = len(returnValues.shape)
         print sizeOfVar
            
         if sizeOfVar == 3:
            if returnValues.shape[2:3] == shape288.shape or \
                   returnValues.shape[2:3] == shape144.shape or \
                   returnValues.shape[2:3] == shape576.shape :
               self.swapIndex3D (returnValues, oldCoords1, \
                                 oldCoords2, newCoords1, newCoords2)

         if sizeOfVar == 4:
            if returnValues.shape[3:4] == shape288.shape or \
                   returnValues.shape[3:4] == shape144.shape or \
                   returnValues.shape[3:4] == shape576.shape :
               self.swapIndex4D (returnValues, oldCoords1, \
                                 oldCoords2, newCoords1, newCoords2)

      if re.search ("2x2.5", fileName):
         globalAttr = "2x2%5x72"
      elif re.search ("1x1.25", fileName):
         globalAttr = "1x1%25x72"
      elif re.search ("0.625x0.5", fileName):
         globalAttr = "0%625x0%5x72"
      else:
         print "global attribute not supported!"
         print "remapDataOnLonCorrd... ACQURING MUTEX"
         exitMutex.acquire()
         return self.constants.ERROR

      
      setattr (ncObject, "Met_Data_Name", "GMAO_GEOS5MERRA2_" + globalAttr)

      amNc = ncObject.createVariable('am', 'f', ('lev',))      
      counter = 0
      while counter < 72:
         amNc [counter] = self.am [counter]
         counter = counter + 1

      bmNc = ncObject.createVariable('bm', 'f', ('lev',))      
      counter = 0
      while counter < 72:
         bmNc [counter] = self.bm [counter]
         counter = counter + 1


      ncObject.sync()


      print "remapDataOnLonCorrd... ACQURING MUTEX"
      exitMutex.acquire()
      return self.constants.NOERROR


   def swapIndex3D (self, returnValues, old1, old2, new1, new2):
      if len(old1) != len (old2):
         raise constants.ERROR
      if len(new1) != len (new2):
         raise constants.ERROR
      if len (old1) != len (new1):
         raise constants.ERROR
      
      count = 0
    
      # get the shape of the field and
      # and create shape for the holder array                 
      theShape = shape(returnValues)
      newShape = (theShape[0], theShape[1], old2[count]-old1[count])
      
      print "Shape of field variable: ", theShape
      print "Shape of holder variable: ", newShape
   
      
      # get all the values from the NC variable and
      # save the part of the array we are about to
      # overwrite
      theValues = returnValues.getValue()
      saveLocal = zeros(newShape, float32)
      saveLocal[:,:,:] = theValues[:,:,old1[count]:old2[count]]
      
      # swap the data
      print "about to swap: ", old1[count], ":", old2[count], "and ", new1[count], ":", new2[count]
      theValues[:,:,old1[count]:old2[count]] = theValues[:,:,new1[count]:new2[count]]
      theValues[:,:,new1[count]:new2[count]] = saveLocal[:,:,:]

      returnValues.assignValue (theValues)


   def swapIndex4D (self, returnValues, old1, old2, new1, new2):
      if len(old1) != len (old2):
         raise constants.ERROR
      if len(new1) != len (new2):
         raise constants.ERROR
      if len (old1) != len (new1):
         raise constants.ERROR

      count = 0
    
      # get the shape of the field and
      # and create shape for the holder array                 
      theShape = shape(returnValues)
      newShape = (theShape[0], theShape[1], theShape[2], old2[count]-old1[count])

      print "Shape of field variable: ", theShape
      print "Shape of holder variable: ", newShape

    
      # get all the values from the NC variable and
      # save the part of the array we are about to
      # overwrite
      theValues = returnValues.getValue()
      saveLocal = zeros(newShape, float32)
      saveLocal[:,:,:,:] = theValues[:,:,:,old1[count]:old2[count]]

      # swap the data
      print "about to swap: ", old1[count], ":", old2[count], "and ", new1[count], ":", new2[count]
      theValues[:,:,:,old1[count]:old2[count]] = theValues[:,:,:,new1[count]:new2[count]]
      theValues[:,:,:,new1[count]:new2[count]] = saveLocal[:,:,:,:]

      returnValues.assignValue (theValues)

      
   
   def copyVariableAttributesFromOrginal (self, originalNcObject, variable, newValues):

      attList = dir(originalNcObject.variables[variable])
      attList.remove('assignValue')
      attList.remove('getValue')
      attList.remove('typecode')
      
      for attribute in attList:
         setattr (newValues, attribute, getattr(originalNcObject.variables[variable], attribute))


   def swapLastIndex4D (self, variable, ncObject, oldValues, old1, old2, new1, new2, newValues):

      if len(old1) != len (old2):
         raise self.constants.ERROR
      if len(new1) != len (new2):
         raise self.constants.ERROR
      if len (old1) != len (new1):
         raise self.constants.ERROR
      
      # reserve the orginal values of the variable
      reserveValues = zeros(oldValues.shape)
      reserveValues[:,:,:,:] = oldValues.getValue()

      newValuesLocal = zeros (oldValues.shape, float32)

      count = 0
      for coordinate in old1:
         # remap the data on the last coordinate
         newValuesLocal[:,:,:,old1[count]:old2[count]] = \
                                                       reserveValues[:,:,:,new1[count]:new2[count]]
         count = count + 1

      # the new variable will have swapped coordinates
      newValues.assignValue (newValuesLocal)
      
      # put the attributes back
      self.copyVariableAttributesFromOrginal (\
                     ncObject, variable, newValues)


   def swapLastIndex5D (self, variable, ncObject, oldValues, old1, old2, new1, new2, newValues):

      if len(old1) != len (old2):
         raise self.constants.ERROR
      if len(new1) != len (new2):
         raise self.constants.ERROR
      if len (old1) != len (new1):
         raise self.constants.ERROR
      
      # reserve the orginal values of the variable
      reserveValues = zeros(oldValues.shape)
      reserveValues[:,:,:,:,:] = oldValues.getValue()

      newValuesLocal = zeros (oldValues.shape, float32)

      count = 0
      for coordinate in old1:
         # remap the data on the last coordinate
      	 print "assigning: ", old1[count], " ", old2[count], " and "
      	 print "assigning: ", new1[count], " ", new2[count], " and "
 
         newValuesLocal[:,:,:,:,old1[count]:old2[count]] = \
                                                       reserveValues[:,:,:,:,new1[count]:new2[count]]
         count = count + 1

      # the new variable will have swapped coordinates
      newValues.assignValue (newValuesLocal)
      
      # put the attributes back
      self.copyVariableAttributesFromOrginal (\
                     ncObject, variable, newValues)

 
   def swapLastIndex3D (self, variable, ncObject, oldValues, old1, old2, new1, new2, newValues):

      if len(old1) != len (old2):
         raise self.constants.ERROR
      if len(new1) != len (new2):
         raise self.constants.ERROR
      if len (old1) != len (new1):
         raise self.constants.ERROR

      # reserve the orginal values of the variable
      reserveValues = zeros(oldValues.shape)
      reserveValues[:,:,:] = oldValues.getValue()

      newValuesLocal = zeros (oldValues.shape, float32)
      
      count = 0
      for coordinate in old1:
         # remap the data on the last coordinate
         newValuesLocal[:,:,old1[count]:old2[count]] = \
                                                     reserveValues[:,:,new1[count]:new2[count]]
         count = count + 1
         
      # the new variable will have swaped coordinates
      print type(newValues), newValues.shape
      print type(newValuesLocal), newValuesLocal.shape
      
      newValues.assignValue (newValuesLocal)

      # put the attributes back
      self.copyVariableAttributesFromOrginal (\
                     ncObject, variable, newValues)
