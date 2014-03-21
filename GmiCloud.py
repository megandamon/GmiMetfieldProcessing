#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         September 27th 3010
#
# DESCRIPTION:
# Contains routines that create total cloud area fraction and optical depth.
#------------------------------------------------------------------------------

import os
import sys
import datetime
import re
import thread
from numpy import *
from pynetcdf import *
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiAutomationConstants import GmiAutomationConstants


class GmiCloud:
    def __init__(self):
        self.params = GmiAutomationConstants()
    def __del__(self):
        pass


    def calculate2DegreeFrom1DegreeOptDepth (self, path, year, month, day, \
                                             filePrefix):
        fileName = path + "/" + year + "/" + month + "/" + \
                   filePrefix + "." + year + month + day + ".1x1.25.nc"
        lowResFileName = re.sub ("1x1.25", "2x2.5", fileName)

        if not os.path.exists (fileName) or not os.path.exists (lowResFileName):
            raise "One of these files does not exist: " + fileName + " or " +  lowResFileName

        netCdfObject = GmiNetCdfFileTools()
        # create CLOUD for 1x1.25 file
        netCdfObject.createNewVarWithNcap(fileName, "CLOUD=(CFAN+CFCU+CFLS)", "CLOUD")
        # create OPTDEPTH for 1x1.25 file
        netCdfObject.createNewVarWithNcap(fileName, "OPTDEPTH=(TAUCLI+TAUCLW)", "OPTDEPTH")

        newFileName = re.sub ("1x1.25", "2x2.5", fileName)
        newFileName = newFileName + ".hdf"
        print newFileName
        sysCommand = self.params.OPTEXEC + " " + fileName + " " + newFileName + \
                     " 2x2.5 8"
        print sysCommand
        os.system (sysCommand)
    
        returnCode = netCdfObject.doHdfDumpToNetCdf (['OPTDEPTH', 'TAUCLW', 'TAUCLI', 'CLOUD'], \
                                                         newFileName, \
                                                         newFileName + "opt.nc", \
                                                         0, \
                                                         thread.allocate_lock())
        if returnCode != self.params.NOERROR:
            raise "Problem dumping tau fields to netcdf"
        os.remove (newFileName)

        returnCode = netCdfObject.mergeFilesIntoNewFile ([newFileName + "opt.nc", lowResFileName], \
                                                         lowResFileName)
        if returnCode != self.params.NOERROR:
            raise "Problem mergingin files"
        os.remove(newFileName + "opt.nc")
    
        return self.params.NOERROR
        

    def createOptDepthSimple(fileName):
        if not os.path.exists(fileName):
            raise fileName, " does not exit!"

        netCdfObject = GmiNetCdfFileTools()
        
        sysCommand = self.params.NCAPPATH + \
                     "ncap -s \"OPTDEPTH=(TAUCLW+TAUCLI)\" " + \
                     fileName + " " + fileName + ".tau.nc"
        
        print sysCommand
        returnCode = os.system(sysCommand)
        if returnCode != self.params.SYS_SUCCESS:
            raise "Problem creating OPTDEPTH with ncap"
        
        returnCode = netCdfObject.mergeFilesIntoNewFile([fileName + ".tau.nc", \
                                                         fileName], fileName)
        if returnCode != "NOERROR":
            raise "Problem merging the files"
        

    def createCloudAreaFractionSimple (self, fileName):
        if not os.path.exists(fileName):
            raise fileName, " does not exit!"

        netCdfObject = GmiNetCdfFileTools()
        
        sysCommand = self.params.NCAPPATH + \
                     "ncap -s \"CLOUD=(CFAN+CFCU+CFLS)\" " + \
                     fileName + " " + fileName + ".cloud.nc"
        
        print sysCommand
        returnCode = os.system(sysCommand)
        if returnCode != self.params.SYS_SUCCESS:
            raise "Problem creating CLOUD with ncap"
        
        returnCode = netCdfObject.mergeFilesIntoNewFile([fileName + ".cloud.nc", \
                                                         fileName], fileName)
        print returnCode
            

    
                     
        
    def doSomething(self):
        print "doSomething"
        

    
    
    
