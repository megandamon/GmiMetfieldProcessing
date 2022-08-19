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
import _thread
from numpy import *
from pathlib import Path


#from pynetcdf import *
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiAutomationConstants import GmiAutomationConstants


class GmiCloud:
    def __init__(self):
        self.params = GmiAutomationConstants()
    def __del__(self):
        pass
    
    # This routine uses the CLOUD, OPTDEPTH, TAUCLW, and TAUCLI
    # vars in the cloudFile below (native res)
    # to create those variables are on the 1 and 2 degree grids
    # the cloud vars at all resolutions are merged into 
    # the tavg3d file at the end of the routine
    def createCloudVars (self, path, year, month, day, \
                                     filePrefix):


        print("\nin createCloudVars")

        
        netCdfObject = GmiNetCdfFileTools()

        nativeRes = "0.625x0.5" 
        # CLOUD, calculated OPTDEPTH (TAUCLW + TAUCLI) 
        # are in this file
        cloudFile = path + "/" + year \
            + "/" + month + "/" + filePrefix + \
            ".CLOUD." + year + month + \
            day + "." + nativeRes + ".nc"



        print("\nnative degree file: ", cloudFile)

        if not os.path.exists(cloudFile):
            print ("Native degree file does not exist: ", cloudFile)            
            sys.exit(0)

            
        #for resolution in ["1x1.25", "2x2.5"]:
        for resolution in ["1x1.25"]:

            print ("\n\nCreating optdepth for: ", resolution)
            
            newFileName = re.sub (nativeRes, resolution, cloudFile)
            newFileName = newFileName + ".hdf"

            sysCommand = self.params.OPTEXEC + " " + cloudFile + " " + newFileName + " " + \
                resolution + " 8"

            print("\nExecuting: \n", sysCommand)

            os.system (sysCommand)

            returnCode = netCdfObject.doHdfDumpToNetCdf (['OPTDEPTH', 'TAUCLW', 'TAUCLI', 'CLOUD'], \
                                                             newFileName, \
                                                             newFileName + "opt.nc", \
                                                             0, \
                                                             _thread.allocate_lock())
            if returnCode != self.params.NOERROR:
                raise Exception("Problem dumping tau fields to netcdf")

            print ("\nRemoving: ", newFileName)
            os.remove (newFileName)


            lowResFileName = path + "/" + year \
                + "/" + month + "/" + filePrefix + \
                ".tavg3d." + year + month + \
                day + "." + resolution + ".nc"

            print ("\nMerging files: ", newFileName + "opti.nc", lowResFileName)
            returnCode = netCdfObject.mergeFilesIntoNewFile ([newFileName + "opt.nc", lowResFileName], \
                                                         lowResFileName)
            if returnCode != self.params.NOERROR:
                raise Exception("Problem mergingin files")

            os.remove(newFileName + "opt.nc")


        highResFileName = path + "/" + year \
            + "/" + month + "/" + filePrefix + \
            ".tavg3d." + year + month + \
            day + "." + nativeRes + ".nc"

        returnCode = netCdfObject.mergeFilesIntoNewFile ([cloudFile, highResFileName], highResFileName)
        return self.params.NOERROR


    def calculate2DegreeFrom1DegreeOptDepth (self, path, year, month, day, \
                                             filePrefix):
        fileName = path + "/" + year + "/" + month + "/" + \
                   filePrefix + "." + year + month + day + ".1x1.25.nc"
        lowResFileName = re.sub ("1x1.25", "2x2.5", fileName)

        if not os.path.exists (fileName) or not os.path.exists (lowResFileName):
            raise Exception("One of these files does not exist: " + fileName + " or " +  lowResFileName)

        netCdfObject = GmiNetCdfFileTools()
        # create CLOUD for 1x1.25 file
        netCdfObject.createNewVarWithNcap(fileName, "CLOUD=(CFAN+CFCU+CFLS)", "CLOUD")
        # create OPTDEPTH for 1x1.25 file
        netCdfObject.createNewVarWithNcap(fileName, "OPTDEPTH=(TAUCLI+TAUCLW)", "OPTDEPTH")

        newFileName = re.sub ("1x1.25", "2x2.5", fileName)
        newFileName = newFileName + ".hdf"
        print(newFileName)
        sysCommand = self.params.OPTEXEC + " " + fileName + " " + newFileName + \
                     " 2x2.5 8"
        print(sysCommand)
        os.system (sysCommand)
    
        returnCode = netCdfObject.doHdfDumpToNetCdf (['OPTDEPTH', 'TAUCLW', 'TAUCLI', 'CLOUD'], \
                                                         newFileName, \
                                                         newFileName + "opt.nc", \
                                                         0, \
                                                         _thread.allocate_lock())
        if returnCode != self.params.NOERROR:
            raise Exception("Problem dumping tau fields to netcdf")
        os.remove (newFileName)

        returnCode = netCdfObject.mergeFilesIntoNewFile ([newFileName + "opt.nc", lowResFileName], \
                                                         lowResFileName)
        if returnCode != self.params.NOERROR:
            raise Exception("Problem mergingin files")
        os.remove(newFileName + "opt.nc")
    
        return self.params.NOERROR
        

    def createOptDepthSimple(fileName):
        if not os.path.exists(fileName):
            raise fileName(" does not exit!")

        netCdfObject = GmiNetCdfFileTools()
        
        sysCommand = self.params.NCAPPATH + \
                     "ncap2 -s \"OPTDEPTH=(TAUCLW+TAUCLI)\" " + \
                     fileName + " " + fileName + ".tau.nc"
        
        print(sysCommand)
        returnCode = os.system(sysCommand)
        if returnCode != self.params.SYS_SUCCESS:
            raise Exception("Problem creating OPTDEPTH with ncap")
        
        returnCode = netCdfObject.mergeFilesIntoNewFile([fileName + ".tau.nc", \
                                                         fileName], fileName)
        if returnCode != "NOERROR":
            raise Exception("Problem merging the files")
        

    def createCloudAreaFractionSimple (self, fileName):
        if not os.path.exists(fileName):
            raise fileName(" does not exit!")

        netCdfObject = GmiNetCdfFileTools()
        
        sysCommand = self.params.NCAPPATH + \
                     "ncap2 -s \"CLOUD=(CFAN+CFCU+CFLS)\" " + \
                     fileName + " " + fileName + ".cloud.nc"
        
        print(sysCommand)
        returnCode = os.system(sysCommand)
        if returnCode != self.params.SYS_SUCCESS:
            raise Exception("Problem creating CLOUD with ncap")
        
        returnCode = netCdfObject.mergeFilesIntoNewFile([fileName + ".cloud.nc", \
                                                         fileName], fileName)
        print(returnCode)
            

    
                     
        
    def doSomething(self):
        print("doSomething")
        

    
    
    
