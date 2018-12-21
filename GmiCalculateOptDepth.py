#!/usr/bin/python

__author__ = 'Megan Damon'
__version__ = '0.0'
__date__ = '2010/08/12'

import getopt
import sys
import os
import re
import thread
from GmiCloud import GmiCloud
from GmiNetCdfFileTools import GmiNetCdfFileTools
from GmiAutomationConstants import GmiAutomationConstants

print "Creating CLOUD and OPTDEPTH..."

fields = ['CFAN' ,'CFCU','CFLS','CMFMC','DELP','DTRAIN','KH','MFZ','PFICU','PFILSAN','PFLCU','PFLLSAN','QI','QL','QV','T', 'U','V','lat','lev','lev_edges','lon','ALBEDO','ALBNIRDF','ALBNIRDR','ALBVISDF','ALBVISDR','BSTAR','CLDTOT','EMIS','FRSEAICE','GRN','GWETROOT','GWETTOP','LAI','PARDF','PARDR','PBLH','PLE','PRECANV','PRECCON','PRECLSC','PRECSNO','PS','QV10M','QV2M','RHOA','SLP','SNODP','SWGNET','T10M','T2M','TROPP','TROPQ','TROPT','TSKIN','U10M','U2M','U50M','USTAR','V10M','V2M','V50M','Z0M','FRLAKE','FRLAND','FRLANDICE','FROCEAN']

fileNamePrefix = "/discover/nobackup/projects/gmi/gmidata2/input/metfields/geos5das-merra/1x1.25/2004/MERRA300.prod.assim.200410"
cloudObject = GmiCloud()
netCdfObject = GmiNetCdfFileTools()
constants = GmiAutomationConstants()

try:

    for date in ['01', '02', '03', '04', \
                 '05', '06', '07', '08', '09', '10', \
                 '11', '12', '13', '14', '15', \
                 '16', '17', '18', '19', '20', \
                 '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31']:
        fileName = fileNamePrefix + date + ".1x1.25.nc"
        lowResFileName = re.sub ("1x1.25", "2x2.5", fileName)
        lowResFileName = re.sub ("1x1.25", "2x2.5", fileName)

        if not os.path.exists (fileName) or not os.path.exists (lowResFileName):
            raise "One of these files does not exist: " + fileName + " or " +  lowResFileName

    
        # create CLOUD for 1x1.25 file
        netCdfObject.createNewVarWithNcap(fileName, "CLOUD=(CFAN+CFCU+CFLS)", "CLOUD")
        # create OPTDEPTH for 1x1.25 file
        netCdfObject.createNewVarWithNcap(fileName, "OPTDEPTH=(TAUCLI+TAUCLW)", "OPTDEPTH")

        newFileName = re.sub ("1x1.25", "2x2.5", fileName)
        newFileName = re.sub ("1x1.25", "2x2.5", newFileName)
        newFileName = newFileName + ".hdf"
        print newFileName
        sysCommand = constants.OPTEXEC + " " + fileName + " " + newFileName + \
                    " 2x2.5 8"
        print sysCommand
        os.system (sysCommand)
    
        returnCode = netCdfObject.doHdfDumpToNetCdf (['OPTDEPTH', 'TAUCLW', 'TAUCLI', 'CLOUD'], \
                                                     newFileName, \
                                                     newFileName + "opt.nc", \
                                                     thread.allocate_lock())
        if returnCode != constants.NOERROR:
            raise "Problem dumping tau fields to netcdf"
        os.remove (newFileName)

        returnCode = netCdfObject.extractSubsetOfVariables (fields, \
                                                            lowResFileName, lowResFileName)
        if returnCode != constants.NOERROR:
            raise "Problem extracting subset of variables"

        returnCode = netCdfObject.mergeFilesIntoNewFile ([newFileName + "opt.nc", lowResFileName], \
                                                         lowResFileName)
        if returnCode != constants.NOERROR:
            raise "Problem mergingin files"
        os.remove(newFileName + "opt.nc")
    

except:
    print ("Problem with optical depth calculation: " + \
                            str (sys.exc_info ()))











print "Done"
