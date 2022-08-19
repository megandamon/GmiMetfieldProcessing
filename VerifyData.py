#!/usr/bin/python

__author__ = 'Megan Damon'
__version__ = '0.0'
__date__ = '2013/06/07'

import getopt
import sys
import os
import _thread
import cProfile
import pstats
import string
import glob

from CommonUtilities import CommonUtilities
from GmiAutomationTools import GmiAutomationTools
from GmiAutomationConstants import GmiAutomationConstants
from GmiMetFieldTask import GmiMetFieldTask
from GmiPreStage import GmiPreStage
from GmiLogUtilities import GmiLogUtilities
from IoRoutines import IoRoutines
from GmiDasTasks import GmiDasTasks
from GmiNetCdfFileTools import GmiNetCdfFileTools

NUM_ARGS = 1

def usage ():
    print("usage: VerifyData.py [-i]")
    sys.exit (0)

#---------------------------------------------------------------
# START:: Get options from command line
#---------------------------------------------------------------
optList, argList = getopt.getopt(sys.argv[1:],'i:')
if len (optList) != NUM_ARGS:
   usage ()
   sys.exit (0)

configFile = optList[0][1]

#---------------------------------------------------------------
# objects 
#---------------------------------------------------------------
netCdfTools = GmiNetCdfFileTools ()
gmiAutomationObject = GmiAutomationTools ()
ioRoutines = IoRoutines ()
comUtilities = CommonUtilities ()

#---------------------------------------------------------------
# Read input file and set up internal variables
#---------------------------------------------------------------
configOptions = netCdfTools.readFileAndReturnFileLines (configFile)
processDirectory = comUtilities.returnValue (configOptions, "RUN_DIR")
archiveDirectory = comUtilities.returnValue (configOptions, "ARCHIVE_PATH")
mailTo = comUtilities.returnValue (configOptions, "MAIL_TO")
archiveSystem = comUtilities.returnValue (configOptions, "ARCHIVE_SYSTEM")
logFile = comUtilities.returnValue (configOptions, "LOG_FILE")
realTime = comUtilities.returnValue (configOptions, "REAL_TIME")
lockFile = comUtilities.returnValue (configOptions, "LOCK_FILE")
autoConstants = GmiAutomationConstants ()
taskFile = lockFile + ".tasks"
resolutions = ["2x2.5", "1x1.25"]



#---------------------------------------------------------------
# objects that need config information to construct
#---------------------------------------------------------------
dasTasks = GmiDasTasks (realTime)

if not os.path.exists (logFile):
    print("Log file does not exist")
    sys.exit(0)

#----------------------------------------------------------------
# Construct the tasks in memory and on disk
#----------------------------------------------------------------
try:
   gmiAutomationObject.createTasks (configOptions, processDirectory, mailTo, \
                                    realTime, taskFile)
except:
    print("Problem creating tasks from: ", taskFile)


report = []
files = []

# for each day:
for task in gmiAutomationObject.tasks:

    if len (gmiAutomationObject.tasks) == 0:
        print("No dates found!")
    else: 
        for resolution in resolutions: 
            basePath = archiveDirectory + "/" + \
                resolution + "/" + task.year + \
                "/" + task.filePrefix + "."
            sourceFile = basePath + task.year + task.month + \
                task.day + "." + resolution + ".nc"
            files = glob.glob(sourceFile)
            
            if files == []:
                report.append (task.year + task.month + task.day + " " + resolution + ", DNE")

            else:
                sourceFile = files[0]

                # does it exist? if yes, check resolution file sizes
                if (os.path.exists(sourceFile)): 
                    fileSize = os.path.getsize(sourceFile)
                    if resolution == "2x2.5" and fileSize < 716650000:
                        report.append (task.year + task.month + task.day + " " + resolution + ", FILE SIZE")

                    elif resolution == "1x1.25" and fileSize < 2850800000:
                        report.append (task.year + task.month + task.day + " " + resolution + ", FILE SIZE")

                else:   # if no, add to report
                    report.append (task.year + task.month + task.day + " " + resolution + ", DNE")


# email or print report
print("Reporting...")
for line in report:
    print(line)

os.remove(taskFile)

