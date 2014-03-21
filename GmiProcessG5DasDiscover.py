#!/usr/bin/python

#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# BEGIN DATE:         May 15th, 2008
#
# DESCRIPTION:
# This is the main driver for regridding the GEOS5 MERRA fields to GMI format.
# Please email Megan.R.Damon@nasa.gov for assistance.
#
# Execute this script by typing : python GmiProcessGeos5DasDiscover.py -i <config filename>
#    where <config filename> is the name of the input file which contains the
#    following information:
#
#    - RUN_DIR          : directory the python files are in;
#                         i.e. the root of your cloned repository

#    - LOCK_FILE        : File to lock out other processes from doing this job
#                         This should be a uniquely named file, as other
#                         instances will check for this file before processing metdata

#    - ARCHIVE_PATH     : Where to archive the proccessed fields when done

#    - DESTINATION_PATH : Where to hold the intermediate files while processing
#                         i.e. a place that can hold a few gigabytes of data

#    - MAIL_TO          : A comma seperated list of email addresses to send
#                         updates to.

#    - REALTIME         : flags if mode is real-time
#                         It is recommended not to change this optoin

#    - ARCHIVE_SYSTEM   : name of archive system (where ARCHIVE_PATH is)

#    - ARCH_TYPE        : Can be either serial, shared_mem, or dist_mem (single cpu execution,
#                         shared memory archtitecture - will use threads, or distributed nodes)
#                         It is recommended not to change this option.

#    - LOG_FILE         : File for keeping track of fields processed. 
#                         create this file in your RUN_DIR defined above

#    - PRESTAGE_ONLY    : Set to true if you don't want any processing to occur.  Only prestaging will be done. 

#    - SOURCE_PATH      : path to the GEOS5DAS fields for input
#                         or "ftp" in the case of MERRA data

#    - START_DATE       : first day 

#    - NUM_DAYS         : number of days to compute, including the start date
#                         should be 1 if real-time mode

#    - FILE_PREFIX      : prefix of the raw files
#                         
# See config*.in files for examples. 
#------------------------------------------------------------------------------

__author__ = 'Megan Damon'
__version__ = '0.0'
__date__ = '2008/05/15'

import getopt
import sys
import os
import thread
import cProfile
import pstats

from GmiNetCdfFileTools import GmiNetCdfFileTools
from CommonUtilities import CommonUtilities
from GmiAutomationTools import GmiAutomationTools
from GmiAutomationConstants import GmiAutomationConstants
from GmiMetFieldTask import GmiMetFieldTask
from GmiPreStage import GmiPreStage
from GmiLogUtilities import GmiLogUtilities
from GmiGEOS5DasFields import GmiGEOS5DasFields
from GmiGEOS5DasInst2D import GmiGEOS5DasInst2D
from GmiGEOS5DasAvg2D import GmiGEOS5DasAvg2D
from GmiGEOS5DasAvg3D import GmiGEOS5DasAvg3D
from IoRoutines import IoRoutines
from GmiDasTasks import GmiDasTasks

NUM_ARGS = 1

def usage ():
    print "usage: GmiProcessGeos5Das.py [-i]"
    sys.exit (0)


#---------------------------------------------------------------
# START:: Get options from command line
#---------------------------------------------------------------

print "Initializing config options from input file..."

optList, argList = getopt.getopt(sys.argv[1:],'i:')
if len (optList) != NUM_ARGS:
   usage ()
   sys.exit (0)

configFile = optList[0][1]


#---------------------------------------------------------------
# objects 
#---------------------------------------------------------------
netCdfTools = GmiNetCdfFileTools ()
comUtilities = CommonUtilities ()
gmiAutomationObject = GmiAutomationTools ()
autoConstants = GmiAutomationConstants ()
preStage = GmiPreStage ()
logUtility = GmiLogUtilities ()
dasFields = GmiGEOS5DasFields ()
ioRoutines = IoRoutines ()
inst2DObject = GmiGEOS5DasInst2D ()
avg2DObject = GmiGEOS5DasAvg2D ()
avg3DObject = GmiGEOS5DasAvg3D ()

dasObjects = []
dasObjects.append (avg2DObject)
dasObjects.append (inst2DObject)
dasObjects.append (avg3DObject)

#---------------------------------------------------------------
# Read input file and set up internal variables
#---------------------------------------------------------------
configOptions = netCdfTools.readFileAndReturnFileLines (configFile)
processDirectory = comUtilities.returnValue (configOptions, "RUN_DIR")
lockFile = comUtilities.returnValue (configOptions, "LOCK_FILE")
archiveDirectory = comUtilities.returnValue (configOptions, "ARCHIVE_PATH")
mailTo = comUtilities.returnValue (configOptions, "MAIL_TO")
destPath = comUtilities.returnValue (configOptions, "DESTINATION_PATH")
realTime = comUtilities.returnValue (configOptions, "REAL_TIME")
archiveSystem = comUtilities.returnValue (configOptions, "ARCHIVE_SYSTEM")
archType = comUtilities.returnValue (configOptions, "ARCH_TYPE")
logFile = comUtilities.returnValue (configOptions, "LOG_FILE")
preStageOnly = comUtilities.returnValue (configOptions, "PRESTAGE_ONLY")
sourcePath = comUtilities.returnValue (configOptions, "SOURCE_PATH")
ftpScript = None
taskFile = lockFile + ".tasks"
levsFile = processDirectory + "netcdf/levs.nc"
constFieldsPath = processDirectory + "netcdf/"
transferFile = processDirectory + "scripts/transfer.qsub"
profileDir = processDirectory + "profiles/"

#---------------------------------------------------------------
# objects that need config information to construct
#---------------------------------------------------------------
dasTasks = GmiDasTasks (realTime)


print "done initializing task info"

print "Checking for lock file. Will exit quietly not to override existing process..."
#----------------------------------------------------------------
# Check for the existence of the processing file in the directory
# If processing, just quit quietly, otherwise create the file
#----------------------------------------------------------------
if os.path.exists (lockFile):
    comUtilities.justQuit ()

gmiAutomationObject.touchNewFile (lockFile)

if not os.path.exists (logFile):
    ioRoutines.errorAndQuit ("The log file: " + logFile + " does not exist!", \
                             autoConstants.ERROR_SUBJECT, \
                             mailTo, lockFile)

print "Constructing each task in memory and on disk..."

#----------------------------------------------------------------
# Construct the tasks in memory and on disk
#----------------------------------------------------------------
try:
   gmiAutomationObject.createTasks (configOptions, processDirectory, mailTo, \
                                    realTime, taskFile)
except:
   ioRoutines.errorAndQuit ("Creating the MERRA regridding tasks failed " \
                            + "due to the exception:" + \
                            str (sys.exc_info ()), autoConstants.ERROR_SUBJECT, \
                            mailTo, lockFile)

print "Setting up fetching information..."

if sourcePath == 'ftp':
    ftpScript = processDirectory + "scripts/" + gmiAutomationObject.tasks[0].filePrefix + ".bash"

newTasks = []
for task in gmiAutomationObject.tasks:
    # check the log 
    theEntry = task.filePrefix + \
                task.year + \
                task.month + \
                task.day
    if logUtility.checkLogForEntry (theEntry, logFile) != True:
                
                task.archType = archType
                newTasks.append (task)

    else:
        dasTasks.updateConfigFile (task, configFile)
        ioRoutines.printMessage (autoConstants.STD_OUT, "Found the entry: " \
                                 + theEntry + " in the log file.  Not processing")
        gmiAutomationObject.removeTaskFromFile (taskFile, task)




    

gmiAutomationObject.tasks = []
gmiAutomationObject.tasks = newTasks

if len (gmiAutomationObject.tasks) == 0:
    ioRoutines.printMessage (autoConstants.STD_OUT, "No tasks to process. " \
                             + " Will exit quietly.")
    #----------------------------------------------------------------
    # END:: Exit and remove lock file
    #----------------------------------------------------------------
    comUtilities.releaseLockFileAndQuit (lockFile)



print " About to process tasks..."

#----------------------------------------------------------------
# START:: Process each task
#----------------------------------------------------------------
threadCount = 0
taskCount = 0
exitMutexes = []

for task in gmiAutomationObject.tasks:
     dasTasks.removeInterFiles (task, dasObjects)

# Prestage the first task before starting the task loop
entry = gmiAutomationObject.tasks[0].filePrefix + gmiAutomationObject.tasks[0].year \
          + gmiAutomationObject.tasks[0].month + gmiAutomationObject.tasks[0].day

print "Prestaging the first task: ", gmiAutomationObject.tasks[0].year, \
      gmiAutomationObject.tasks[0].month, gmiAutomationObject.tasks[0].day


try:
    dasTasks.preStageAllDas (gmiAutomationObject.tasks[0], \
                                 dasObjects, archiveSystem, transferFile, \
                                 processDirectory, thread.allocate_lock(), mailTo, \
                                 ftpScript)
except:
    ioRoutines.errorAndQuit ("Aborting MERRA regridding after inital pre staging " \
                                 + "due to the exception:" \
                                 + str (sys.exc_info ()), autoConstants.ERROR_SUBJECT, \
                                 mailTo, lockFile)

print "Entering task loop..."

try:
    for task in gmiAutomationObject.tasks:

        # Prestage the next task in a seperate thread and process the current task
        if taskCount != len(gmiAutomationObject.tasks)-1:
            
            print "Advance prestaging: ", gmiAutomationObject.tasks[taskCount+1].year, \
                  gmiAutomationObject.tasks[taskCount+1].month, \
                  gmiAutomationObject.tasks[taskCount+1].day
            exitMutexes.append (thread.allocate_lock())
            thread.start_new (dasTasks.preStageAllDas, (gmiAutomationObject.tasks[taskCount+1], \
                                                        dasObjects, \
                                                        archiveSystem, transferFile, \
                                                        processDirectory, \
                                                        exitMutexes[threadCount], \
                                                        mailTo, ftpScript))
        threadCount = threadCount + 1
            
        profileTaskFile = profileDir + "task." + entry + ".profile"
        if os.path.exists (profileTaskFile): os.remove (profileTaskFile)
        if preStageOnly != "true":        
            print "Processing: ", entry
            #cProfile.run('dasTasks.processTask (task, dasObjects, task.destinationPath, \
            #                      processDirectory, archiveDirectory, archiveSystem, \
            #                      levsFile, constFieldsPath, mailTo, logFile, \
            #                      configFile, taskFile, \
            #                      transferFile)', profileTaskFile)
            returnCode = dasTasks.processTask (task, dasObjects, task.destinationPath, \
                                                   processDirectory, archiveDirectory, archiveSystem, \
                                                   levsFile, constFieldsPath, mailTo, logFile, \
                                                   configFile, taskFile, \
                                                   transferFile)
        else:
            print "Running in PRESTAGE_ONLY mode.  Will not do any processing."
            


        print "Waiting for pre-staging thread"
        #----------------------------------------------------------------
        #  Wait for the thread that is pre-staging before continuing
        #----------------------------------------------------------------
        for mutex in exitMutexes:
            while not mutex.locked (): 
                pass

        #print "Profiling for: ", entry, " (also in: ", profileTaskFile
        #p = pstats.Stats (profileTaskFile)
        #p.sort_stats('name')
        #p.print_stats()

        print "done with task: ", taskCount

        taskCount = taskCount + 1
        if taskCount <= len(gmiAutomationObject.tasks)-1:
            entry = gmiAutomationObject.tasks[taskCount].filePrefix + \
                    gmiAutomationObject.tasks[taskCount].year \
                    + gmiAutomationObject.tasks[taskCount].month + \
                    gmiAutomationObject.tasks[taskCount].day

           
except:
    ioRoutines.errorAndQuit ("Aborting MERRA regridding due to the exception:" \
                             + str (sys.exc_info ()), autoConstants.ERROR_SUBJECT, \
                             mailTo, lockFile)
    

        
           
#----------------------------------------------------------------
# END:: Exit and remove lock file
#----------------------------------------------------------------
comUtilities.releaseLockFileAndQuit (lockFile)
