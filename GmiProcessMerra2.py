#!/usr/bin/python

#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / SSAI
# BEGIN DATE:         Feb 2 2016
#
# DESCRIPTION:
# This is the main driver for the regridding of the MERRA2 fields
#
# Execute this script by typing : python GmiProcessMerra2.py -i <config filename>
#    where <config filename> is the name of the input file which contains the
#    following information:
#
#    - RUN_DIR          : directory the python files are in
#    - LOCK_FILE        : File to lock out other processes from doing this job
#    - ARCHIVE_PATH     : Where to archive the proccessed fields
#    - DESTINATION_PATH : Where to hold the intermediate files while processing
#    - MAIL_TO          : A comma seperated list of email addresses to send
#                         updates to.
#    - REALTIME         : flags if mode is real-time
#    - ARCHIVE_SYSTEM   : name of archive system (where ARCHIVE_PATH is)
#    - ARCH_TYPE        : Can be either serial, shared_mem, or dist_mem (single cpu execution,
#                         shared memory archtitecture - will use threads, or distributed nodes)
#    - LOG_FILE         : File for keeping track of fields processed
#    - PRESTAGE_ONLY    : Set to true if you don't want any processing to occur.  Only prestaging will be done. 
#    - SOURCE_PATH      : path to the GEOS5DAS fields for input
#    - START_DATE       : first day 
#    - NUM_DAYS         : number of days to compute, including the start date
#                         should be 1 if real-time mode
#    - FILE_PREFIX      : prefix of the raw files
#                         
#
# Here is an example of the config file:

#RUN_DIR:/discover/nobackup/mrdamon/MERRA2
#LOCK_FILE:.IamMerra2LockJan
#ARCHIVE_PATH:/archive/anon/pub/gmidata2/input/metfields/geos5das-merra2/
#DESTINATION_PATH:/discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2
#MAIL_TO:megandamon@gmail.com
#REAL_TIME:false
#ARCHIVE_SYSTEM:discover
#ARCH_TYPE:dist_mem
#LOG_FILE:MERRA2.log
#PRESTAGE_ONLY:false
#SOURCE_PATH:/discover/nobackup/projects/gmao/merra2/data/products/d5124_m2_jan10/
#START_DATE:20160106
#NUM_DAYS:1
#FILE_PREFIX:MERRA2_400

#------------------------------------------------------------------------------

__author__ = 'Megan Damon'
__version__ = '0.0'
__date__ = '2016/02/02'

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
    print "usage: GmiProcessMerra2.py [-i]"
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
if processDirectory[-1:] != "/":
    processDirectory = processDirectory + "/"
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
   ioRoutines.errorAndQuit ("Creating the MERRA-2 regridding tasks failed " \
                            + "due to the exception:" + \
                            str (sys.exc_info ()), autoConstants.ERROR_SUBJECT, \
                            mailTo, lockFile)

print "Setting up fetching information..."

if sourcePath == 'ftp':
    ftpScript = processDirectory + "scripts/" + gmiAutomationObject.tasks[0].filePrefix + ".bash"

else: 
    print "non ftp staging sourcePath: ", sourcePath

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
#exitMutexes = []

for task in gmiAutomationObject.tasks:
     dasTasks.removeInterFiles (task, dasObjects)

print "Entering task loop..."

try:
    for task in gmiAutomationObject.tasks:

        # Prestage the first task before starting the task loop
        entry = task.filePrefix + task.year \
            + task.month + task.day

        print "Copying data for :  ", task.year, task.month, task.day

        task.sourcePath = sourcePath

        print "Source path: ", sourcePath
        print "Task source path: ", task.sourcePath

        try:
            dasTasks.copyMerra2DataToWorkingDir (task, dasObjects, \
                                                     archiveSystem, transferFile, \
                                                     processDirectory, \
                                                     thread.allocate_lock(), mailTo, \
                                                     ftpScript)
        except:
            ioRoutines.errorAndQuit ("Aborting MERRA2 regridding after copying failed " \
                                         + "due to the exception:" \
                                         + str (sys.exc_info ()), autoConstants.ERROR_SUBJECT, \
                                         mailTo, lockFile)


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
        #for mutex in exitMutexes:
        #    while not mutex.locked (): 
        #        pass

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
    ioRoutines.errorAndQuit ("Aborting MERRA2 regridding due to the exception:" \
                             + str (sys.exc_info ()), autoConstants.ERROR_SUBJECT, \
                             mailTo, lockFile)
    

        
           
#----------------------------------------------------------------
# END:: Exit and remove lock file
#----------------------------------------------------------------
print "Releasing lock file: ", lockFile
comUtilities.releaseLockFileAndQuit (lockFile)

