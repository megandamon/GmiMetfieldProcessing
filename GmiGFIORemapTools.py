#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         November 6th 2007
#
# DESCRIPTION:
# Provides access to GFIO_remap functionality and does error checking.
#------------------------------------------------------------------------------

import os
import sys
import datetime
import re

from GmiAutomationConstants import GmiAutomationConstants

class GmiGFIORemapTools:

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   def __init__(self):
      GmiGFIORemapTools.constants = GmiAutomationConstants ()
      pass
 
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
   # Calls hrepack to uncompress hdf files
   #---------------------------------------------------------------------------    
   def uncompressFile (self, inputFile, outputFile, executable, exitMutex):

      print "inputFile: ", inputFile
      print "executable: ", executable
      
      if not os.path.exists (inputFile):
         raise self.constants.INVALIDINPUT
      
      if len (outputFile) <= 0:
         raise self.constants.INVALIDINPUT
      
      if not os.path.exists (executable):
         raise self.constants.INVALIDINPUT

      cmd = executable + " -i " + inputFile + " -o " + \
            outputFile + " -t \*:NONE"

      os.system (cmd)

      print "uncompressFile ACQURING MUTEX"
      exitMutex.acquire ()
      
      
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Will check for valid input and then call the GFIO_remap routine.
   #---------------------------------------------------------------------------    
   def gfioRemap (self, sourceFile, destinationFile, resolution, executable, \
                  bit, exitMutex):

      if not os.path.exists (sourceFile):
         raise self.constants.INVALIDINPUT
      
      if len (destinationFile) <= 0:
         raise self.constants.INVALIDINPUT
      
      if not os.path.exists (executable):
        print executable  
	raise self.constants.INVALIDINPUT

      if resolution == "4x5":
         shortRes = "a"
      elif resolution == "2x2.5":
         shortRes = "b"
      elif resolution == "1x1.25":
         shortRes = "c"
      elif resolution == "0.625x0.5":
         shortRes = "d"
      else:
         raise self.constants.INVALIDINPUT
      
      cmd = executable + " -res " + shortRes + " -prec " + bit + " -o " + \
            destinationFile + " " + sourceFile

      print "cmd: ", cmd
      returnCode = os.system (cmd)
      print "returnCode = ", returnCode

      print "gfioRemap ACQURING MUTEX"
      exitMutex.acquire ()

   def gfioRemap2 (self, args):

      sourceFile = args[0]
      destinationFile = args[1]
      resolution = args[2]
      executable = args[3]
      exitMutex = args[4]
      toExecute = args[5]

      print sourceFile
      
      if not os.path.exists (sourceFile):
         raise self.constants.INVALIDINPUT
      
      if len (destinationFile) <= 0:
         raise self.constants.INVALIDINPUT
      
      if not os.path.exists (executable):
         raise self.constants.INVALIDINPUT

      if resolution == "4x5":
         shortRes = "a"
      elif resolution == "2x2.5":
         shortRes = "b"
      elif resolution == "1x1.25":
         shortRes = "c"
      elif resolution == "0.625x0.5":
         shortRes = "d"
      else:
         raise self.constants.INVALIDINPUT
      
      cmd = executable + " -res " + shortRes + " -prec 64 -o " + \
            destinationFile + " " + sourceFile

      if toExecute == 'yes':
         os.system (cmd)

      print "gfioRemap2 ACQUIRING MUTEX"
      exitMutex.acquire ()
      
      return cmd
