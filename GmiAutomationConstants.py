#!/usr/bin/python

#------------------------------------------------------------------------------
# NASA/GSFC, Software Integration & Visualization Office, Code 610.3
#------------------------------------------------------------------------------
# AUTHORS:      Megan Damon
# AFFILIATION:  NASA GSFC / NGIT / TASC
# DATE:         November 7th 2006
#
# DESCRIPTION:
# This class contains constants for the Gmi Automation process.
#------------------------------------------------------------------------------

class GmiAutomationConstants:
   
   # TODO_make these static
   STD_OUT = 0
   STD_ERR = 1
   SYS_SUCCESS = 0
   Q_BADID = 1
   Q_UNKNOWNID = 153
   SSHERROR = "SSHERROR"
   ERROR = "ERROR"
   NOERROR = "NOERROR"
   NOSUCHFILE = "NOSUCHFILE"
   READERROR = "READERROR"
   WRITEERROR = "WRITEERROR"
   BADFILENAME = "BADFILENAME"
   INCORRECTNUMBEROFFILES = "INCORRECTNUMBEROFFILES"
   BADSYSTEMRETURNCODE = "BADSYSTEMRETURNCODE"
   INCOMPLETEFILE = "INCOMPLETEFILE"
   INVALIDINPUT = "INVALIDINPUT"
   INVALIDPATTERN = "INVALIDPATTERN"
   INVALIDFILENAMES = "INVALIDFILENAMES"
   INCOMPLETEDATA = "INCOMPLETEDATA"
   NOSUCHPATH = "NOSUCHPATH"
   FILEALREADYEXISTS = "FILEALREADYEXISTS"
   NOSUCHIDENTIFIER = "NOSUCHIDENTIFIER"
   ERROR_SUBJECT = "MERRA processing error!"
   WARNING_SUBJECT = "MERRA processing warning!"
   SUCCESS_MESSAGE = "MERRA Success!"
   VARIABLE_UNINIT = "Variable not initialized!"
      
   # binary paths
   GREPPATH = "/usr/bin/"
   AWKPATH = "/usr/bin/"
   MAIL_PATH = "/usr/bin/mail"
   SED_PATH = "/usr/bin/"
   MKDIRPATH = "/bin/"
   RMPATH = "/bin/"
   MVPATH = "/bin/"
   BBSCPPATH = "/usr/local/bin/"
   NCKSPATH = ""
   NCAPPATH = ""
   NCATTEDPATH = ""
   HDFDUMPPATH = ""
   NCDUMPPATH = ""
   NCDUMPPATHSAFE = ""
   HDFGENPATH = ""
   NCECATPATH = ""
   NCPDQPATH = ""
   NCWAPATH = ""
   NCGENPATH = ""
   NCRENAMEPATH = ""
   NCEAPATH = ""
   NCRCATPATH = ""
   NCREGRIDPATH = ""
   NCLCONVERTPATH  = ""
   NCLPATH = ""
   MAILPATH = "/usr/bin/"
   DATEPATH = "/bin/"
   SEDPATH = "/usr/bin/"
   REMAPEXEC = "/home/mrdamon/bin/GFIO_remap.x"
   H5TOH4 = "/home/mrdamon/bin/h5toh4"
#/usr/local/other/h4h5tools/2.1_GNU/bin/h5toh4"
   HREPACK = "/home/byin/fszip/hdf4-szipf-2006-02-09/NewHDF/bin/hrepack"
   SSHPATH = "/usr/bin/"
   SCPPATH = "/usr/bin/"
   DMGETPATH = "/usr/bin/"
   QSUBPATH = "/usr/slurm/bin/"
   QSTATPATH = "/usr/slurm/bin/"
   BASHPATH = "/usr/local/bin/bash"
   PODSPATH = "/usr/local/other/pods/"
   #OPTEXEC = "/discover/nobackup/mrdamon/Devel/GmiAutomation/GEOS5DAS-discover/GEOS_Chem/CodeNew/Main"
   OPTEXEC = "/discover/nobackup/mrdamon/Devel/GmiAutomation/GEOS5DAS-discover/GEOS_Chem/CodeNewRegridTau/Main"
   # other
   TMPPATH = "/tmp/" 
   FINAL_TMPPATH = "/home/mrdamon/ProcessMerra300/tmp/"
   ENVFILE = "/home/mrdamon/ProcessMerra300/loadEnv"

   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Constructor routine.
   #---------------------------------------------------------------------------  
   
   def __init__(self):
      pass
 
   #---------------------------------------------------------------------------  
   # AUTHORS: Megan Damon NASA GSFC / NGIT / TASC
   #
   # DESCRIPTION: 
   # Destructor routine.
   #---------------------------------------------------------------------------    
   
   def __del__(self):
      pass
