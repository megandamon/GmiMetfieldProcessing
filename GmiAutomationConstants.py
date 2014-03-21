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
   NCKSPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCAPPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCATTEDPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   HDFDUMPPATH = ""
   NCDUMPPATH = ""
   NCDUMPPATHSAFE = "/usr/local/other/SLES11.1/hdf4/4.2.6/intel-12.1.0.233/bin/"
   HDFGENPATH = ""
   NCECATPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCPDQPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCWAPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCGENPATH = "/usr/local/other/SLES11.1/netcdf/4.3.0/intel-13.0.1.117/bin/"
   NCRENAMEPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCEAPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCRCATPATH = "/usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/"
   NCREGRIDPATH = ""
   NCLCONVERTPATH  = "/usr/local/other/SLES11.1/ncl/6.1.2/intel-12.1.0.233/bin/"
   NCLPATH = "/usr/local/other/SLES11.1/ncl/6.1.2/intel-12.1.0.233/bin/"
   MAILPATH = "/usr/bin/"
   DATEPATH = "/bin/"
   SEDPATH = "/usr/bin/"
   REMAPEXEC = "/home/mrdamon/bin/GFIO_remap.x"
   H5TOH4 = "/usr/local/other/h4h5tools/2.1_GNU/bin/h5toh4"
   HREPACK = "/home/byin/fszip/hdf4-szipf-2006-02-09/NewHDF/bin/hrepack"
   SSHPATH = "/usr/bin/"
   SCPPATH = "/usr/bin/"
   DMGETPATH = "/usr/bin/"
   QSUBPATH = "/usr/slurm/bin/"
   QSTATPATH = "/usr/slurm/bin/"
   BASHPATH = "/usr/local/bin/bash"
   PODSPATH = "/usr/local/other/pods/"
   OPTEXEC = "/discover/nobackup/mrdamon/Devel/GmiAutomation/GEOS5DAS-discover/GEOS_Chem/Code/OptDepth.x"

   # other
   TMPPATH = "/tmp/" 
   FINAL_TMPPATH = "/home/mrdamon/ProcessMerra300/tmp/"
   ENVFILE = "/home/mrdamon/GmiMetfieldProcessing/loadEnv"

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
