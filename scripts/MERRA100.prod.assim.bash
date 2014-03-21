#!/usr/local/bin/bash



export year=$1
export month=$2
export day=$3
export transferDir=$4

export date=$year$month$day
export user="anonymous"
export passwd="megan.r.damon@nasa.gov"

echo $transferDir
cd $transferDir

ftp -n goldsmr1.sci.gsfc.nasa.gov <<END_SCRIPT
user $user $passwd

cd /data/s4pa/MERRA/MAT3FXCHM.5.2.0/$year/$month
get MERRA100.prod.assim.tavg3_2d_chm_Fx.$date.hdf

cd /data/s4pa/MERRA/MAI3NECHM.5.2.0/$year/$month
get MERRA100.prod.assim.inst3_3d_chm_Ne.$date.hdf

cd /data/s4pa/MERRA/MAT3FECHM.5.2.0/$year/$month
get MERRA100.prod.assim.tavg3_3d_chm_Fe.$date.hdf

cd /data/s4pa/MERRA/MAT3FVCHM.5.2.0/$year/$month
get MERRA100.prod.assim.tavg3_3d_chm_Fv.$date.hdf

cd /data/s4pa/MERRA/MAT3NECHM.5.2.0/$year/$month
get MERRA100.prod.assim.tavg3_3d_chm_Ne.$date.hdf

bye
END_SCRIPT


ftp -n goldsmr2.sci.gsfc.nasa.gov <<END_SCRIPT
user $user $passwd

cd /data/s4pa/MERRA/MAT1NXRAD.5.2.0/$year/$month
get MERRA101.prod.assim.tavg1_2d_rad_Nx.$date.hdf

cd /data/s4pa/MERRA/MAT1NXFLX.5.2.0/$year/$month
get MERRA100.prod.assim.tavg1_2d_flx_Nx.$date.hdf

cd /data/s4pa/MERRA/MAT1NXLND.5.2.0/$year/$month
get MERRA100.prod.assim.tavg1_2d_lnd_Nx.$date.hdf

cd /data/s4pa/MERRA/MAT1NXSLV.5.2.0/$year/$month
get MERRA100.prod.assim.tavg1_2d_slv_Nx.$date.hdf

bye
END_SCRIPT
mv MERRA101.prod.assim.tavg1_2d_rad_Nx.$date.hdf MERRA100.prod.assim.tavg1_2d_rad_Nx.$date.hdf


ftp -n goldsmr3.sci.gsfc.nasa.gov <<END_SCRIPT
user $user $passwd
cd /data/s4pa/MERRA/MAI6NVANA.5.2.0/$year/$month
get MERRA100.prod.assim.inst6_3d_ana_Nv.$date.hdf

bye
END_SCRIPT

exit 0
