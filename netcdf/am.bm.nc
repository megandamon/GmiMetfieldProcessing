CDF       
      lev    H   lon    �         history      +�Wed Jul 15 19:45:45 2020: ncks --append netcdf/lon.2x2.5.nc netcdf/am.bm.nc
Wed Jul 15 18:27:26 2020: ncks -v am,bm /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/WORKFLOW-discover/Hindcast-May-Sles12Merra-Test/MERRA2_400.20200511.1x1.25.nc netcdf/am.bm.nc
Thu May 14 15:55:48 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append netcdf/lon.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.20200501.1x1.25.nc
Thu May 14 15:50:49 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncrename -v TROPPB,TROPP -v TS,TSKIN -v SWGNT,SWGNET -v PARDRLAND,PARDR -v PARDFLAND,PARDF -v lon,lon_old /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.20200501.1x1.25.nc
Thu May 14 14:59:06 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.20200501.1D.1x1.25.nc
Thu May 14 14:54:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.inst1_2d_asm_Nx.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg3d.20200501.1x1.25.nc
Thu May 14 14:54:32 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.inst1_2d_asm_Nx.20200501.1x1.25.nc
Thu May 14 14:31:47 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncwa -O -a record /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc.ncwa.nc
Thu May 14 14:31:45 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncpdq -U -O -a time,record /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc.ncpdq.nc
Thu May 14 14:31:45 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncecat -O /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc.ncecat.nc
Thu May 14 14:31:43 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks -v QV10M,SLP,T10M,TROPPB,TROPQ,TROPT,U2M,U50M,V2M,V50M,QV2M,T2M,TS,U10M,V10M,RHOA,BSTAR,FRSEAICE,USTAR,Z0M,PBLH,PRECANV,PRECCON,PRECLSC,PRECSNO,EMIS,ALBEDO,ALBNIRDF,ALBNIRDR,ALBVISDF,ALBVISDR,CLDTOT,SWGNT,GRN,LAI,SNODP,GWETROOT,GWETTOP,PARDFLAND,PARDRLAND /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg2d.20200501.1x1.25.nc.extract.tmp
Thu May 14 14:31:43 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_rad_Nx.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_lnd_Nx.20200501.1x1.25.nc
Thu May 14 14:31:42 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_flx_Nx.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_rad_Nx.20200501.1x1.25.nc
Thu May 14 14:31:42 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_flx_Nx.20200501.1x1.25.nc
Thu May 14 14:31:41 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks -d time,0,23,3 /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.3hr.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.V2M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.V50M.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.SLP.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.V2M.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.T10M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.SLP.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.U50M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.T10M.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.QV10M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.U50M.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.V10M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.QV10M.nc
Thu May 14 14:31:37 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TS.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.V10M.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.U2M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TS.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.T2M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.U2M.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TROPPB.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.T2M.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.QV2M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TROPPB.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TROPT.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.QV2M.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TROPQ.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TROPT.nc
Thu May 14 14:31:36 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks --append /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.U10M.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.TROPQ.nc
Thu May 14 14:31:34 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncrename -d fakeDim102,time -d fakeDim103,lat -d fakeDim104,lon /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.U10M.nc
Thu May 14 14:31:34 2020: /usr/local/other/SLES11.1/nco/4.2.3/intel-12.1.0.233/bin/ncks -v U10M /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc /discover/nobackup/projects/gmi/gmidata2/users/mrdamon/metfield-processing/merra2//2020/05/MERRA2_400.tavg1_2d_slv_Nx.20200501.1x1.25.nc.extract.tmp     NCO       `netCDF Operators version 4.8.1 (Homepage = http://nco.sf.net, Code = http://github.com/nco/nco)    nco_openmp_thread_number            Met_Data_Name         GMAO_GEOS5MERRA2_2x2%5x72      Conventions       COARDS     Source        Unknown    Title         Unknown    Contact       Unknown    History      'Mon Mar  7 12:43:00 2016: ncks -v lon MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc lon.2x2.5.nc
Wed Sep 15 18:12:59 2010: ncrename -v time,time1 /home/mrdamon/ProcessMerra300/MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:29:03 2010: ncks -v FRLAKE,FRLAND,FRLANDICE,FROCEAN MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc 2x2.5.nc
Mon Sep 13 18:23:40 2010: ncrename -v am,am3 -v bm,bm3 MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:17:25 2010: ncrename -v am,am_2 -v bm,bm_2 MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:16:41 2010: ncrename -v lev72,lev MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:14:29 2010: ncrename -v lev,lev72 MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:00:35 2010: ncks -v lev /discover/nobackup/mrdamon/Devel/GmiAutomation/GEOS5DAS-discover/levs.nc --append MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 17:56:26 2010: ncks -v FRLAKE,FRLAND,FRLANDICE,FROCEAN MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc reduced.2x2.5.nc
Mon Sep 13 17:55:23 2010: ncrename -d record,time MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 17:53:52 2010: ncwa -a time test.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.removed.nc
Mon Sep 13 17:53:05 2010: ncecat MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.1.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.2.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.4.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.3.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.6.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.5.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.7.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.8.nc test.nc
File written by GFIO v1.0.8    ptop             pint             ks        ��     ak               bk               history_of_appended_files        �Wed Jul 15 19:45:45 2020: Appended file netcdf/lon.2x2.5.nc had following "History" attribute:
Mon Mar  7 12:43:00 2016: ncks -v lon MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc lon.2x2.5.nc
Wed Sep 15 18:12:59 2010: ncrename -v time,time1 /home/mrdamon/ProcessMerra300/MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:29:03 2010: ncks -v FRLAKE,FRLAND,FRLANDICE,FROCEAN MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc 2x2.5.nc
Mon Sep 13 18:23:40 2010: ncrename -v am,am3 -v bm,bm3 MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:17:25 2010: ncrename -v am,am_2 -v bm,bm_2 MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:16:41 2010: ncrename -v lev72,lev MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:14:29 2010: ncrename -v lev,lev72 MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 18:00:35 2010: ncks -v lev /discover/nobackup/mrdamon/Devel/GmiAutomation/GEOS5DAS-discover/levs.nc --append MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 17:56:26 2010: ncks -v FRLAKE,FRLAND,FRLANDICE,FROCEAN MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc reduced.2x2.5.nc
Mon Sep 13 17:55:23 2010: ncrename -d record,time MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.nc
Mon Sep 13 17:53:52 2010: ncwa -a time test.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.removed.nc
Mon Sep 13 17:53:05 2010: ncecat MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.1.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.2.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.4.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.3.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.6.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.5.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.7.nc MERRA300.prod.assim.const_2d_asm_Nx.00000000.2x2.5.8.nc test.nc
File written by GFIO v1.0.8
          am                             >    bm                             ?    lev                 CLASS         DIMENSION_SCALE    NAME      lev    	long_name         vertical level     units         layer      positive      down     @      @@   lon                	long_name         	longitude      units         degrees_east     @      B�<u<���=$lv=h�A=��=��>�
>=�D>z�>���>�A�?��?4��?hC�?�z:?��?��@��@<�F@kK�@��]@�7�@�b�AM`A$��AG\�Ap�A���A��A�d�A��B��B/��BPHBu��B��B���B�+B� CqC#�yC=�CQmC\�/C`!�C]VFCWv�CO�9CF-�C;[�C/�/C"�C��C͑B��mB�#�BЋ�B�wB��(B�t�B��Bt��B[�BBB(�ZB	AꦵA�� A���A�@T��<���                                                                                                                                                                    ;e`B<�\)=;�6=��">
#:>6�F>d�K>���>�F�>�~(>�+k>�4n?<6?K^?0�?"e?+o?4�?<<�?B�\?H  ?Mw2?R�?Xh�?]�?c]�?h��?n_�?s��?ye,?~{?�      @       @      @      @      @      @      @       @"      @$      @&      @(      @*      @,      @.      @0      @1      @2      @3      @4      @5      @6      @7      @8      @9      @:      @;      @<      @=      @>      @?      @@      @@�     @A      @A�     @B      @B�     @C      @C�     @D      @D�     @E      @E�     @F      @F�     @G      @G�     @H      @H�     @I      @I�     @J      @J�     @K      @K�     @L      @L�     @M      @M�     @N      @N�     @O      @O�     @P      @P@     @P�     @P�     @Q      @Q@     @Q�     @Q�     @R          @   @�  @�  A   AH  Ap  A�  A�  A�  A�  A�  A�  B  B  B  B   B*  B4  B>  BH  BR  B\  Bf  Bp  Bz  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  B�  C  C� C  C	� C  C� C  C� C  C� C  C� C   C"� C%  C'� C*  C,� C/  C1� C4  C6� C9  C;� C>  C@� CC  CE� CH  CJ� CM  CO� CR  CT� CW  CY� C\  C^� Ca  Cc� Cf  Ch� Ck  Cm� Cp  Cr� Cu  Cw� Cz  C|� C  C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� C�  C�@ C�� C�� 