# -*- coding: utf-8 -*-

from osgeo import gdal

ifile = "OR_ABI-L2-CMIPF-M3C04_G16_s20190010745366_e20190010756133_c20190010756196.nc"

ds = gdal.Open(ifile)
ds = gdal.Translate(ds)


#/usr/bin/gdal_translate \
#-a_srs "+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023 +sweep=x" \
#-a_ullr -5434390.3880000000000 5434390.3880000000000 5434390.3880000000000 -5434390.3880000000000 \
#-outsize 5424 5424 -unscale \
#HDF5:"OR_ABI-L2-CMIPF-M6C14_G16_s20201051610166_e20201051619474_c20201051619566.nc"://CMI \
#-of netCDF OR_ABI-L2-CMIPF-M6C14_G16_s20201051610166_e20201051619474_c20201051619566_nav.nc
#
#E o segundo é o gdalwarp que eu utilizo para reprojetar e interpolar o dado e nesse mesmo comando eu já recorto a área de interesse:
#
#/usr/bin/gdalwarp \
#-s_srs "+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023 +sweep=x" \
#-te -116.0 -56.0 -25.0 35.0 \
#-t_srs '+proj=latlong +datum=WGS84' \
#OR_ABI-L2-CMIPF-M6C14_G16_s20201051610166_e20201051619474_c20201051619566_nav.nc \
#-of netCDF -co compress=DEFLATE -co FORMAT=NC4C OR_ABI-L2-CMIPF-M6C14_G16_s20201051610166_e20201051619474_c20201051619566_ret.nc

/usr/bin/gdal_translate -q -a_srs "+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023 +sweep=x" -a_ullr -5434390.3880000000000 5434390.3880000000000 5434390.3880000000000 -5434390.3880000000000 HDF5:"OR_ABI-L2-CMIPF-M3C02_G16_s20190431900312_e20190431911079_c20190431911150.nc"://CMI -a_nodata -1 -of netCDF navigation.modified.nc


/usr/bin/gdalwarp -q -multi -s_srs "+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023" -te -75.0 -35.0 -33.0 7.0 -t_srs '+proj=latlong +datum=WGS84' navigation.modified.nc -of netCDF -co compress=DEFLATE -co FORMAT=NC4C -r ${method} "cmi.modified.${method}.nc"
