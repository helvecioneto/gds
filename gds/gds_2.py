# -*- coding: utf-8 -*-
from config import START,END,INTERVAL,INIT_H,END_H,SATELLITE,PRODUCT,CHANNEL,PATH
import s3fs
import pandas as pd
import numpy as np
import requests
from datetime import *
import pathlib

import netCDF4
import os

def file_list():
    days = pd.date_range(start=START, end=END, freq=INTERVAL)
    hours = pd.date_range(INIT_H,END_H, freq=INTERVAL).strftime('%H:%M:%S')

    data_range = []
    for d in range(len(days)):
        for m in range(len(hours)):
            if days[d].strftime('%H:%M:%S') == hours[m]:
                data_range.append(days[d])
    return data_range

def aws_file_list(list_of_files):

    DownloadList = pd.DataFrame()  
    timestamp = []
    fileslist = []
    stringList = []
    
    for i in list_of_files:
        ListFiles = np.array(aws.ls(server+i.strftime('%Y/%j/%H')))
        stringList.append(i.strftime('%Y%j%H%M'))
        timestamp.append(i.strftime('%Y/%m/%d %H:%M'))
        
        for file in ListFiles: 
            if file.find('M3C'+str(CHANNEL)) >= 1:
                fileslist.append(str(file))
                
            elif file.find('M6C'+str(CHANNEL)) >= 1:
                fileslist.append(str(file))
                
                
    list_ = pd.DataFrame({'url':fileslist})
    list_ = list_.drop_duplicates()

    regstr = '|'.join(stringList)
    list_['strings'] = list_['url'].str.upper().str.contains(regstr)

    DownloadList = list_[list_.strings]
    DownloadList = DownloadList.drop(['strings'], axis=1)
    DownloadList['timestamp'] = timestamp
    DownloadList['path'] = DownloadList['url'].str.replace(SATELLITE, PATH)
    DownloadList['url'] = DownloadList['url'].str.replace(SATELLITE, 'https://'+SATELLITE+'.s3.amazonaws.com')
    DownloadList['file'] = DownloadList.apply(lambda x: pathlib.Path(x.path).name, axis=1)
    DownloadList['path'] = DownloadList.apply(lambda x: pathlib.Path(x.path).parent, axis=1)

    return DownloadList

def download_files(list_of_files):

    fils = len(list_of_files)
    cnt = 0
    
    for i,row in list_of_files.iterrows():
        print('File Counter',cnt+1,'/',fils)
        print('Downloading...')

        StartTime = datetime.now()
        req = requests.get(row.url, stream = True)
        total_size = int(req.headers['content-length'])
        size = 0
        
        pathlib.Path(row.path).mkdir(parents=True, exist_ok=True)

        with open(str(row.path)+'/'+str(row.file),'wb') as output:
            for chunk in req.iter_content(chunk_size=1024):
                if chunk:
                    rec_size = output.write(chunk)
                    size = rec_size + size
                    print('{}\t{:3.0f}%\t{:.2f} min'.format(row.file,100.0*size/total_size, (datetime.now()-StartTime).seconds/60.0), end='\r', flush=True)
        print('\n')
        
        ## Process
        print('Processing...')
        path_ = (str(row.path)+'/'+str(row.file))
        open_netcdf(path_,row.file)
        cnt +=1

def open_netcdf(path_,file):
    ## Copy global atributes
    dataset = netCDF4.Dataset(path_, 'a')
    scale_factor = dataset.variables['CMI'].scale_factor
    long_name = dataset.variables['CMI'].long_name
    ## Atributes
    naming_authority = dataset.naming_authority
    
    
    ### Translate
    print('Translating file...')
    cmd1 = "gdal_translate -q -a_srs"
    cmd2 = " \"+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023 +sweep=x\" "
    cmd3 = "-a_scale "+str(scale_factor)+" -a_ullr -5434390.3880000000000 5434390.3880000000000 5434390.3880000000000 -5434390.3880000000000 HDF5:"
    cmd4 = "\"./"+str(path_)+"\""
    cmd5 = "://CMI -a_nodata -1 -of netCDF temp/navigation.modified.nc"
    full_cmd = cmd1+cmd2+cmd3+cmd4+cmd5

    os.environ['HDF5_USE_FILE_LOCKING']='FALSE'
    os.system(full_cmd)
    
    ## Warp file
    print('Warping file...')
    cmd1_ = "gdalwarp -q -multi "
    cmd2_ = "-s_srs \"+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023\" "
    cmd3_ = "-te -75.0 -35.0 -33.0 7.0 -t_srs '+proj=latlong +datum=WGS84\' "
    cmd4_ = "./temp/navigation.modified.nc "
    cmd5_ = "-of netCDF -co compress=DEFLATE -co FORMAT=NC4C -r near "
    cmd6_ = "temp/"+file
    
    full_cmd_ = (cmd1_+cmd2_+cmd3_+cmd4_+cmd5_+cmd6_)

    os.system(full_cmd_)
    ## Rename Band 1 to CMI
    os.system("ncrename -h -O -v Band1,CMI temp/"+str(file))
    ## Rename longname
    os.system("ncatted -O -a long_name,CMI,o,c,\""+str(long_name)+"\" temp/"+str(file))

    ## Add Global atributes 
    os.system("ncatted -O -h -a naming_authority,global,o,c,"+str(naming_authority)+" temp/"+str(file))
    
    ## Remove navigation file
    os.system("rm -rf temp/navigation.modified.nc")
    dataset.close()

list_of_files = file_list()
server = SATELLITE+'/'+PRODUCT+'/'    
aws = s3fs.S3FileSystem(anon=True)

lista = aws_file_list(list_of_files)
download_files(lista)