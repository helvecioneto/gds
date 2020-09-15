# -*- coding: utf-8 -*-
from config import START,END,INTERVAL,INIT_H,END_H,SATELLITE,PRODUCT,CHANNEL,TMP,OUTPUT,BBOX,PROJ,INTERP,DQC_THRESHOLD
import s3fs
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import pathlib
import errno
import os
import netCDF4
import logging
import warnings
warnings.filterwarnings("ignore")

server = SATELLITE+'/'+PRODUCT+'/'
aws = s3fs.S3FileSystem(anon=True)

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
    timestamp = []
    fileslist = []
    stringList = []
    logging.basicConfig(filename='missing.txt', filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    
    for i in list_of_files:
        os.system('cls' if os.name == 'nt' else 'clear')
        print('GOES-DOWNLOADER-SLICER')
        print('Mount list of files')
        print('Server :'+str(server)+ 'CHANNEL' + str(CHANNEL) + ' Date Time:  '+str(i))
        try:
            download_list = np.array(aws.ls(server+i.strftime('%Y/%j/%H')))
            for file in download_list:
                    if file.find('M3C'+str(CHANNEL)) >= 1:
                        fileslist.append(str(file))
                        stringList.append(i.strftime('%Y%j%H%M'))
                        timestamp.append(i.strftime('%Y/%m/%d %H:%M'))
                    if file.find('M6C'+str(CHANNEL)) >= 1:
                        fileslist.append(str(file))
                        stringList.append(i.strftime('%Y%j%H%M'))
                        timestamp.append(i.strftime('%Y/%m/%d %H:%M'))
                    else:
                        pass
        except:
            logging.warning('FILE NOT FOUND! ->  Server :'+str(server)+ 'CHANNEL' + str(CHANNEL) + ' Date Time:  '+str(i))

    list_ = pd.DataFrame({'url':fileslist,'timestamp':timestamp})
    list_['strings'] = list_['url'].str.upper().str.contains('|'.join(stringList))
    list_ = list_.drop_duplicates()
    list_ = list_[list_['strings'] == True]
    list_['path'] = list_['url'].str.replace(SATELLITE, TMP)
    list_['url'] = list_['url'].str.replace(SATELLITE, 'https://'+SATELLITE+'.s3.amazonaws.com')
    list_['file'] = list_.apply(lambda x: pathlib.Path(x.path).name, axis=1)
    list_['path'] = list_.apply(lambda x: pathlib.Path(x.path).parent, axis=1)
    
    return list_

def download_files(list_of_files):

    fils = len(list_of_files)
    cnt = 0
    logging.basicConfig(filename='warning_logs.txt', filemode='w', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
    for i,row in list_of_files.iterrows():
        print('File Counter',cnt+1,'/',fils)
        dat_time = datetime.strptime(row.timestamp, '%Y/%m/%d %H:%M')
        path = str(dat_time.year)+'/'+str(dat_time.strftime('%m'))+'/'+str(dat_time.strftime('%d'))
        output = OUTPUT+'/'+path+'/'
        print('Downloading '+str(row.file)+'...')

        try:
            StartTime = datetime.now()
            req = requests.get(row.url, stream = True)
            total_size = int(req.headers['content-length'])
            size = 0

            pathlib.Path(row.path).mkdir(parents=True, exist_ok=True)
            
            print('GOES-DOWNLOADER-SLICER')
            print('File Counter',cnt+1,'/',fils)
            print('Downloading '+str(row.file)+'...')

            with open(str(row.path)+'/'+str(row.file),'wb') as output_:
                for chunk in req.iter_content(chunk_size=1024):
                    if chunk:
                        rec_size = output_.write(chunk)
                        size = rec_size + size

#                        print('Date ->  ' +str(row.timestamp) + '   File->   {}\t{:3.0f}%\t{:.2f} min'.format(row.file,100.0*size/total_size, (datetime.now()-StartTime).seconds/60.0), end='\r', flush=True)
#                        os.system('cls' if os.name == 'nt' else 'clear')
        except:
            logging.warning('Error in Download -> Date Time: '+str(row.timestamp)+ ' -> File: '+str(row.file)+'')
            cnt +=1
        try:
            print('Filtering CMI based on quality control flag...')
            os.system("./dqcfilter " + str(row.path) + '/' + str(row.file) + " " + str(DQC_THRESHOLD))
        except:
            logging.warning('Error filtering -> Date Time: '+str(row.timestamp)+ ' -> File: '+str(row.file)+'')
        try:
        	 ## Process
	        print('Processing...')
	        path_ = (str(row.path)+'/'+str(row.file))
	        open_netcdf(path_,row.file,output)
	        cnt +=1
        except:
            	logging.warning('Error to proccess -> Date Time: '+str(row.timestamp)+ ' -> File: '+str(row.file)+'')
            	cnt +=1

def open_netcdf(path_,file,output):

    path_outt = output
    if not os.path.exists(os.path.dirname(str(path_outt))):
            try:
                os.makedirs(os.path.dirname(str(path_outt)))
            except OSError as exc: # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

    output = str(output)+str(file)

    ## Copy global atributes
    dataset = netCDF4.Dataset(path_, 'a')
    scale_factor = dataset.variables['CMI'].scale_factor
    long_name = dataset.variables['CMI'].long_name
    standard_name = dataset.variables['CMI'].standard_name
    sensor_band_bit_depth = dataset.variables['CMI'].sensor_band_bit_depth
    valid_range = dataset.variables['CMI'].valid_range
    add_offset = dataset.variables['CMI'].add_offset
    units = dataset.variables['CMI'].units
    resolution = dataset.variables['CMI'].resolution
    grid_mapping = dataset.variables['CMI'].grid_mapping
    cell_methods = dataset.variables['CMI'].cell_methods
    ## Atributes
    naming_authority = dataset.naming_authority
    inst_ = dataset.institution
    project = dataset.project
    production_site = dataset.production_site
    production_environment = dataset.production_environment
    spatial_resolution = dataset.spatial_resolution
    orbital_slot = dataset.orbital_slot
    platform_ID = dataset.platform_ID
    instrument_type = dataset.instrument_type
    scene_id = dataset.scene_id
    instrument_ID = dataset.instrument_ID
    dataset_name = dataset.dataset_name
    title = dataset.title
    summary = dataset.summary
    keywords = dataset.keywords
    keywords_vocabulary = dataset.keywords_vocabulary
    license_ = dataset.license
    processing_level = dataset.processing_level
    date_created = dataset.date_created
    cdm_data_type = dataset.cdm_data_type
    time_coverage_start = dataset.time_coverage_start
    time_coverage_end = dataset.time_coverage_end
    timeline_id = dataset.timeline_id
    production_data_source = dataset.production_data_source
    id__ = dataset.id

    ### Translate
    print('Translating file...')
    cmd1 = "gdal_translate -q -a_srs"
    cmd2 = " \"+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023 +sweep=x\" "
    cmd3 = "-a_scale "+str(scale_factor)+" -a_ullr -5434390.3880000000000 5434390.3880000000000 5434390.3880000000000 -5434390.3880000000000 HDF5:"
    cmd4 = "\"./"+str(path_)+"\""
    cmd5 = "://CMI -a_nodata -1 -of netCDF tmp/navigation.modified.nc"
    full_cmd = cmd1+cmd2+cmd3+cmd4+cmd5
    os.environ['HDF5_USE_FILE_LOCKING']='FALSE'
    os.system(full_cmd)

    ## Warp file
    print('Warping file...')
    cmd1_ = "gdalwarp -q -multi "
    cmd2_ = "-s_srs \"+proj=geos +a=6.37814e+06  +b=6.35675e+06 +lon_0=-75 +f=298.257 +h=35786023\" "
    cmd3_ = "-nomd -te "+str(BBOX)+" -t_srs '+proj=latlong +datum="+str(PROJ)+"\' "
    cmd4_ = "./tmp/navigation.modified.nc "
    cmd5_ = "-of netCDF -co compress=DEFLATE -co FORMAT=NC4C -r "+str(INTERP)+" "
    cmd6_ = output

    full_cmd_ = (cmd1_+cmd2_+cmd3_+cmd4_+cmd5_+cmd6_)
    os.system(full_cmd_)

    ## Rename Band 1 to CMI
    os.system("ncrename -h -O -v Band1,CMI "+str(output))
    ## Rename longname
    os.system("ncatted -O -a long_name,CMI,o,c,\""+str(long_name)+"\" "+str(output))
    os.system("ncatted -O -a standard_name,CMI,o,c,\""+str(standard_name)+"\" "+str(output))
    os.system("ncatted -O -a scale_factor,CMI,o,f,\""+str(scale_factor)+"\" "+str(output))
    os.system("ncatted -O -a sensor_band_bit_depth,CMI,o,b,\""+str(sensor_band_bit_depth)+"\" "+str(output))
    os.system("ncatted -O -a valid_range,CMI,o,c,\""+str(valid_range)+"\" "+str(output))
    os.system("ncatted -O -a add_offset,CMI,o,f,\""+str(add_offset)+"\" "+str(output))
    os.system("ncatted -O -a units,CMI,o,c,\""+str(units)+"\" "+str(output))
    os.system("ncatted -O -a resolution,CMI,o,c,\""+str(resolution)+"\" "+str(output))
    os.system("ncatted -O -a grid_mapping,CMI,o,c,\""+str(grid_mapping)+"\" "+str(output))
    os.system("ncatted -O -a cell_methods,CMI,o,c,\""+str(cell_methods)+"\" "+str(output))

    logo_inpe = "INPE-LABREN: Laboratorio de Modelagem e Estudos de Recursos Renovaveis de Energia"

    ## Add Global atributes
    os.system("ncatted -O -h -a naming_authority,global,o,c,"+str(naming_authority)+" "+str(output))

    os.system("ncatted -O -h -a institution,global,o,c,\""+str(inst_)+"\" "+str(output))
    os.system("ncatted -O -h -a clipping,global,o,c,\""+str(logo_inpe)+"\" "+str(output))

    os.system("ncatted -O -h -a reprojection,global,o,c,WGS84 "+str(output))
    os.system("ncatted -O -h -a project,global,o,c,\""+str(project)+"\" "+str(output))
    os.system("ncatted -O -h -a production_site,global,o,c,\""+str(production_site)+"\" "+str(output))
    os.system("ncatted -O -h -a production_environment,global,o,c,\""+str(production_environment)+"\" "+str(output))
    os.system("ncatted -O -h -a spatial_resolution,global,o,c,\""+str(spatial_resolution)+"\" "+str(output))
    os.system("ncatted -O -h -a orbital_slot,global,o,c,\""+str(orbital_slot)+"\" "+str(output))
    os.system("ncatted -O -h -a platform_ID,global,o,c,\""+str(platform_ID)+"\" "+str(output))
    os.system("ncatted -O -h -a instrument_type,global,o,c,\""+str(instrument_type)+"\" "+str(output))
    os.system("ncatted -O -h -a scene_id,global,o,c,\""+str(scene_id)+"\" "+str(output))
    os.system("ncatted -O -h -a instrument_ID,global,o,c,\""+str(instrument_ID)+"\" "+str(output))
    os.system("ncatted -O -h -a dataset_name,global,o,c,\""+str(dataset_name)+"\" "+str(output))
    os.system("ncatted -O -h -a title,global,o,c,\""+str(title)+"\" "+str(output))
    os.system("ncatted -O -h -a summary,global,o,c,\""+str(summary)+"\" "+str(output))
    os.system("ncatted -O -h -a keywords,global,o,c,\""+str(keywords)+"\" "+str(output))
    os.system("ncatted -O -h -a keywords_vocabulary,global,o,c,\""+str(keywords_vocabulary)+"\" "+str(output))
    os.system("ncatted -O -h -a license,global,o,c,\""+str(license_)+"\" "+str(output))
    os.system("ncatted -O -h -a processing_level,global,o,c,\""+str(processing_level)+"\" "+str(output))
    os.system("ncatted -O -h -a date_created,global,o,c,\""+str(date_created)+"\" "+str(output))
    os.system("ncatted -O -h -a cdm_data_type,global,o,c,\""+str(cdm_data_type)+"\" "+str(output))
    os.system("ncatted -O -h -a time_coverage_start,global,o,c,\""+str(time_coverage_start)+"\" "+str(output))
    os.system("ncatted -O -h -a time_coverage_end,global,o,c,\""+str(time_coverage_end)+"\" "+str(output))
    os.system("ncatted -O -h -a timeline_id,global,o,c,\""+str(timeline_id)+"\" "+str(output))
    os.system("ncatted -O -h -a production_data_source,global,o,c,\""+str(production_data_source)+"\" "+str(output))
    os.system("ncatted -O -h -a id,global,o,c,\""+str(id__)+"\" "+str(output))

    dataset.close()
    ## Remove file
    os.system("rm -rf tmp/navigation.modified.nc")
    os.system("rm -rf "+path_)
    print('File saved!')


def main():
    list_of_files = file_list()
    lista = aws_file_list(list_of_files)
    download_files(lista)

if __name__ == "__main__":
    main()
