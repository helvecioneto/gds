# -*- coding: utf-8 -*-

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr 14 18:51:39 2020

@author: helvecioneto
"""
import s3fs
import numpy as np
import datetime as dt
import os
from datetime import datetime


def to_julian_day(year, month, day):    
    date = dt.datetime.strptime('%s-%s-%s'%(year,month, day), '%Y-%m-%d')
    return str(date.timetuple().tm_yday).zfill(3)

def download(**kargs):
    import datetime as dt
    if kargs.get('day'):
        day = kargs.get('day')
    if kargs.get('month'):
        month = kargs.get('month')
    if kargs.get('year'):
        year = kargs.get('year')
    if kargs.get('ch'):
        ch = kargs.get('ch')
        
    
    aws = s3fs.S3FileSystem(anon=True)    
    julian_day = to_julian_day(year, month, day)
    bucket = 'noaa-goes16/ABI-L2-CMIPF' 
    query = aws.ls('%s/%s/%s'%(bucket, year, julian_day))
    hours = np.array(query)
    path = 'data/%s/%s/%s'%(year, month, day)
    
    if not os.path.exists(path):
        os.makedirs(path)

    #Get Files from 8am to 9pm
    hours = (hours[8:22])

    for hour in hours:
        files = aws.ls(hour)
        print(files)
#        for file in files:
#            if file.find('M3C'+str(ch)) >= 1:
#                print('Downloading %s...'%file.split('/')[-1])
#        
#                time = dt.datetime.now()
#                year_, month_, day_ = str(time.year), str(time.month).zfill(2), str(time.day).zfill(2)
#                hour_, minute_, second_ = str(time.hour).zfill(2), str(time.minute).zfill(2), str(time.second).zfill(2)
#            
#                time = '%s-%s-%s %s:%s:%s'%(day_, month_, year_, hour_, minute_, second_)
#                with open('./log_download_goes.txt', 'a') as f:
#                    f.write('%s : STARTED DOWNOLAD, CH %s\n'%(time, ch))
#                
#                aws.get(file, '%s/%s'%(path, file.split('/')[-1]))
#                
#                time = dt.datetime.now()
#                year_, month_, day_ = str(time.year), str(time.month).zfill(2), str(time.day).zfill(2)
#                hour_, minute_, second_ = str(time.hour).zfill(2), str(time.minute).zfill(2), str(time.second).zfill(2)
#            
#                time = '%s-%s-%s %s:%s:%s¨'%(day_, month_, year_, hour_, minute_, second_)
#                with open('./log_download_goes.txt', 'a') as f:
#                    f.write('%s : FINISHED\n'%(time))


