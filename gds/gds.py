# -*- coding: utf-8 -*-
from config import START,END,INTERVAL,INIT_H,END_H,SATELLITE,PRODUCT,CHANNEL,PATH
import s3fs
import pandas as pd
import numpy as np
import requests
from datetime import *
import pathlib
from nco import Nco


def file_list():
    days = pd.date_range(start=START, end=END, freq=INTERVAL)
    hours = pd.date_range(INIT_H,END_H, freq=INTERVAL).strftime('%H:%M:%S')
    
    data_range = []
    for d in range(len(days)):
        for m in range(len(hours)):
            if days[d].strftime('%H:%M:%S') == hours[m]:
                data_range.append(days[d])
    return data_range



#def main():
#    list_of_files = file_list()
#    
#if __name__ == "__main__":  
#    list_of_files = main()  
#    
list_of_files = file_list()
server = SATELLITE+'/'+PRODUCT+'/'    
aws = s3fs.S3FileSystem(anon=True)


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

    for i,row in list_of_files.iterrows():
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
                    print('\t{}\t{:3.0f}%\t{:.2f} min'.format(row.file,100.0*size/total_size, (datetime.now()-StartTime).seconds/60.0), end='\r', flush=True)
        print('\n')
        

lista = aws_file_list(list_of_files)
download_files(lista)