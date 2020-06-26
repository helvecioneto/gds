import GOES
import pandas as pd
import datetime



#
## download one ABI's channels of full disk
#GOES.download('goes16', 'ABI-L2-CMIPF',
#              DateTimeIni = '20190101-203000', DateTimeFin = '20190101-204000', 
#              Channel = ['02'], PathOut='data/')
#
#cbh = pd.offsets.CustomBusinessHour(
#                                    normalize=False,
#                                    start='08:00', 
#                                    end='23:00', 
#                                    weekmask='Sun Mon Tue Wed Thu Fri Sat')
###
#day_range = pd.date_range(pd.datetime(2019,1,1,0),pd.datetime(2020,1,1,0),freq=cbh)
##
#d

days = pd.date_range(start='1/1/2019', end='30/12/2019', freq='10min')
hours = pd.date_range("07:50", "20:00", freq="10min").strftime('%H:%M:%S')

data_range = []

for d in range(len(days)):
    for m in range(len(hours)):
        if days[d].strftime('%H:%M:%S') == hours[m]:
            data_range.append(days[d])


