# -*- coding: utf-8 -*-
# Start and end dates are in MM/DD/YYYY format.
# End date is not inclusive (it will not be downloaded).
START = '1/1/2019'
END = '1/1/2020'
INTERVAL = '60min'
INIT_H = '09:00'
END_H = '21:00'

SATELLITE = 'noaa-goes16'
PRODUCT = 'ABI-L2-CMIPF'
CHANNEL = '02'

BBOX   = "-75.0 -35.0 -33.0 7.0"
PROJ   = "WGS84"

# Interpolation algorithm:
INTERP = "near"
# Data quality control threshold: highest acceptable value for DQC flag.
# (values of CMI with a DQF *above, but not including,* this threshold will be)
# filtered out.
DQC_THRESHOLD = 1

## Path directories
TMP = 'tmp'
OUTPUT = 'output'
