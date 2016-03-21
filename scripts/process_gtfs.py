import sys, os
import numpy as np
import pandas as pd
sys.path.insert(0,os.path.join(__file__,r'..'))
import gtfs_utils

if __name__=='__main__':
    path = r'Q:\Model Development\SHRP2-fasttrips\Task2\gtfs\SFMTA_20120319'
    gtfs = gtfs_utils.GTFSFeed(path)
    gtfs.apply_time_periods
    time_periods = {'EA':"03:00:00-05:59:59",
                    'AM':"06:00:00-08:59:59",
                    'MD':"09:00:00-15:29:59",
                    'PM':"15:30:00-18:29:59",
                    'EV':"18:30:00-27:00:00"}
    gtfs.set_route_patterns()
    