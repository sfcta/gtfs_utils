'''
'''

import sys, os
import numpy as np
import pandas as pd
import shapefile
import itertools
import copy

def HHMMSS_to_MPM(hhmmss):
    sep = ':'
    if sep:
        hh, mm, ss = hhmmss.split(sep)
    else:
        hh = hhmmss[:2]
        mm = hhmmss[2:4]
        ss = hhmmss[4:]

    return 60 * int(hh) + int(mm) + int(ss)/60

def HHMMSSpair_to_MPMpair(hhmmsspair):
    hhmmss1, hhmmss2 = hhmmsspair.split('-')
    mpm1 = HHMMSS_to_MPM(hhmmss1)
    mpm2 = HHMMSS_to_MPM(hhmmss2)
    return (mpm1,mpm2)

class GTFSFeed(object):
    def __init__(self, path='.',agency='agency.txt',calendar='calendar.txt',calendar_dates='calendar_dates.txt',fare_attributes='fare_attributes.txt',
                 fare_rules='fare_rules.txt',routes='routes.txt',shapes='shapes.txt',stop_times='stop_times.txt',stops='stops.txt',
                 trips='trips.txt',weekday_only=True, segment_by_service_id=True):
        # GTFS files
        self.path           = path
        self.agency         = None
        self.calendar       = None
        self.calendar_dates = None
        self.fare_attributes= None
        self.fare_rules     = None
        self.routes         = None
        self.shapes         = None
        self.stop_times     = None
        self.stops          = None
        self.trips          = None
        
        all_files = [agency, calendar, calendar_dates, fare_attributes, fare_rules, routes, shapes, stop_times, stops, trips]
        all_names = ['agency','calendar','calendar_dates','fare_attributes','fare_rules','routes','shapes','stop_times','stops','trips']

        for name, file in itertools.izip(all_names, all_files):
            try:
                self.__dict__[name] = pd.read_csv(os.path.join(path,file))
            except:
                print "%s not found in %s" % (file, path)
        print self.agency

        # settings
        self.weekday_only           = weekday_only
        self.segment_by_service_id  = segment_by_service_id

        # common groupings
        # route_patterns group columns = []
        # Useful GTFS manipulations
        self.stop_sequence_cols = self.get_stop_sequence_cols()
        self.weekday_service_ids = self.get_weekday_service_ids()
        if self.weekday_only:
            self.trips = self.trips[self.trips['service_id'].isin(self.weekday_service_ids)]
        self.route_patterns  = self.get_route_patterns()
        self.route_statistics = self.get_route_statistics()
        self.used_stops     = self.get_used_stops(index='stop_id')

##    def _reload(self):
##        pass
        
    def drop_weekend(self):
        self.weekday_only = True
        self.trips = self.trips[self.trips['service_id'].isin(self.weekday_service_ids)]
        self.route_statistics = self.route_statistics[self.route_statistics['service_id'].isin(self.weekday_service_ids)]
        self.route_pattern = self.route_pattern[self.route_pattern['service_id'].isin(self.weekday_service_ids)]

    def drop_days(self, days=['saturday','sunday']):
        service_ids = []
        for day in days:
            service_ids += self.get_service_ids_by_day(day)
        self.trips = self.trips[~self.trips['service_id'].isin(service_ids)]
        self.route_statistics = self.route_statistics[~self.route_statistics['service_id'].isin(service_ids)]
        self.route_patterns = self.route_patterns[~self.route_patterns['service_id'].isin(service_ids)]
        
    def get_service_ids_by_day(self, day='monday'):
        service_ids = self.calendar[self.calendar[day] == 1]['service_id'].tolist()
        return list(set(service_ids))
    
    def get_weekday_service_ids(self, weekdays=['monday','tuesday','wednesday','thursday','friday']):
        weekday_service_ids = []
        for day in weekdays:
            weekday_service_ids += self.calendar[self.calendar[day] == 1]['service_id'].tolist()
        weekday_service_ids = list(set(weekday_service_ids))
        return weekday_service_ids

    def get_used_stops(self, index='stop_id'):
        used_stops = pd.DataFrame(self.stop_times,columns=['stop_id'])
        used_stops = used_stops.drop_duplicates()
        if index:
            used_stops = used_stops.set_index(index)
        used_stops['used_flag'] = 1
        return used_stops
        
    def match_stops(self, other):
        return True

    def get_route_statistics(self, custom_time_periods=None):
        if custom_time_periods != None and not isinstance(custom_time_periods,list) and not isinstance(custom_time_periods,dict):
            raise Exception("custom_time_periods MUST be None-type OR list-type of HH:MM:SS-HH:MM:SS pairs")

        trip_route = pd.merge(self.routes,self.trips,on='route_id')
        route_pattern = self.route_patterns
        
        #trip_route = pd.DataFrame(trip_route,columns=['route_id','route_short_name','trip_id','service_id'])
        stop_times = pd.merge(self.stop_times,trip_route,on='trip_id',how='left')
        stop_times['arr_mpm'] = stop_times['arrival_time'].map(HHMMSS_to_MPM)
        stop_times['dep_mpm'] = stop_times['departure_time'].map(HHMMSS_to_MPM)
        if isinstance(custom_time_periods, list):
            for ctp in custom_time_periods:
                tp_name = ctp
                tp_start, tp_end = HHMMSSpair_to_MPMpair(ctp)
                stop_times['%s' % ctp] = 0
                stop_times['%s' % ctp] += 1 * stop_times['arr_mpm'].between(tp_start,tp_end)

        elif isinstance(custom_time_periods, dict):
            for key, value in custom_time_periods.iteritems():
                tp_name = key
                tp_start, tp_end = HHMMSSpair_to_MPMpair(value)
                stop_times[key] = 0
                stop_times[key] += 1 * stop_times['arr_mpm'].between(tp_start,tp_end)
        
        route_statistics = None
        return route_statistics

    def get_route_patterns(self, custom_time_periods=None):
        trip_route = pd.merge(self.routes,self.trips,on='route_id')
        trip_route = pd.DataFrame(trip_route,columns=['route_id','trip_id','shape_id','direction_id','route_short_name','route_long_name','route_desc'])
        patterns = self.stop_times.pivot(index='trip_id',columns='stop_sequence',values='stop_id')
        patterns = patterns.reset_index()
        
        route_pattern = pd.merge(trip_route, patterns, on='trip_id')
        columns = route_pattern.columns.tolist()
        columns.remove('trip_id')
        columns.remove('shape_id')

        route_pattern = route_pattern.fillna(-1)
        grouped_route_pattern = route_pattern.groupby(columns)
        route_pattern = grouped_route_pattern.count()
        route_pattern['count'] = route_pattern['trip_id']
        route_pattern['trip_id'] = grouped_route_pattern.first()['trip_id']
        route_pattern['shape_id'] = grouped_route_pattern.first()['shape_id']
        route_pattern = route_pattern.reset_index()
        route_pattern = route_pattern.replace(-1, np.nan)
        
        return route_pattern

    def get_stop_sequence_cols(self):
        stop_sequence_cols = list(set(self.stop_times['stop_sequence'].tolist()))
        return stop_sequence_cols
        
    def get_similarity_index(self, base_flag_column='is_base_route'):
        # figure out which pattern is the 'base' pattern
        route_pattern = self.route_patterns
        grouped = route_pattern.groupby(['route_id','direction_id','route_short_name','route_long_name','route_desc'])
        
        return route_patterns
    
    def __str__(self):
        ret = 'GTFS Feed at %s containing:' % self.path
        i = 0
        for key, value in self.__dict__.items():
            if not key.startswith('__'):
                if not isinstance(value, pd.DataFrame):
                    continue
                if i == 0:
                    ret = ret + '\n%s' % (key)
                    i += 1
                else:
                    ret = ret + ', %s' % (key)
        return ret
        
if __name__=='__main__':
    gtfs = GTFSFeed('..\SFMTA_20120319')
    print gtfs
    route_patterns = gtfs.get_route_patterns()
    print route_patterns[0:10]
    
    
    