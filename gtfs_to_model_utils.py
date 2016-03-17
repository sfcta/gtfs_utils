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
    if mpm2 < mpm1: mpm2 += 24*60
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

        # settings
        self.has_time_periods       = False
        self.weekday_only           = weekday_only
        self.segment_by_service_id  = segment_by_service_id

        # Useful GTFS manipulations
        self.stop_sequence_cols = self.get_stop_sequence_cols()
        self.weekday_service_ids = self.get_weekday_service_ids()
        self.used_stops     = self.get_used_stops(index='stop_id')
        if self.weekday_only:
            self.trips = self.trips[self.trips['service_id'].isin(self.weekday_service_ids)]

        # common groupings
        self.route_trips        = None
        self.route_patterns     = None
        self.route_statistics   = None
##        self.route_patterns  = self.get_route_patterns()
##        self.route_patterns  = self.get_similarity_index()
##        self.route_statistics = self.get_route_statistics()

        # standard index columns to be used for grouping
        self._route_trip_idx_cols = ['route_id','trip_id','shape_id','direction_id','route_short_name','route_long_name','route_desc']
        self._trip_idx_cols = ['trip_id','direction_id']

    def assign_direction(self):
        '''
        if direction is missing, assign inbound/outbound
        '''
        pass
    
    def is_aligned_stop_sequence(self):
        '''
        return true if there is a set correspondence between stop_id and stop_sequence for all routes
        '''
        pass
        
    def align_stop_sequence(self):
        '''
        reassign stop_sequences so there is a unique correspondence between stop_id and stop_sequence for all routes
        '''
        pass
        
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

    def apply_time_periods(self, time_periods):
        # update column collections
        self._route_trip_idx_cols += ['trip_departure_tp']
        self._trip_idx_cols += ['trip_departure_tp']
        
        self.has_time_periods = True        
        if time_periods != None and not isinstance(time_periods,list) and not isinstance(time_periods,dict):
            raise Exception("time_periods MUST be None-type OR list-type of HH:MM:SS-HH:MM:SS pairs")

        self.stop_times['arr_mpm'] = self.stop_times['arrival_time'].map(HHMMSS_to_MPM)
        self.stop_times['dep_mpm'] = self.stop_times['departure_time'].map(HHMMSS_to_MPM)
        self.stop_times['arr_tp'] = 'other'
        self.stop_times['dep_tp'] = 'other'
        if isinstance(time_periods, list):
            ntp = {}
            for ctp in time_periods:
                ntp['%s' % ctp] = ctp
            time_periods = ntp

        for key, value in time_periods.iteritems():
            tp_name = key
            tp_start, tp_end = HHMMSSpair_to_MPMpair(value)
            arr_idx = self.stop_times[(self.stop_times['arr_mpm'].between(tp_start,tp_end))
                                      | (self.stop_times['arr_mpm'].between(tp_start-24*60,tp_end-24*60))
                                      | (self.stop_times['arr_mpm'].between(tp_start+24*60,tp_end+24*60))].index
            dep_idx = self.stop_times[(self.stop_times['dep_mpm'].between(tp_start,tp_end))
                                      | (self.stop_times['dep_mpm'].between(tp_start-24*60,tp_end-24*60))
                                      | (self.stop_times['dep_mpm'].between(tp_start+24*60,tp_end+24*60))].index
            self.stop_times.loc[arr_idx,'arr_tp'] = key
            self.stop_times.loc[dep_idx,'dep_tp'] = key

        first_stop = self.stop_times.groupby(['trip_id']).first()
        self.trips = self.trips.set_index(['trip_id'])
        self.trips['trip_departure_time'] = first_stop['departure_time']
        self.trips['trip_departure_mpm'] = first_stop['dep_mpm']
        self.trips['trip_departure_tp'] = first_stop['dep_tp']
        self.trips = self.trips.reset_index()
                
    def set_route_statistics(self):
        self.route_statistics = None
        self.route_statistics = self.get_route_statistics()
        return self.route_statistics
        
    def get_route_statistics(self):
        if isinstance(self.route_statistics, pd.DataFrame): return self.route_statistics
        if not isinstance(self.route_trips, pd.DataFrame):
            self.route_trips = pd.merge(self.routes,self.trips,on='route_id')

        
        route_statistics = None
        return route_statistics

    def set_route_patterns(self):
        self.route_patterns = None
        self.route_patterns = self.get_route_patterns()
        return self.route_patterns
            
    def get_route_patterns(self):
        trip_route = pd.merge(self.routes,self.trips,on='route_id')
        trip_route = pd.DataFrame(trip_route,columns=self._route_trip_idx_cols)
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
        idx_cols = ['route_id','direction_id']
        grouped = self.route_patterns.groupby(idx_cols)
        route_pattern = self.route_patterns #.set_index(idx_cols)
        route_pattern['is_base_route'] = 0
        route_pattern['total_base_stops'] = 0
        route_pattern['similar_base_stops'] = 0
        route_pattern['similarity_index'] = 0
        
        for name, group in grouped:
            this_group = pd.DataFrame(group,columns=self.stop_sequence_cols+['count'])
            # assume the pattern that shows up the most is the base route
            maxrow = this_group[this_group.index == this_group['count'].idxmax()]
            route_pattern.loc[this_group.index,'total_base_stops'] = maxrow.T.count().sum()-1
            route_pattern.loc[this_group.index,'similar_base_stops'] = this_group.eq(maxrow.values.tolist()[0]).T.sum() - 1
        route_pattern['similarity_index'] = route_pattern['similar_base_stops'] / route_pattern['total_base_stops']

        return route_pattern
    
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
    time_periods = {'EA':"03:00:00-05:59:59",
                    'AM':"06:00:00-08:59:59",
                    'MD':"09:00:00-15:29:59",
                    'PM':"15:30:00-18:29:59",
                    'EV':"18:30:00-27:00:00"}
    
    gtfs.apply_time_periods(time_periods)
    gtfs.set_route_patterns()
    route_patterns = gtfs.get_route_patterns()
    print route_patterns[0:10]
    
    
    