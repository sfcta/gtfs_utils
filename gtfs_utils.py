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

    return 60 * int(hh) + int(mm) + float(ss)/60

def HHMMSSpair_to_MPMpair(hhmmsspair):
    if hhmmsspair == np.nan: return (np.nan, np.nan)
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

        self.all_files = [agency, calendar, calendar_dates, fare_attributes, fare_rules, routes, shapes, stop_times, stops, trips]
        self.all_names = ['agency','calendar','calendar_dates','fare_attributes','fare_rules','routes','shapes','stop_times','stops','trips']
        
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
        
        # settings
        self.has_time_periods       = False
        self.weekday_only           = weekday_only
        self.segment_by_service_id  = segment_by_service_id

        # initialize other vars
        self.time_periods           = None
        self.stop_sequence_cols     = None
        self.weekday_service_ids    = None
        self.used_stops             = None

        self.route_trips    = None
        self.route_patterns = None
        self.repr_trips     = None
        self.trip_to_repr_trip = None
        self.route_statistics = None

        # standard index columns to be used for grouping
        self._route_trip_idx_cols = ['route_id','trip_id','shape_id','direction_id','route_short_name','route_long_name','route_desc']
        self._route_dir_idx_cols = ['route_id','route_short_name','route_long_name','shape_id','direction_id']
        self._trip_idx_cols = ['trip_id','direction_id']

    
    def load(self):
        for name, file in itertools.izip(self.all_names, self.all_files):
            try:
                self.__dict__[name] = pd.read_csv(os.path.join(self.path,file))
            except:
                print "%s not found in %s" % (file, self.path)
                
        # Useful GTFS manipulations
        self.stop_sequence_cols = self._get_stop_sequence_cols()
        self.weekday_service_ids = self._get_weekday_service_ids()
        self.used_stops     = self._get_used_stops(index='stop_id')
        if self.weekday_only:
            self.trips = self.trips[self.trips['service_id'].isin(self.weekday_service_ids)]
    
    def standardize(self):
        self._drop_stops_no_times()
        #self._assign_direction()
    
    def build_common_dfs(self):
        # common groupings
        self.route_trips        = pd.merge(self.routes,self.trips,on=['route_id'])
        self.route_patterns     = self._get_route_patterns()
        self._get_representative_trips()
        self.route_patterns     = self._get_similarity_index(self.route_patterns, idx_cols=['route_id'])
        self.route_statistics   = self._get_route_statistics()
        
##    def drop_deadheads(self, freq_threshold=0.25, similarity_threshold=0.5, how='and'):
##        if how == 'and':
##            pass
##
##        elif how == 'or':
##            pass
##        
##    def _assign_direction(self):
##        '''
##        if direction is missing, assign inbound/outbound
##        '''
##        patterns = self._get_route_patterns()
##        #patterns = self._get_similarity_index(patterns)
##        first_pattern = patterns.groupby(['route_id']).first().reset_index()
##        patterns = patterns.set_index(['route_id']+self.stop_sequence_cols)
##        patterns['direction_id'] = -1
##        first_pattern = first_pattern.set_index(['route_id']+self.stop_sequence_cols)
##        patterns.loc[first_pattern.index,'direction_id'] = 0 #patterns['direction_id']
##        patterns = self._get_similarity_index(patterns.reset_index())
##        patterns.loc[first_pattern['similarity_index'] >= 1,['direction_id']] = 0
##
##    def _drop_stops_no_times(self):
##        self.stop_times = self.stop_times[(pd.isnull(self.stop_times['arrival_time']) != True)
##                                          & (pd.isnull(self.stop_times['departure_time']) != True)]
##        
##    def is_aligned_stop_sequence(self):
##        '''
##        return true if there is a set correspondence between stop_id and stop_sequence for all routes
##        '''
##        pass
##
##    def align_stop_sequence(self):
##        '''
##        reassign stop_sequences so there is a unique correspondence between stop_id and stop_sequence for all routes
##        '''
##        rts = pd.merge(self.routes, self.trips, on=['route_id'])
##        
    def drop_weekend(self):
        self.weekday_only = True
        self.trips = self.trips[self.trips['service_id'].isin(self.weekday_service_ids)]
        self.route_statistics = self.route_statistics[self.route_statistics['service_id'].isin(self.weekday_service_ids)]
        self.route_patterns = self.route_patterns[self.route_patterns['service_id'].isin(self.weekday_service_ids)]

    def drop_days(self, days=['saturday','sunday']):
        service_ids = []
        for day in days:
            service_ids += self.get_service_ids_by_day(day)
        self.trips = self.trips[self.trips['service_id'].isin(service_ids) != True]
        self.route_statistics = self.route_statistics[self.route_statistics['service_id'].isin(service_ids) != True]
        self.route_patterns = self.route_patterns[self.route_patterns['service_id'].isin(service_ids) != True]
        
    def get_service_ids_by_day(self, day='monday'):
        service_ids = self.calendar[self.calendar[day] == 1]['service_id'].tolist()
        return list(set(service_ids))
    
    def _get_weekday_service_ids(self, weekdays=['monday','tuesday','wednesday','thursday','friday']):
        weekday_service_ids = []
        for day in weekdays:
            weekday_service_ids += self.calendar[self.calendar[day] == 1]['service_id'].tolist()
        weekday_service_ids = list(set(weekday_service_ids))
        return weekday_service_ids

    def _get_used_stops(self, index='stop_id'):
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
        self._route_dir_idx_cols += ['trip_departure_tp']
        self._trip_idx_cols += ['trip_departure_tp']
        self.time_periods = time_periods
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
                
    def _get_route_statistics(self):
        if isinstance(self.route_statistics, pd.DataFrame): return self.route_statistics
        grouped = self.route_trips.fillna(-1).groupby(self._route_dir_idx_cols) #self.route_patterns.groupby(self._route_idx_cols)
        route_statistics = grouped.sum()
        route_statistics = pd.DataFrame(route_statistics,columns=[])
        route_statistics['num_runs'] = grouped.size()
        route_statistics = route_statistics.reset_index()#.set_index('route_id')
        for tp, hhmmsspair in self.time_periods.iteritems():
            start, stop = HHMMSSpair_to_MPMpair(hhmmsspair)
            length = round(stop-start,0)
            route_statistics.loc[route_statistics['trip_departure_tp'] == tp,'period_len_minutes'] = length
        route_statistics = route_statistics.set_index(self._route_dir_idx_cols)
        route_statistics['freq'] = 60 * route_statistics['num_runs'] / route_statistics['period_len_minutes']
        route_statistics = route_statistics.reset_index()
        return route_statistics
   
    def _get_route_patterns(self):
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
        route_pattern['pattern_id'] = grouped_route_pattern.first()['trip_id']
        route_pattern['shape_id'] = grouped_route_pattern.first()['shape_id']
        route_pattern = route_pattern.reset_index()
        route_pattern = route_pattern.replace(-1, np.nan)
        
        return route_pattern

    def _get_representative_trips(self):
        patterns = self.stop_times.pivot(index='trip_id',columns='stop_sequence',values='stop_id').reset_index()
        patterns = patterns.fillna(-1)
        columns = patterns.columns.tolist()
        grouped_pattern = patterns.groupby(columns)
        trip_to_repr_trip = pd.DataFrame(index=self.trips['trip_id'],columns=['repr_trip_id'])
        repr_trip_ids = []
        for name, group in grouped_pattern:
            #print group
            repr_trip_id = group['trip_id'].irow(0)
            repr_trip_ids.append(repr_trip_id)
            all_trip_ids = group['trip_id'].values.tolist()
            if len(all_trip_ids) == 1: all_trip_ids = all_trip_ids[0]
            trip_to_repr_trip.loc[all_trip_ids,'repr_trip_id'] = repr_trip_id
            
        self.trip_to_repr_trip = trip_to_repr_trip.reset_index()
        self.repr_trips = self.trips[self.trips['trip_id'].isin(repr_trip_ids)]
        
    def _get_stop_sequence_cols(self):
        stop_sequence_cols = list(set(self.stop_times['stop_sequence'].tolist()))
        return stop_sequence_cols
        
    def _get_similarity_index(self, route_patterns, idx_cols=['route_id','direction_id']):
        # figure out which pattern is the 'base' pattern
        grouped = route_patterns.groupby(idx_cols)
        route_patterns['total_base_stops'] = 0
        route_patterns['similar_base_stops'] = 0
        route_patterns['similarity_index'] = 0
        route_patterns['is_base_pattern'] = 0
        
        for name, group in grouped:
            this_group = pd.DataFrame(group,columns=self.stop_sequence_cols+['count'])
            # assume the pattern that shows up the most is the base route
            maxrow = this_group[this_group.index == this_group['count'].idxmax()]
            if len(maxrow) > 1:
                print "maxrow contains multiple records, selecting first"
                maxrow = maxrow[0]
                
            route_patterns.loc[this_group['count'].idxmax(),'is_base_pattern'] = 1
            route_patterns.loc[this_group.index,'total_base_stops'] = maxrow.T.count().sum()-1
            route_patterns.loc[this_group.index,'similar_base_stops'] = this_group.eq(maxrow.values.tolist()[0]).T.sum() - 1
        route_patterns['similarity_index'] = route_patterns['similar_base_stops'] / route_patterns['total_base_stops']
        
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
    time_periods = {'EA':"03:00:00-05:59:59",
                    'AM':"06:00:00-08:59:59",
                    'MD':"09:00:00-15:29:59",
                    'PM':"15:30:00-18:29:59",
                    'EV':"18:30:00-27:00:00"}
    
    gtfs.apply_time_periods(time_periods)
    gtfs.set_route_patterns()
    route_patterns = gtfs.get_route_patterns()
    print route_patterns[0:10]
    
    
    