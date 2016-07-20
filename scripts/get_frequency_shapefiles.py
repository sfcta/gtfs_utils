'''
author: Drew Cooper
date:   7/1/2016
'''

import sys, os, getopt
import numpy as np
import pandas as pd
sys.path.insert(0,r'Y:\champ\util\pythonlib-migration\master_versions\gtfs_utils')
import gtfs_utils
import shapefile

USAGE = '''
get_frequency_shapefiles.py -s -l -i gtfs_directory tag
'''
if __name__=='__main__':
    opts, args = getopt.getopt(sys.argv[1:], 'sli')
    path = args[0]
    tag = args[1]
    
    write_stops, write_lines, as_separate_files = False, False, False
    for o, a in opts:
        if o == '-s':
            write_stops = True
        if o == '-l':
            write_lines = True
        if o == '-i':
            as_separate_files = True
    
    
    gtfs = gtfs_utils.GTFSFeed(path)
    print gtfs
    time_periods = {'AM':"06:00:00-08:59:59",
                    'MD':"09:00:00-15:29:59",
                    'PM':"15:30:00-18:29:59",
                    'EV1':"18:30:00-23:59:59",
                    'EV2':"00:00:00-02:59:59",
                    'EA':"03:00:00-05:59:59"}
    tp_list = {'AM','MD','PM','EV1','EV2','EA'}
                    
    print "loading gtfs"
    gtfs.load()

    print "standardizing"
    gtfs.standardize()
    
    print "applying time periods"
    gtfs.apply_time_periods(time_periods)

    print "building common dataframes"
    gtfs.build_common_dfs()
    
    print "getting true shapes"
    shape_ids = gtfs.route_trips['shape_id'].drop_duplicates().tolist()
    shapes = gtfs.shapes[gtfs.shapes['shape_id'].isin(shape_ids)]

    print "attaching frequency statistics to shapes"
    shape_freq = pd.merge(shapes, gtfs.route_patterns, how='left', on='shape_id')
    shape_freq = pd.DataFrame(shape_freq, columns=shapes.columns.tolist()+['pattern_id'])
    shape_freq = pd.merge(shape_freq, gtfs.route_statistics, how='left', on='pattern_id')

    if write_lines:
        print "writing shapes"
        if not as_separate_files:
            shape_writer = shapefile.Writer(shapeType=shapefile.POLYLINE)
            shape_writer.field('route_id',          'N',    10, 0)
            shape_writer.field('route_short_name',  'C',    10, 0)
            shape_writer.field('route_long_name',   'C',    50, 0)
            shape_writer.field('direction_id',      'C',    15,  0)
            shape_writer.field('shape_id',          'N',    10, 0)
            # add timeperiod frequency columns
            for tp in tp_list:
                shape_writer.field(tp, 'N', 10, 0)
            for tp in tp_list:
                if tp not in shape_freq.columns.tolist():
                    shape_freq[tp] = 0
                    
        # write shapefile    
        shape_id = None
        for i, shape in shape_freq.iterrows():
            if shape_id == None:
                if as_separate_files:
                    # then open up a new shapefile
                    shape_writer = shapefile.Writer(shapeType=shapefile.POLYLINE)
                    shape_writer.field('route_id',          'N',    10, 0)
                    shape_writer.field('route_short_name',  'C',    10, 0)
                    shape_writer.field('route_long_name',   'C',    50, 0)
                    shape_writer.field('direction_id',      'C',    15,  0)
                    shape_writer.field('shape_id',          'N',    10, 0)
                    # add timeperiod frequency columns
                    for tp in tp_list:
                        shape_writer.field(tp, 'N', 10, 0)
                    for tp in tp_list:
                        if tp not in shape_freq.columns.tolist():
                            shape_freq[tp] = 0

                # set up attributes
                route_id = shape['route_id']
                route_short_name =  shape['route_short_name']
                route_long_name = shape['route_long_name']
                direction_id = shape['direction_id']
                shape_id = shape['shape_id']
                tp_freqs = []
                for tp in tp_list:
                    tp_freqs.append(round(shape['%s_freq' % tp],2))
                #print "tp_freqs:", tp_freqs
                points = []
                
            if shape_id != shape['shape_id']:
                # it's a new line, so write the previous one
                shape_writer.line([points])
                shape_writer.record(route_id, route_short_name, route_long_name, direction_id, shape_id, *tp_freqs)
                
                if as_separate_files:
                    # then save the shapefile
                    shape_writer.save('%s_%s_%s_%s_%s.shp' % (tag, route_id,route_short_name,direction_id,shape_id))
                    prj = open('%s_%s_%s_%s_%s.prj' % (tag, route_id,route_short_name,direction_id,shape_id), "w")
                    epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
                    prj.write(epsg)
                    prj.close()
                    # and open up a new shapefile
                    shape_writer = shapefile.Writer(shapeType=shapefile.POLYLINE)
                    shape_writer.field('route_id',          'N',    10, 0)
                    shape_writer.field('route_short_name',  'C',    10, 0)
                    shape_writer.field('route_long_name',   'C',    50, 0)
                    shape_writer.field('direction_id',      'C',    15,  0)
                    shape_writer.field('shape_id',          'N',    10, 0)
                    # add timeperiod frequency columns
                    for tp in tp_list:
                        shape_writer.field(tp, 'N', 10, 0)
                    for tp in tp_list:
                        if tp not in shape_freq.columns.tolist():
                            shape_freq[tp] = 0
                        
                # get attributes for this line
                route_id = shape['route_id']
                route_short_name =  shape['route_short_name']
                route_long_name = shape['route_long_name']
                direction_id = shape['direction_id']
                shape_id = shape['shape_id']
                tp_freqs = []
                for tp in tp_list:
                    tp_freqs.append(round(shape['%s_freq' % tp],2))
                points = []
                
            point = [shape['shape_pt_lon'],shape['shape_pt_lat']]
            points.append(point)
        # write the last one
        shape_writer.line([points])
        shape_writer.record(shape['route_id'], shape['route_short_name'],
                            shape['route_long_name'], shape['direction_id'], shape['shape_id'], *tp_freqs)
        if as_separate_files:
            shape_writer.save('%s_%s_%s_%s_%s.shp' % (tag, shape['route_id'],shape['route_short_name'],shape['direction_id'],shape['shape_id']))
            prj = open('%s_%s_%s_%s_%s.prj' % (tag, shape['route_id'],shape['route_short_name'],shape['direction_id'],shape['shape_id']), "w")
        else:
            shape_writer.save('%s_lines.shp' % (tag))
            prj = open('%s_lines.prj' % (tag), "w")
        epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
        prj.write(epsg)
        prj.close()

    if write_stops:
        if not as_separate_files:
            shape_writer = shapefile.Writer(shapeType=shapefile.POINT)
            shape_writer.field('route_id',          'N',    10, 0)
            shape_writer.field('route_short_name',  'C',    10, 0)
            shape_writer.field('route_long_name',   'C',    50, 0)
            shape_writer.field('direction_id',      'C',    15,  0)
        print "writing stops files"
        route_id = None
        for i, stops in gtfs.stop_routes.iterrows():
            if route_id == None:
                if as_separate_files:
                    # then open up a new shapefile
                    shape_writer = shapefile.Writer(shapeType=shapefile.POINT)
                    shape_writer.field('route_id',          'N',    10, 0)
                    shape_writer.field('route_short_name',  'C',    10, 0)
                    shape_writer.field('route_long_name',   'C',    50, 0)
                    shape_writer.field('direction_id',      'C',    15,  0)

                # set up attributes
                route_id = stops['route_id']
                route_short_name =  stops['route_short_name']
                route_long_name = stops['route_long_name']
                direction_id = stops['direction_id']
                
            if route_id != stops['route_id'] or route_short_name != stops['route_short_name'] or direction_id != stops['direction_id']:
                if as_separate_files:
                    # then save the last shapefile
                    shape_writer.save('%s_%s_%s_%s_stops.shp' % (tag, route_id, route_short_name, direction_id))
                    prj = open('%s_%s_%s_%s_stops.prj' % (tag, route_id, route_short_name, direction_id), "w")
                    epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
                    prj.write(epsg)
                    prj.close()
                    # and open up a new shapefile
                    shape_writer = shapefile.Writer(shapeType=shapefile.POINT)
                    shape_writer.field('route_id',          'N',    10, 0)
                    shape_writer.field('route_short_name',  'C',    10, 0)
                    shape_writer.field('route_long_name',   'C',    50, 0)
                    shape_writer.field('direction_id',      'C',    15,  0)

                # set up attributes
                route_id = stops['route_id']
                route_short_name =  stops['route_short_name']
                route_long_name = stops['route_long_name']
                direction_id = stops['direction_id']

            x, y = stops['stop_lon'], stops['stop_lat']
            shape_writer.point(x,y)
            shape_writer.record(route_id, route_short_name, route_long_name, direction_id)
        # write the last one
        if as_separate_files:
            shape_writer.save('%s_%s_%s_%s_stops.shp' % (tag, route_id, route_short_name, direction_id))
            prj = open('%s_%s_%s_%s_stops.prj' % (tag, route_id, route_short_name, direction_id), "w")
        else:
            shape_writer.save('%s_stops.shp' % (tag))
            prj = open('%s_stops.prj' % (tag), "w")
        epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
        prj.write(epsg)
        prj.close()
        # write projection
        prj = open('%s_route_freq.prj' % tag, "w")
        epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
        prj.write(epsg)
        prj.close()