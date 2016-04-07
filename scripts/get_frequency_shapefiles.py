import sys, os
import numpy as np
import pandas as pd
sys.path.insert(0,r'Y:\champ\util\pythonlib-migration\master_versions\gtfs_utils')
import gtfs_utils
import shapefile

if __name__=='__main__':
    args = sys.argv[1:]
    path = args[0]
    tag = args[1]
    
    gtfs = gtfs_utils.GTFSFeed(path)
    print gtfs
    time_periods = {'09 to 10':"21:00:00-21:59:59",
                    '10 to 11':"22:00:00-22:59:59",
                    '11 to 12':"23:00:00-23:59:59",
                    '12 to 01':"12:00:00-12:59:59",
                    '01 to 02':"01:00:00-01:59:59",
                    '02 to 03':"02:00:00-02:59:59",
                    '03 to 04':"03:00:00-03:59:59",
                    '04 to 05':"04:00:00-04:59:59"}

    print "loading gtfs"
    gtfs.load()

    print "conventionalizing"
    gtfs.conventionalize()
    
    #gtfs.routes = gtfs.routes[gtfs.routes['route_id'] == 7517]
    print "applying time periods"
    gtfs.apply_time_periods(time_periods)

    #gtfs.routes = gtfs.routes[gtfs.routes['route_id'] == 7517]
    print "building common dataframes"
    gtfs.build_common_dfs()
    print "writing route statistics"
    late_night_routes = gtfs.route_statistics[(gtfs.route_statistics['freq'] > 0) & (gtfs.route_statistics['trip_departure_tp'] != 'other')]
    late_night_routes.to_csv('%s_late_night_route_stats.csv' % tag)
    shape_ids = late_night_routes['shape_id'].drop_duplicates().tolist()
    shapes = gtfs.shapes[gtfs.shapes['shape_id'].isin(shape_ids)]
    print "writing shapes"
    shapes.to_csv('%s_late_night_shapes.csv' % tag)
   
    #ln_freq = late_night_routes.fillna(-1)
    #ln_freq = ln_freq.set_index(['route_id','route_short_name','route_long_name','shape_id','direction_id'])
    ln_freq = late_night_routes.pivot_table(index=['route_id','route_short_name','route_long_name','shape_id','direction_id'],
                                            columns='trip_departure_tp',values='freq')
    ln_freq.to_csv('%s_late_night_route_freqs.csv' % tag)
    ln_freq = ln_freq.reset_index()
    #print ln_freq[ln_freq['shape_id'] == '40151']
    shape_freq = pd.merge(shapes, ln_freq, how='left',on='shape_id')
    shape_freq.to_csv('sfreq.csv')
    # write shapefile
    shape_writer = shapefile.Writer(shapeType=shapefile.POLYLINE)
    shape_writer.field('route_id',          'N',    10, 0)
    shape_writer.field('route_short_name',  'C',    10, 0)
    shape_writer.field('route_long_name',   'C',    50, 0)
    shape_writer.field('direction_id',      'N',    1,  0)
    shape_writer.field('shape_id',          'N',    10, 0)        
    shape_writer.field('09 to 10',          'N',    10, 0)
    shape_writer.field('10 to 11',          'N',    10, 0)
    shape_writer.field('11 to 12',          'N',    10, 0)
    shape_writer.field('12 to 01',          'N',    10, 0)
    shape_writer.field('01 to 02',          'N',    10, 0)
    shape_writer.field('02 to 03',          'N',    10, 0)
    shape_writer.field('03 to 04',          'N',    10, 0)
    shape_writer.field('04 to 05',          'N',    10, 0)

    for tp in ['09 to 10','10 to 11','11 to 12','12 to 01','01 to 02','02 to 03','03 to 04','04 to 05']:
        if tp not in shape_freq.columns.tolist():
            shape_freq[tp] = 0
            
    shape_id = None
    #print shape_freq[shape_freq['shape_id'] == '40182']
    #print shape_freq[shape_freq['shape_id'] == '40151']
    for i, shape in shape_freq.iterrows():
        if shape_id == None:
            route_id = shape['route_id']
            route_short_name =  shape['route_short_name']
            route_long_name = shape['route_long_name']
            direction_id = shape['direction_id']
            shape_id = shape['shape_id']
            s0910 = shape['09 to 10']
            s1011 = shape['10 to 11']
            s1112 = shape['11 to 12']
            s1201 = shape['12 to 01']
            s0102 = shape['01 to 02']
            s0203 = shape['02 to 03']
            s0304 = shape['03 to 04']
            s0405 = shape['04 to 05']
            points = []
            
        if shape_id != shape['shape_id']:
            shape_writer.line([points])
            shape_writer.record(route_id, route_short_name, route_long_name, direction_id, shape_id,
                                s0910, s1011, s1112, s1201, s0102, s0203, s0304, s0405)
            route_id = shape['route_id']
            route_short_name =  shape['route_short_name']
            route_long_name = shape['route_long_name']
            direction_id = shape['direction_id']
            shape_id = shape['shape_id']
            s0910 = shape['09 to 10']
            s1011 = shape['10 to 11']
            s1112 = shape['11 to 12']
            s1201 = shape['12 to 01']
            s0102 = shape['01 to 02']
            s0203 = shape['02 to 03']
            s0304 = shape['03 to 04']
            s0405 = shape['04 to 05']
            points = []
            
        point = [shape['shape_pt_lon'],shape['shape_pt_lat']]
        points.append(point)
    # write the last one
    shape_writer.line([points])
    shape_writer.record(shape['route_id'], shape['route_short_name'],
                        shape['route_long_name'], shape['direction_id'], shape['shape_id'],
                        shape['09 to 10'], shape['10 to 11'], shape['11 to 12'],
                        shape['12 to 01'], shape['01 to 02'], shape['02 to 03'],
                        shape['03 to 04'], shape['04 to 05'])
    shape_writer.save('%s_late_night_service.shp' % tag)

    # write projection
    prj = open('%s_late_night_service.prj' % tag, "w")
    epsg = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
    prj.write(epsg)
    prj.close()