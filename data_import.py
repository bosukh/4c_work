from config import Config
from time import time
import pandas as pd
import math
import os
import sys
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, load_only
from vizio_models import VizioViewingFact, VizioDemographicDim, VizioLocationDim, \
                         VizioNetworkDim, VizioProgramDim, VizioTimeDim, \
                         VizioActivityDim

class VizioImporter(object):

    def __init__(self, year, month, day):
        # SQLalchemy initializtion
        self.config  = Config()
        self.engine  = create_engine("mysql+mysqldb://{user}:{password}@{host}:{port}/{database}".format(**self.config.CONNECTIONS['vizio']))
        self.Session = sessionmaker(bind = self.engine)
        self.Base    = declarative_base()

        # Date of the data
        self.year  = year
        self.month = month
        self.day   = day
        self.today  = date(year, month, day)

        # Tables
        self.Viewing     = VizioViewingFact(self.Base, self.year,
                                            self.month, self.day)
        self.Demographic = VizioDemographicDim(self.Base, self.year, self.month)
        self.Activity    = VizioActivityDim(self.Base)
        self.Location    = VizioLocationDim(self.Base)
        self.Network     = VizioNetworkDim(self.Base)
        self.Program     = VizioProgramDim(self.Base)
        self.Time        = VizioTimeDim(self.Base)

        # Columns
        self.ViewingCols     = [col.key for col in self.Viewing.__table__.c]
        self.DemographicCols = [col.key for col in self.Demographic.__table__.c]
        self.ActivityCols    = [col.key for col in self.Activity.__table__.c]
        self.LocationCols    = [col.key for col in self.Location.__table__.c]
        self.NetworkCols     = [col.key for col in self.Network.__table__.c]
        self.ProgramCols     = [col.key for col in self.Program.__table__.c]
        self.TimeCols        = [col.key for col in self.Time.__table__.c]

        # Initialize tables
        self.Base.metadata.create_all(self.engine, checkfirst=True)

        # Initialize reference Tables
        self.initialize_references()


    def initialize_references(self):
        # Demographics Table
        self.load_demographics()

        # Activities Table
        self.load_activities()

        # Location Table + Load zipcode-to-timezone reference csv file
        self.load_locations()

        zipcode_ref = pd.read_csv('./reference/zipcode_with_tz.csv')
        zipcode_ref.zipcode = [
            "{:05d}".format(int(x)) if pd.isnull(x) == False
            else x for x in zipcode_ref.zipcode
        ]
        zipcode_ref.columns = ['zipcode', 'timezone', 'tz_offset']
        zipcode_ref['zipcode_4'] = [x[:4] for x in zipcode_ref.zipcode]
        zipcode_ref['zipcode_3'] = [x[:3] for x in zipcode_ref.zipcode]
        zipcode_ref['zipcode_2'] = [x[:2] for x in zipcode_ref.zipcode]
        self.zipcode_ref = zipcode_ref

        # Network Table + Load reference csv file
        self.load_networks()
        #self.call_signs_ref = pd.read_csv('./reference/vizio_to_fcc_callsign.csv')
        self.call_signs_ref = pd.read_excel('./reference/Inscape_Active_Stations_6_6_17.xlsx')
        self.call_signs_ref.columns = ['station_type', 'station_dma', 'network_affiliate', 'call_sign', 'station_name']
        self.call_signs_ref = self.call_signs_ref.drop_duplicates()

        # Program Table
        self.load_programs()

        # Time Table
        self.load_times()

        # Datetime table and TimeSlot table
        datetimes  = {}
        time_slots = {}
        datetime_str = '{year}-{month}-{day}'.format(year  = self.year,
                                                     month = '{:02d}'.format(self.month),
                                                     day   = '{:02d}'.format(self.day))
        current_slot = 0 # time slot is 1-indexed.
        for hr in range(24):
            for minute in range(60):
                if minute in [30, 0]:
                    current_slot += 1
                for second in range(60):
                    key = datetime_str + ' ' + ':'.join(['{:02d}'.format(x)
                                                         for x in [hr, minute, second]])
                    datetimes[key] = (datetime(self.year, self.month, self.day,
                                               hr, minute, second),
                                      current_slot)
                    time_slots[key[-8:]] = current_slot
        self.datetimes  = datetimes
        self.time_slots = time_slots


    def __db_session(func):
        # wrapper around database operations. Open and close session when needed
        def wrapped(self, *args, **kwargs) :
            self.session = self.Session()
            func(self, *args, **kwargs)
            self.session.close()
        return wrapped


    @__db_session
    def load_demographics(self):
        demographics = []

        for row in self.session.query(
                        self.Demographic).options(load_only('id', 'household_id')):
            demographics.append([row.id,
                                 row.household_id])

        demographics = pd.DataFrame(demographics,
                                    columns = ['id',
                                               'household_id'])
        self.demographics = demographics


    @__db_session
    def load_activities(self):
        # id here should match the id of of demographic
        activities = []

        for row in self.session.query(self.Activity):
            activities.append([row.id,
                               row.household_id,
                               row.last_active_date])

        activities = pd.DataFrame(activities,
                                  columns = ['id',
                                             'household_id',
                                             'last_active_date'])
        self.activities = activities


    @__db_session
    def load_locations(self):
        # One zipcode sometimes have more than one dma, like null.
        # Use (zipcode, dma) for the mapping
        locations = []

        for row in self.session.query(
                        self.Location).options(load_only('id', 'zipcode', 'dma')):
            locations.append([row.id,
                              row.zipcode,
                              row.dma])

        locations = pd.DataFrame(locations,
                                 columns = ['id',
                                            'zipcode',
                                            'dma'])
        self.locations = locations


    @__db_session
    def load_networks(self):
        # mapping with Call_sign for now.
        # With tms, station_id will be used instead.
        networks = []

        for row in self.session.query(
                        self.Network).options(load_only('id', 'call_sign')):
            networks.append([row.id,
                             row.call_sign])

        networks = pd.DataFrame(networks,
                                columns = ['id',
                                           'call_sign'])
        self.networks = networks


    @__db_session
    def load_programs(self):
        # Oddly, One tms_id can have more than one start_time
        # (tms_id, program_name, program_start_tie) for mapping
        programs = []

        for row in self.session.query(self.Program):
            programs.append([row.id,
                             row.tms_id,
                             row.program_name,
                             row.program_start_time
                             ])

        programs = pd.DataFrame(programs,
                                columns = ['id',
                                           'tms_id',
                                           'program_name',
                                           'program_start_time'])
        self.programs = programs


    @__db_session
    def load_times(self):
        times = []
        for row in self.session.query(
                        self.Time).options(load_only('id', 'time_slot', 'date')):
            times.append([row.id,
                          row.time_slot,
                          row.date])

        times = pd.DataFrame(times,
                             columns = ['id',
                                        'time_slot',
                                        'date'])
        self.times = times


    def raw_insert(self, table_obj, pd_dataframe):
        # use external shell script to do the insertion.
        def __put_placeholder(pd_df, columns):
            for col in columns:
                if col not in pd_df.columns:
                    pd_df[col] = [None for _ in range(len(pd_df))]
            return pd_df[columns]
        table_name = table_obj.__tablename__
        table_cols = [col.key for col in table_obj.__table__.c]
        file_name = '%s_to_insert.csv'%table_name
        __put_placeholder(pd_dataframe, table_cols).to_csv(file_name,
                                                           index = False,
                                                           header = False,
                                                           sep = '^')
        os.system(
            './db_import_script.sh {file_name} {table_name}'.format(file_name = file_name,
                                                                    table_name = table_name)
        )
        os.remove(file_name)


    @__db_session
    def update_activity(self, list_of_dict):
        for i, row in enumerate(self.session.query(
                                    self.Activity).filter(
                                        self.Activity.id.in_(list_of_dict.id))):
            row.last_active_date = self.today
            self.session.add(row)
            self.session.flush()
            if i % 20000 == 0 and i != 0:
                self.session.commit()
        self.session.commit()


    def get_datetime(self, datetime_str):
        # datetime_str = '%Y-%m-%d %H-%M-%S'
        if self.datetimes.get(datetime_str) is None:
            try:
                self.datetimes[datetime_str] = (
                    datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S'),
                    self.time_slots[datetime_str[-8:]]
                )
            except ValueError:
                return (None, None)
        return self.datetimes[datetime_str]


    def extend_viewing_data(self, viewing_data):
        # Split viewing_data to fit into time slots
        # Since Vizio data is reported hourly, only need to concern with
        # the rows that falls on 30 minutes
        for col in ['viewing_start_time', 'viewing_end_time']:
            viewing_data[col + '_hour']   = [x.hour for x in viewing_data[col]]
            viewing_data[col + '_minute'] = [x.minute for x in viewing_data[col]]
            viewing_data[col + '_second'] = [x.second for x in viewing_data[col]]
        condition_1 = (
            (viewing_data.viewing_start_time_minute < 30) &
            (viewing_data.viewing_end_time_minute >= 30)
        )
        # condition 2 should actually be zero all the time. Vizio reports data in one hour interval
        condition_2 = (
            (viewing_data.viewing_start_time_minute > 30) &
            (viewing_data.viewing_end_time_hour > viewing_data.viewing_start_time_hour)
        )
        condition_3 = (condition_1 == False) & (condition_2 == False)

        extended_viewing_data = pd.DataFrame(columns = viewing_data.columns)
        viewing_data['temp_end'] = viewing_data['viewing_start_time'].copy()
        extended_viewing_data = pd.concat(
            [extended_viewing_data,
             viewing_data[condition_3]],
            ignore_index=True
        )

        temp = viewing_data[condition_1].reset_index(drop = True).copy()
        temp['temp_end'] = (
            temp.temp_end
            + pd.Series([timedelta(minutes = (30 - x.minute))
                           - timedelta(seconds = x.second)
                           for x in temp.viewing_start_time])
        )
        actual_end_time = temp.viewing_end_time.copy()
        temp['viewing_end_time'] = temp['temp_end']
        extended_viewing_data = pd.concat([extended_viewing_data, temp],
                                          ignore_index=True)
        temp['viewing_start_time'] = temp.viewing_end_time.copy()
        temp.viewing_end_time = actual_end_time
        extended_viewing_data = pd.concat([extended_viewing_data, temp],
                                          ignore_index=True)
        # Time slot
        extended_viewing_data['time_slot']        = [
            self.time_slots.get(x.strftime('%H:%M:%S'))
            for x in extended_viewing_data['viewing_end_time']
        ]
        # Date, YYYY-MM-DD
        extended_viewing_data['date']             = [
            x.date() for x in extended_viewing_data['viewing_end_time']
        ]
        # Day of week, 0-6
        extended_viewing_data['day_of_week']      = [
            x.isoweekday() for x in extended_viewing_data['date']
        ]
        # Week, of year
        extended_viewing_data['week']             = [
            x.isocalendar()[1] for x in extended_viewing_data['date']
        ]
        # Quarter
        extended_viewing_data['quarter']          = [
            math.ceil(x.month/3.0) for x in extended_viewing_data['date']
        ]
        # Viewing duration, secs
        extended_viewing_data['viewing_duration'] = [
            x.seconds for x in (extended_viewing_data['viewing_end_time']
                                - extended_viewing_data['viewing_start_time'])
        ]
        return extended_viewing_data

    def import_file(self, filepath):

        ### File Import
        if not os.path.isfile(filepath):
            raise IOError('%s - not found.'%filepath)

        columns = [
            'household_id',
            'zipcode',
            'dma',
            'tms_id',
            'program_name',
            'program_start_time',
            'call_sign',
            'program_time_at_start',
            'viewing_start_time',
            'viewing_end_time'
        ]

        viewing_data = pd.read_csv(filepath, names = columns)
        viewing_data.zipcode = [
            "{:05d}".format(int(x)) if pd.isnull(x) == False else x
            for x in viewing_data.zipcode
        ]
        viewing_data.program_start_time = [
            x[:-1].replace('T', ' ') if x != 'null' else None
            for x in viewing_data.program_start_time
        ]
        for col in ['program_start_time', 'viewing_start_time', 'viewing_end_time']:
            viewing_data[col] = pd.to_datetime(viewing_data[col])
        ### End of File Import

        ### ACTIVITY & DEMOGRAPHICS
        all_demographics = pd.DataFrame(viewing_data.household_id.unique(),
                                        columns = ['household_id'])
        in_activity_dim = pd.merge(
            all_demographics,
            self.activities,
            on = 'household_id',
            how = 'left',
            indicator=True
        )
        # households to insert to activity table and demographics table
        insert_to_activity_demo = in_activity_dim[
            (in_activity_dim['_merge'] == 'left_only')
        ].copy()
        # households to update in activity table
        update_activity = in_activity_dim[
            (in_activity_dim['_merge'] != 'left_only')
        ].copy()
        # households to insert to demographics table
        insert_to_demo = update_activity.loc[
            update_activity.id.isin(self.demographics.id) == False
        ].copy()
        # households to update in activity table
        update_activity = self.activities.loc[
            (self.activities.id.isin(update_activity.id)) & \
            (self.activities.last_active_date != self.today)
        ].copy()

        if len(insert_to_activity_demo) > 0:
            # if household_id IS NOT found in BOTH Activity_Dim table and Demographic_Dim_{month}
            start_idx = 1
            if len(self.activities):
                start_idx = int(self.activities.id.max() + 1)
            insert_to_activity_demo['last_active_date'] = [
                self.today for _ in range(len(insert_to_activity_demo))
            ]
            insert_to_activity_demo['id'] = [
                start_idx + i for i in range(len(insert_to_activity_demo))
            ]
            self.raw_insert(
                self.Activity,
                insert_to_activity_demo.filter(self.ActivityCols)
            )
            self.activities = pd.concat(
                [self.activities,
                 insert_to_activity_demo[['id',
                                          'household_id',
                                          'last_active_date']]],
                ignore_index = True
            )
            self.demographics = pd.concat(
                [self.demographics,
                 insert_to_activity_demo[['id',
                                          'household_id']]],
                ignore_index = True
            )
            self.raw_insert(
                self.Demographic,
                insert_to_activity_demo[['id',
                                         'household_id']]
            )

        if len(insert_to_demo) > 0:
            # if household_id IS found in Activity_Dim table, but NOT in Demographic_Dim_{month}
            self.raw_insert(
                self.Demographic,
                insert_to_demo[['id',
                                'household_id']]
            )
            self.demographics = pd.concat(
                [self.demographics,
                 insert_to_demo[['id',
                                 'household_id']]],
                ignore_index = True
            )

        if len(update_activity) > 0:
            # if household_id IS found in BOTH Activity_Dim table and Demographic_Dim_{month}
            # Assuming that the current time inmport is the most recent data available.
            update_activity['last_active_date'] = [
                self.today for _ in range(len(update_activity))
            ]
            self.update_activity(
                update_activity[['id',
                                 'household_id',
                                 'last_active_date']]
            )
        ### End of ACTIVITY & DEMOGRAPHICS


        ### LOCATIONS
        loc_cols = ['zipcode',
                    'dma']
        temp = pd.merge(viewing_data[loc_cols],
                        self.locations[loc_cols],
                        how='left',
                        indicator=True)['_merge'] == 'left_only'
        all_locations = viewing_data.loc[temp, loc_cols].drop_duplicates().dropna()
        all_locations = pd.merge(all_locations,
                                 self.zipcode_ref[['zipcode',
                                                   'tz_offset',
                                                   'timezone']],
                                 on = 'zipcode',
                                 how='left')
        self.all_locations = all_locations

        # if zipcode not found in reference table, try to match first 4, 3, 2 digits respectively
        for idx in all_locations.loc[all_locations.timezone.isnull()].index:
            a = self.zipcode_ref.loc[
                    self.zipcode_ref.zipcode_4 == all_locations.ix[idx].zipcode[:4],
                    ['tz_offset', 'timezone']
                ].head(1)
            if len(a) != 1:
                a = self.zipcode_ref.loc[
                        self.zipcode_ref.zipcode_3 == all_locations.ix[idx].zipcode[:3],
                        ['tz_offset', 'timezone']
                    ].head(1)
            if len(a) != 1:
                a = self.zipcode_ref.loc[
                        self.zipcode_ref.zipcode_2 == all_locations.ix[idx].zipcode[:2],
                        ['tz_offset', 'timezone']
                    ].head(1)
            if len(a):
                all_locations.ix[idx, 'tz_offset'] = a.tz_offset.values[0]
                all_locations.ix[idx,'timezone']   = a.timezone.values[0]
        all_locations = all_locations.where(pd.notnull(all_locations), None)
        self.all_locations = all_locations

        if len(all_locations) > 0:
            #print 'Locations to insert: ', len(all_locations)
            start_idx = 1
            if len(self.locations):
                start_idx = int(self.locations.id.max() + 1)
            all_locations['id'] = [start_idx + i for i in range(len(all_locations))]
            self.raw_insert(
                self.Location,
                all_locations[self.LocationCols]
            )
            self.locations = pd.concat(
                [self.locations,
                 all_locations[['id',
                                'zipcode',
                                'dma']]]
            )
        ### End of LOCATIONS


        ### NETWORK
        all_networks = viewing_data.loc[
            viewing_data.call_sign.isin(self.networks.call_sign.unique()) == False,
            ['call_sign']
        ].drop_duplicates()
        all_networks = pd.merge(
            all_networks,
            self.call_signs_ref[['call_sign',
                                 'network_affiliate',
                                 'station_dma',
                                 'station_name']],
            on = 'call_sign',
            how ='left'
        )
        all_networks = all_networks.where(pd.notnull(all_networks), None)
        self.all_networks = all_networks

        if len(all_networks) > 0:
            #print 'Networks to insert: ', len(all_networks)
            start_idx = 1
            if len(self.networks):
                start_idx = int(self.networks.id.max() + 1)
            all_networks['id'] = [start_idx + i for i in range(len(all_networks))]
            self.raw_insert(
                self.Network,
                all_networks.filter(self.NetworkCols)
            )
            self.networks = pd.concat(
                [self.networks,
                 all_networks[['id',
                               'call_sign']]]
            )
        ### End of NETWORKS


        ### PROGRAMS
        program_cols = [
            'tms_id',
            'program_name',
            'program_start_time'
        ]
        temp = pd.merge(
            viewing_data[program_cols],
            self.programs[program_cols],
            how='left',
            indicator=True
        )['_merge'] == 'left_only'

        all_programs = viewing_data.loc[temp, program_cols].drop_duplicates()
        all_programs = all_programs.where(pd.notnull(all_programs), None)
        self.all_programs = all_programs

        if len(all_programs) > 0:
            #print 'Programs to insert: ', len(all_programs)
            start_idx = 1
            if len(self.programs):
                start_idx = int(self.programs.id.max() + 1)
            all_programs['id'] = [start_idx + i for i in range(len(all_programs))]
            self.raw_insert(
                self.Program,
                all_programs.filter(self.ProgramCols)
            )
            self.programs = pd.concat(
                [self.programs,
                all_programs[['id',
                             'tms_id',
                             'program_name',
                             'program_start_time']]],
                ignore_index = True
            )
        ### End of PROGRAMS


        ## Merge reference tables for the appropirate keys
        self.orig = viewing_data.copy()

        # demographic_key
        temp = self.demographics.rename(index = str,
                                        columns = {'id':'demographic_key'})
        viewing_data = pd.merge(viewing_data,
                                temp,
                                on ='household_id',
                                how = 'left')

        # location_key
        temp = self.locations.rename(index = str,
                                     columns = {'id':'location_key'})
        viewing_data = pd.merge(viewing_data,
                                temp,
                                on = ['zipcode', 'dma'],
                                how = 'left')

        # network_key
        temp = self.networks.rename(index = str,
                                    columns = {'id':'network_key'})
        viewing_data = pd.merge(viewing_data,
                                temp,
                                on ='call_sign',
                                how = 'left')

        # program_key
        temp = self.programs.rename(index = str,
                                    columns = {'id':'program_key'})
        viewing_data = pd.merge(viewing_data,
                                temp,
                                on = [ 'tms_id', 'program_name', 'program_start_time'],
                                how = 'left')

        ### Expand viewing data and place appropirate timeslots
        dat = self.extend_viewing_data(viewing_data)

        # sometimes, these columns are interpreted as tuples
        for col in ['day_of_week', 'week']:
            try:
                dat[col]       = [x[0] for x in dat[col]]
            except IndexError:
                continue
        ### End of Expand viewing data


        ### TIMES
        all_times = dat.filter(self.TimeCols).drop_duplicates()
        temp = pd.merge(
            all_times,
            self.times,
            on = ['time_slot', 'date'],
            how = 'left',
            indicator = True
        )['_merge'] == 'left_only'
        all_times = all_times.reset_index(drop = True).loc[temp, ].drop_duplicates()
        all_times = all_times.where(pd.notnull(all_times), None)

        if len(all_times) > 0:
            #print 'Times to insert: ', len(all_times)
            start_idx = 1
            if len(self.times):
                start_idx = int(self.times.id.max() + 1)
            all_times['id'] = [start_idx + i for i in range(len(all_times))]
            self.raw_insert(
                self.Time,
                all_times.filter(self.TimeCols)
            )
            self.times = pd.concat(
                [self.times,
                all_times[['id',
                          'time_slot',
                          'date']]],
                ignore_index = True
            )
        ### End of TIMES


        ### VIEWING
        temp = self.times.rename(index = str,
                                    columns = {'id':'time_key'})
        dat = pd.merge(
            dat,
            temp,
            on = ['time_slot', 'date'],
            how = 'left'
        )
        dat['viewing_duration'] = [
            x.seconds for x in (dat['viewing_end_time'] - dat['viewing_start_time'])
        ]
        dat['program_time_at_start'] = dat['program_time_at_start']
        dat['id'] = [None for _ in range(len(dat))]

        dat = dat.where(pd.notnull(dat), None)

        self.raw_insert(
            self.Viewing,
            dat.filter(self.ViewingCols)
        )
        ### End of VIEWING

        self.dat = dat
        self.viewing_data = viewing_data

### INGNORE ###
# def testing():
#     date_str = '2017-04-02'
#     a = time()
#     for date_str in ['2017-03-01', '2017-04-02', '2017-05-01', '2017-05-17']:
#         file_loc = './data/%s/'%date_str
#         #date_str = '2017-04-03'
#         year, month, day = [int(x) for x in date_str.split('-')]
#         b = time()
#         im = VizioImporter(year, month, day)
#         print time() - b
#         files = []
#         for file_name in os.listdir(file_loc):
#             if file_name.find('historical.content') != -1:
#                 files.append(file_name)
#         files.sort()
#
#         for file_name in files:
#             print '##################### %s ###################'%file_name
#             b = time()
#             filepath = file_loc + file_name
#             im.import_file(filepath)
#             print time() - b, time() - a
### INGNORE ###

def import_historical(folder_names):
    #folder_name is in date string format - YYYY-MM-DD
    path = '/files2/Vizio/data/s3_download/vizio_unzipped/history/%s/'
    for date_str in folder_names:
        file_loc = path%date_str
        year, month, day = [int(x) for x in date_str.split('-')]
        im = VizioImporter(year, month, day)
        files = []
        for file_name in os.listdir(file_loc):
            if file_name.find('historical.content') != -1:
                files.append(file_name)
        files.sort()
        for file_name in files:
            print '##################### %s ###################'%file_name
            filepath = file_loc + file_name
            im.import_file(filepath)


def main(year, month, day, filepath):
    importer = VizioImporter(year, month, day)
    importer.import_file(filepath)

if __name__ == '__main__':
    # Make sure to change config.py file.
    if len(sys.argv) != 3:
        hist = raw_input('Run historical data import module? (y/n)')
        if hist.lower() == 'y':
            filepath = '/files2/Vizio/data/s3_download/vizio_unzipped/history'
            if len(sys.argv) >= 2:
                filepath = sys.argv[1]
            folder_names = []
            for folder_name in os.listdir(filepath):
                try:
                    datetime.strptime(folder_name, '%Y-%m-%d')
                    folder_names.append(folder_name)
                except ValueError:
                    continue
            folder_names.sort()
            import_historical(folder_names)
        else:
            print 'filepath and date string argument are required'
    else:
        filepath = sys.argv[1]
        date_str = sys.argv[2]
        year, month, day = [int(x) for x in date_str.split('-')]
        main(year, month, day, filepath)


#python data_import.py ./data/2017-05-17/historical.content.2017-05-17-07._0000_part_00 2017-05-17
