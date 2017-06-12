from config import Config
from time import time
import pandas as pd
import math
import os
import sys
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import bindparam
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
        self.Session = sessionmaker(bind=self.engine)
        self.Base    = declarative_base()

        # Date of the data
        self.year  = year
        self.month = month
        self.day   = day
        self.today  = date(year, month, day)

        # Tables
        self.Viewing     = VizioViewingFact(self.Base, self.year, self.month, self.day)
        self.Demographic = VizioDemographicDim(self.Base, self.year, self.month)
        self.Activity    = VizioActivityDim(self.Base)
        self.Location    = VizioLocationDim(self.Base)
        self.Network     = VizioNetworkDim(self.Base)
        self.Program     = VizioProgramDim(self.Base)
        self.Time        = VizioTimeDim(self.Base)

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
        zipcode_ref.zipcode = ["{:05d}".format(int(x)) if pd.isnull(x) == False else x for x in zipcode_ref.zipcode]
        zipcode_ref.columns = ['zipcode', 'timezone', 'tz_offset']
        zipcode_ref['zipcode_4'] = [x[:4] for x in zipcode_ref.zipcode]
        zipcode_ref['zipcode_3'] = [x[:3] for x in zipcode_ref.zipcode]
        zipcode_ref['zipcode_2'] = [x[:2] for x in zipcode_ref.zipcode]
        self.zipcode_ref = zipcode_ref

        # Network Table + Load reference csv file
        self.load_networks()
        self.call_signs_ref = pd.read_csv('./reference/vizio_to_fcc_callsign.csv')

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
                    key = datetime_str + ' ' + ':'.join(['{:02d}'.format(x) for x in [hr, minute, second]])
                    datetimes[key] = (datetime(self.year, self.month, self.day, hr, minute, second), current_slot)
                    time_slots[key[-8:]] = current_slot
        self.datetimes = datetimes
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

        for row in self.session.query(self.Demographic).options(load_only('id', 'household_id')):
            demographics.append(
                [row.id,
                 row.household_id]
            )

        demographics = pd.DataFrame(demographics, columns = ['demographic_key',
                                                             'household_id'])
        self.demographics = demographics


    @__db_session
    def load_activities(self):
        # id here should match the id of of demographic
        activities = []

        for row in self.session.query(self.Activity):
            activities.append(
                [row.id,
                 row.household_id,
                 row.last_active_date]
            )

        activities = pd.DataFrame(activities, columns = ['demographic_key',
                                                         'household_id',
                                                         'last_active_date'])
        self.activities = activities


    @__db_session
    def load_locations(self):
        # One zipcode sometimes have more than one dma, like null.
        # Use (zipcode, dma) for the mapping
        locations = []

        for row in self.session.query(self.Location).options(load_only('id', 'zipcode', 'dma')):
            locations.append(
                [row.id,
                 row.zipcode,
                 row.dma]
            )

        locations = pd.DataFrame(locations, columns = ['location_key',
                                                       'zipcode',
                                                       'dma'])
        self.locations = locations


    @__db_session
    def load_networks(self):
        # mapping with Call_sign for now.
        # With tms, station_id will be used instead.
        networks = []

        for row in self.session.query(self.Network).options(load_only('id', 'call_sign')):
            networks.append(
                [row.id,
                 row.call_sign]
             )

        networks = pd.DataFrame(networks, columns = ['network_key',
                                                     'call_sign'])
        self.networks = networks


    @__db_session
    def load_programs(self):
        # Oddly, One tms_id can have more than one start_time
        # (tms_id, program_name, program_start_tie) for mapping
        programs = []

        for row in self.session.query(self.Program):
            programs.append(
                [row.id,
                 row.tms_id,
                 row.program_name,
                 row.program_start_time
                 ]
             )

        programs = pd.DataFrame(programs,
                                columns = ['program_key',
                                           'tms_id',
                                           'program_name',
                                           'program_start_time'])
        self.programs = programs


    @__db_session
    def load_times(self):
        times = []
        for row in self.session.query(self.Time).options(load_only('id', 'time_slot', 'date')):
            times.append(
                [row.id,
                 (row.time_slot, row.date)
                 ]
             )

        times = pd.DataFrame(times, columns = ['time_key',
                                               'unique_time_id'])
        self.times = times


    @__db_session
    def core_insert(self, table_obj, list_of_dict, operation = None):
        # known to be the quickest implementation at least among salalchemy apis
        if operation == None:
            operation = table_obj.__table__.insert()
        for i in range(len(list_of_dict) / 50000):
            self.engine.execute(
                operation,
                list_of_dict[i*50000:(i+1)*50000]
            )
        self.engine.execute(
            table_obj.__table__.insert(),
            list_of_dict[(len(list_of_dict) / 50000)*50000:]
        )


    @__db_session
    def update_activity(self, list_of_dict):
        for i, row in enumerate(self.session.query(self.Activity).filter(self.Activity.id.in_(list_of_dict.id))):
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
                self.datetimes[datetime_str] = (datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S'),
                                                self.time_slots[datetime_str[-8:]])
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
        condition_1 = (viewing_data.viewing_start_time_minute < 30) & (viewing_data.viewing_end_time_minute >= 30)
        # condition 2 should actually be zero all the time. Vizio reports data in one hour interval
        condition_2 = (viewing_data.viewing_start_time_minute > 30) & (viewing_data.viewing_end_time_hour > viewing_data.viewing_start_time_hour)
        condition_3 = (condition_1 == False) & (condition_2 == False)

        extended_viewing_data = pd.DataFrame(columns = viewing_data.columns)
        viewing_data['temp_end'] = viewing_data['viewing_start_time'].copy()
        extended_viewing_data = pd.concat([extended_viewing_data, viewing_data[condition_3]], ignore_index=True)

        temp = viewing_data[condition_1].reset_index(drop=True).copy()
        temp['temp_end'] = temp.temp_end + pd.Series([timedelta(minutes = (30 - x.minute)) - timedelta(seconds = x.second) for x in temp.viewing_start_time])
        actual_end_time = temp.viewing_end_time.copy()
        temp['viewing_end_time'] = temp['temp_end']
        extended_viewing_data = pd.concat([extended_viewing_data, temp], ignore_index=True)
        temp['viewing_start_time'] = temp.viewing_end_time.copy()
        temp.viewing_end_time = actual_end_time
        extended_viewing_data = pd.concat([extended_viewing_data, temp], ignore_index=True)

        extended_viewing_data['time_slot'] = [self.time_slots.get(x.strftime('%H:%M:%S')) for x in extended_viewing_data['viewing_end_time']]
        extended_viewing_data['date'] = [x.date() for x in extended_viewing_data['viewing_end_time']]
        extended_viewing_data['day_of_week'] = [x.isoweekday() for x in extended_viewing_data['date']]
        extended_viewing_data['week'] = [x.isocalendar()[1] for x in extended_viewing_data['date']]
        extended_viewing_data['quarter'] = [math.ceil(x.month/3.0) for x in extended_viewing_data['date']]
        extended_viewing_data['viewing_duration'] = [x.seconds for x in (extended_viewing_data['viewing_end_time'] - extended_viewing_data['viewing_start_time'])]
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
        viewing_data.zipcode = ["{:05d}".format(int(x)) if pd.isnull(x) == False else x for x in viewing_data.zipcode]
        viewing_data.program_start_time = [x[:-1].replace('T', ' ') if x != 'null' else None for x in viewing_data.program_start_time]
        for col in ['program_start_time', 'viewing_start_time', 'viewing_end_time']:
            viewing_data[col] = pd.to_datetime(viewing_data[col])
        ### End of File Import

        ### ACTIVITY & DEMOGRAPHICS
        all_demographics = pd.DataFrame(viewing_data.household_id.unique(), columns = ['household_id'])
        in_activity_dim = pd.merge(all_demographics,
                                   self.activities,
                                   on = 'household_id',
                                   how = 'left',
                                   indicator=True)
        insert_to_activity_demo = in_activity_dim[(in_activity_dim['_merge'] == 'left_only')].copy()
        update_activity = in_activity_dim[(in_activity_dim['_merge'] != 'left_only')].copy()
        insert_to_demo = update_activity.loc[update_activity.demographic_key.isin(self.demographics.demographic_key)==False].copy()
        update_activity = self.activities.loc[(self.activities.demographic_key.isin(update_activity.demographic_key)) & \
                                              (self.activities.last_active_date != self.today)].copy()
        if len(insert_to_activity_demo) > 0:
            # if household_id IS NOT found in BOTH Activity_Dim table and Demographic_Dim_{month}
            insert_to_activity_demo['last_active_date'] = [self.today for _ in range(len(insert_to_activity_demo))]
            to_insert = insert_to_activity_demo.T.to_dict().values()
            self.core_insert(
                self.Activity,
                to_insert
            )
            start_idx = 1
            if len(self.activities):
                start_idx = int(self.activities.demographic_key.max() + 1)
            temp = pd.DataFrame([[start_idx + i, x['household_id'], x['last_active_date'] ] for i, x in enumerate(to_insert)],
                                 columns = ['demographic_key',
                                            'household_id',
                                            'last_active_date'])
            self.activities = pd.concat([self.activities, temp])

            temp = temp[['demographic_key', 'household_id']]
            self.demographics = pd.concat([self.demographics, temp])
            temp.columns = ['id', 'household_id']
            self.core_insert(
                self.Demographic,
                temp.T.to_dict().values(),
                operation = self.Demographic.__table__.insert().values((bindparam('id'), bindparam('household_id')))
            )

        if len(insert_to_demo) > 0:
            # if household_id IS found in Activity_Dim table, but NOT in Demographic_Dim_{month}
            to_insert = insert_to_demo[['demographic_key', 'household_id']]
            to_insert.columns = ['id', 'household_id']
            self.core_insert(
                self.Demographic,
                to_insert.T.to_dict().values(),
                operation = self.Demographic.__table__.insert().values((bindparam('id'), bindparam('household_id')))
            )
            to_insert.columns = ['demographic_key', 'household_id']
            self.demographics = pd.concat([self.demographics, to_insert], ignore_index=True)

        if len(update_activity) > 0:
            # if household_id IS found in BOTH Activity_Dim table and Demographic_Dim_{month}
            # Assuming that the current time inmport is the most recent data available.
            update_activity['last_active_date'] = [self.today for _ in range(len(update_activity))]
            to_update = update_activity[['demographic_key', 'household_id', 'last_active_date']]
            to_update.columns = ['id', 'household_id', 'last_active_date']
            self.update_activity(to_update)
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
                                 self.zipcode_ref[['zipcode', 'tz_offset', 'timezone']],
                                 on = 'zipcode',
                                 how='left')
        self.all_locations = all_locations

        # if zipcode not found in reference table, try to match first 4, 3, 2 digits respectively
        for idx in all_locations.loc[all_locations.timezone.isnull()].index:
            a = self.zipcode_ref.loc[self.zipcode_ref.zipcode_4 == all_locations.ix[idx].zipcode[:4],
                                        ['tz_offset', 'timezone']].head(1)
            if len(a) != 1:
                a = self.zipcode_ref.loc[self.zipcode_ref.zipcode_3 == all_locations.ix[idx].zipcode[:3],
                                            ['tz_offset', 'timezone']].head(1)
            if len(a) != 1:
                a = self.zipcode_ref.loc[self.zipcode_ref.zipcode_2 == all_locations.ix[idx].zipcode[:2],
                                            ['tz_offset', 'timezone']].head(1)
            if len(a):
                all_locations.ix[idx, 'tz_offset'] = a.tz_offset.values[0]
                all_locations.ix[idx,'timezone']   = a.timezone.values[0]
        all_locations = all_locations.where(pd.notnull(all_locations), None)
        self.all_locations = all_locations

        if len(all_locations) > 0:
            #print 'Locations to insert: ', len(all_locations)
            to_insert = all_locations.T.to_dict().values()
            self.core_insert(
                self.Location,
                to_insert
            )
            start_idx = 1
            if len(self.locations):
                start_idx = int(self.locations.location_key.max() + 1)
            temp = pd.DataFrame([[start_idx + i, x['zipcode'], x['dma']] for i, x in enumerate(to_insert)],
                                 columns = ['location_key'] + loc_cols)
            self.locations = pd.concat([self.locations, temp])
        ### End of LOCATIONS


        ### NETWORK
        all_networks = viewing_data.loc[viewing_data.call_sign.isin(self.networks.call_sign.unique())==False,
                                        ['call_sign']].drop_duplicates()
        all_networks = pd.merge(all_networks,
                                self.call_signs_ref[['call_sign', 'network_affiliate']],
                                on = 'call_sign',
                                how ='left')
        all_networks = all_networks.where(pd.notnull(all_networks), None)
        self.all_networks = all_networks

        if len(all_networks) > 0:
            #print 'Networks to insert: ', len(all_networks)
            to_insert = all_networks.T.to_dict().values()
            self.core_insert(
                self.Network,
                to_insert
            )
            start_idx = 1
            if len(self.networks):
                start_idx = int(self.networks.network_key.max() + 1)
            temp = pd.DataFrame([[start_idx + i, x['call_sign']] for i, x in enumerate(to_insert)],
                                 columns = ['network_key', 'call_sign'])
            self.networks = pd.concat([self.networks, temp])
        ### End of NETWORKS


        ### PROGRAMS
        program_cols = ['tms_id',
                        'program_name',
                        'program_start_time']
        temp = pd.merge(viewing_data[program_cols], self.programs[program_cols], how='left', indicator=True)['_merge'] == 'left_only'
        all_programs = viewing_data.loc[temp, program_cols].drop_duplicates()
        all_programs = all_programs.where(pd.notnull(all_programs), None)
        self.all_programs = all_programs

        if len(all_programs) > 0:
            #print 'Programs to insert: ', len(all_programs)
            to_insert = all_programs.loc[all_programs.program_start_time.isnull()==False].T.to_dict().values()
            for x in all_programs.loc[all_programs.program_start_time.isnull()].T.to_dict().values():
                x['program_start_time'] = None
                to_insert.append(x)
            self.core_insert(
                self.Program,
                to_insert
            )
            start_idx = 1
            if len(self.programs):
                start_idx = int(self.programs.program_key.max() + 1)
            temp = pd.DataFrame([[start_idx + i, x['tms_id'], x['program_name'], x['program_start_time']] for i, x in enumerate(to_insert)],
                                 columns = ['program_key'] + program_cols)
            self.programs = pd.concat([self.programs, temp])
        ### End of PROGRAMS


        ## Merge reference tables for the appropirate keys
        # demographic_key
        self.orig = viewing_data.copy()
        viewing_data = pd.merge(viewing_data,
                                self.demographics,
                                on ='household_id',
                                how = 'left')

        # location_key
        viewing_data = pd.merge(viewing_data,
                                self.locations,
                                on = ['zipcode', 'dma'],
                                how = 'left')

        # network_key
        viewing_data = pd.merge(viewing_data,
                                self.networks,
                                on ='call_sign',
                                how = 'left')

        # program_key
        viewing_data = pd.merge(viewing_data,
                                self.programs,
                                on = [ 'tms_id', 'program_name', 'program_start_time'],
                                how = 'left')

        ### Expand viewing data and place appropirate timeslots
        dat = self.extend_viewing_data(viewing_data)

        # sometims, these columns are interpreted as tuples
        for col in ['day_of_week', 'week']:
            try:
                dat[col]       = [x[0] for x in dat[col]]
            except IndexError:
                continue

        dat['unique_time_id'] = zip(dat.time_slot, dat.date)
        ### End of Expand viewing data


        ### TIMES
        time_cols = ['time_slot',
                     'date',
                     'day_of_week',
                     'week',
                     'quarter']
        all_times = dat[time_cols].drop_duplicates()
        all_times['unique_time_id'] = zip(all_times.time_slot, all_times.date)
        all_times = all_times.loc[all_times.unique_time_id.isin(self.times.unique_time_id.unique())==False,
                                  time_cols].drop_duplicates()
        all_times = all_times.where(pd.notnull(all_times), None)
        self.all_times = all_times

        if len(all_times) > 0:
            #print 'Times to insert: ', len(all_times)
            self.core_insert(
                self.Time,
                all_times.T.to_dict().values()
            )
            self.load_times()
        ### End of TIMES


        ### VIEWING
        dat = pd.merge(dat, self.times, on = 'unique_time_id', how = 'left')
        dat['viewing_duration'] = [x.seconds for x in (dat['viewing_end_time'] - dat['viewing_start_time'])]
        dat['program_time_at_start'] = dat['program_time_at_start']
        view_data_cols = ['demographic_key',
                          'location_key',
                          'network_key',
                          'program_key',
                          'program_time_at_start',
                          'viewing_duration',
                          'viewing_end_time',
                          'viewing_start_time',
                          'time_key']
        dat = dat.where(pd.notnull(dat), None)

        self.core_insert(
            self.Viewing,
            dat[view_data_cols].T.to_dict().values()
        )
        ### End of VIEWING

        self.dat = dat
        self.viewing_data = viewing_data

### INGNORE ###
def testing():
    date_str = '2017-04-02'
    for date_str in ['2017-03-01', '2017-04-02', '2017-05-01', '2017-05-17']:
        a = time()
        file_loc = './data/%s/'%date_str
        #date_str = '2017-04-03'
        year, month, day = [int(x) for x in date_str.split('-')]
        b = time()
        im = VizioImporter(year, month, day)
        print time() - b
        files = os.listdir(file_loc)
        files.sort()
        for file_name in files[:10]:
            print '##################### %s ###################'%file_name
            b = time()
            filepath = file_loc + file_name
            im.import_file(filepath)
            print time() - b, time() - a
### INGNORE ###

def main(year, month, day, filepath):
    importer = VizioImporter(year, month, day)
    importer.import_file(filepath)

if __name__ == '__main__':
    #testing()
    # Make sure to change config.py file.
    if len(sys.argv) != 3:
        print 'filepath and date string argument are required'
    else:
        filepath = sys.argv[1]
        date_str = sys.argv[2]
        year, month, day = [int(x) for x in date_str.split('-')]
        main(year, month, day, filepath)


#python data_import.py ./data/2017-05-17/historical.content.2017-05-17-07._0000_part_00 2017-05-17
