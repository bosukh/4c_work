from time import time
import pandas as pd
import math
import os
import sys
from datetime import datetime, date, timedelta
from vizio_db_connection import VizioDBConnection
from local_logger import LocalLogger

logger = LocalLogger(
            logger_name = __name__,
            logfile = 'vizio_data_import_{0}.log'.format(
                datetime.today().strftime(LocalLogger.date_suffix_fmt)
            )
        ).logger

class VizioImporter(VizioDBConnection):
    # 1. Initiate class by VizioImporter(year, month, day)
    # 2. use import_file mothod to import each file

    def extend_viewing_data(self, viewing_data):
        # Split viewing_data to fit into time slots
        # Since Vizio data is reported hourly, only need to concern with
        # the rows that falls on 30 minutes
        for col in ['viewing_start_time', 'viewing_end_time']:
            temp = pd.DataFrame(
                [[x.hour, x.minute, x.second] for x in viewing_data[col]],
                columns = ['hr', 'min', 'sec']
            )
            viewing_data[col + '_hour']   = temp['hr'].copy()
            viewing_data[col + '_minute'] = temp['min'].copy()
            viewing_data[col + '_second'] = temp['sec'].copy()

        condition_1 = (
            (viewing_data.viewing_start_time_minute < 30) &
            (viewing_data.viewing_end_time_minute >= 30)
        )
        # condition 2 should actually be zero all the time.
        # because Vizio reports data in one hour interval
        condition_2 = (
            (viewing_data.viewing_start_time_minute > 30) &
            (viewing_data.viewing_end_time_hour > viewing_data.viewing_start_time_hour)
        )
        condition_3 = (condition_1 == False) & (condition_2 == False)

        extended_viewing_data = pd.DataFrame(columns = viewing_data.columns)
        # ones that do not fall in 30 min clock, no need to split, just add.
        extended_viewing_data = pd.concat(
            [extended_viewing_data,
             viewing_data[condition_3]],
            ignore_index=True
        )

        # Ones that need to be splitted
        viewing_data['temp_end'] = viewing_data['viewing_start_time'].copy()
        temp = viewing_data[condition_1].reset_index(drop = True).copy()
        temp['temp_end'] = (
            temp.temp_end # it is now just a start time.
            + pd.Series([timedelta(minutes = (30 - x.minute))
                           - timedelta(seconds = x.second)
                           for x in temp.viewing_start_time])
        )
        actual_end_time = temp.viewing_end_time.copy()
        temp['viewing_end_time'] = temp['temp_end']
        extended_viewing_data = pd.concat([extended_viewing_data, temp],
                                          ignore_index=True)

        # Adjust offset
        temp['program_time_at_start'] = temp['program_time_at_start']  + pd.Series([
            x.seconds * 1000 for x in (temp['viewing_end_time']
                                       - temp['viewing_start_time'])
        ])
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
        logger.info(
            'Spliting Viewing_Data. Original %s -> Splitted %s rows'%(
                    str(len(viewing_data)),
                    str(len(extended_viewing_data))
                )
            )
        return extended_viewing_data


    def import_file(self, filepath):

        def __insertion_log(rows, table_name):
            # Just do not want to repeat this over and over..
            logger.info(
                'Inserting {rows} rows to {table_name}'.format(
                    rows = rows,
                    table_name = table_name
                )
            )

        logger.info('Start importing - %s'%filepath)
        # reset threads list
        self.threads = []

        ### File Import
        if not os.path.isfile(filepath):
            logger.error('%s - not found '%filepath)
            raise IOError('%s - not found.'%filepath)

        # columns given in the Vizio data
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
        viewing_data = pd.read_csv(filepath,
                                   names = columns,
                                   header = None,
                                   na_values = ['', 'null'])
        #viewing_data = pd.read_csv(filepath, names = columns, na_values = ['', 'null'])
        viewing_data.zipcode = [
            "{:05d}".format(int(x)) if pd.isnull(x) == False else None
            for x in viewing_data.zipcode
        ]
        viewing_data.program_start_time = [
            x[:-1].replace('T', ' ') if pd.isnull(x) == False else None
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
            indicator = True
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
            (self.activities.last_active_date < self.current_date)
        ].copy()

        if len(insert_to_activity_demo) > 0:
            # if household_id IS NOT found in BOTH Activity_Dim table and Demographic_Dim_{month}
            __insertion_log(len(insert_to_activity_demo),
                            self.Activity.__tablename__)
            __insertion_log(len(insert_to_activity_demo),
                            self.Demographic.__tablename__)
            start_idx = 1
            if len(self.activities):
                start_idx = int(self.activities.id.max() + 1)
            insert_to_activity_demo['last_active_date'] = [
                self.current_date for _ in range(len(insert_to_activity_demo))
            ]
            insert_to_activity_demo['id'] = range(start_idx,
                                                  start_idx + len(insert_to_activity_demo))
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
            __insertion_log(len(insert_to_demo),
                            self.Demographic.__tablename__)
            insert_to_demo = insert_to_demo.reset_index(drop=True)
            insert_to_demo.id = insert_to_demo.id.astype(int)
            #insert_to_demo = insert_to_demo.sort_values('id')
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
            logger.info(
                'Updating {rows} rows in {table_name}'.format(
                    rows = len(update_activity),
                    table_name = self.Activity.__tablename__
                )
            )
            update_activity['last_active_date'] = [
                self.current_date for _ in range(len(update_activity))
            ]
            self.raw_update_activity(update_activity[['id', 'last_active_date']])
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
            __insertion_log(len(all_locations),
                            self.Location.__tablename__)
            start_idx = 1
            if len(self.locations):
                start_idx = int(self.locations.id.max() + 1)
            all_locations['id'] = range(start_idx, start_idx + len(all_locations))
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
            __insertion_log(len(all_networks),
                            self.Network.__tablename__)
            start_idx = 1
            if len(self.networks):
                start_idx = int(self.networks.id.max() + 1)
            all_networks['id'] = range(start_idx, start_idx + len(all_networks))
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
        all_programs.dropna(subset = ['tms_id'])
        self.all_programs = all_programs

        if len(all_programs) > 0:
            __insertion_log(len(all_programs),
                            self.Program.__tablename__)
            start_idx = 1
            if len(self.programs):
                start_idx = int(self.programs.id.max() + 1)
            all_programs['id'] = range(start_idx, start_idx + len(all_programs))
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
        # demographic_key
        temp = self.demographics.rename(index = str,
                                        columns = {'id':'demographic_key'})
        viewing_data = pd.merge(viewing_data,
                                temp,
                                on ='household_id',
                                how = 'left')
        if viewing_data.demographic_key.isnull().sum():
            logger.error('Missing household_id in %s'%filepath)
            raise ValueError('Missing demographic_key')

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
            __insertion_log(len(all_times),
                            self.Time.__tablename__)
            start_idx = 1
            if len(self.times):
                start_idx = int(self.times.id.max() + 1)
            all_times['id'] = range(start_idx, start_idx + len(all_times))
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
        dat = dat.where(pd.notnull(dat), None)

        __insertion_log(len(dat),
                        self.Viewing.__tablename__)
        self.raw_insert(
            self.Viewing,
            dat.filter(self.ViewingCols)
        )
        ### End of VIEWING

        for thread in self.threads:
            thread.join()
        self.clean_up_temp()

        logger.info('Finished importing - %s'%filepath)
        self.update_fileinfo(filepath,
                             imported_date = datetime.now())

### INGNORE ###
def testing():
    a = time()
    for date_str in ['2017-03-01', '2017-03-02', '2017-04-21', '2017-04-22', '2017-05-11', '2017-05-12', '2017-05-20']:
        file_loc = './data/%s/'%date_str
        #date_str = '2017-04-03'
        year, month, day = [int(x) for x in date_str.split('-')]
        b = time()
        im = VizioImporter(year, month, day)
        print time() - b
        files = []
        for file_name in os.listdir(file_loc):
            files.append(file_name)
        files.sort()

        for file_name in files[:2]:
            print '##################### %s ###################'%file_name
            b = time()
            filepath = file_loc + file_name
            im.import_file(filepath)
            print time() - b, time() - a
            if im.demographics.household_id.isnull().sum() + (im.demographics.household_id == '').sum() > 0:
                im.demographics.to_csv('demo_problem_%s.csv'%file_name, index=False)

### INGNORE ###
def import_historical(folder_names):
    #folder_name is in date string format - YYYY-MM-DD
    path = '/files2/Vizio/data/s3_download/vizio_unzipped/history/%s/'
    timeit = pd.DataFrame()
    for date_str in folder_names:
        time_lst = []
        g_start = time()
        file_loc = path%date_str
        year, month, day = [int(x) for x in date_str.split('-')]
        im = VizioImporter(year, month, day)
        files = []
        for file_name in os.listdir(file_loc):
            if file_name.find('historical.content') != -1:
                files.append(file_name)
        files.sort()
        time_lst.append(time() - g_start)

        for file_name in files:
            start = time()
            print '##################### %s ###################'%file_name
            filepath = file_loc + file_name
            im.import_file(filepath)
            time_lst.append(time() - start)

        timeit[date_str] = pd.Series(time_lst)
        timeit.to_csv('performance.csv')
    timeit.to_csv('performance.csv')

def main(year, month, day, filepath):
    importer = VizioImporter(year, month, day)
    importer.import_file(filepath)

if __name__ == '__main__':
    # Make sure to change config.py file.
    testing()
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
