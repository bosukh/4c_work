from config import Config
from time import time
import pandas as pd
import numpy as np
import math
import os
import sys
import threading
from uuid import uuid4
from datetime import datetime, date, timedelta
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, load_only
from vizio_models import VizioViewingFact, VizioDemographicDim, VizioLocationDim, \
                         VizioNetworkDim, VizioProgramDim, VizioTimeDim, \
                         VizioActivityDim, VizioFileInfo

class VizioDBConnection(object):
    # 1. Initiate class by VizioDBConnection(year, month, day)

    def __init__(self, year, month, day, load_references = True):
        # dynamic list of threads that will interact with different tables
        self.threads = []

        # create temp folder to save files
        if not os.path.isdir('./vizio_temp'):
            os.mkdir('vizio_temp')

        # SQLalchemy initializtion
        self.config  = Config()
        self.engine  = create_engine(
            "mysql+mysqldb://{user}:{password}@{host}:{port}/{database}".format(
                **self.config.CONNECTIONS['vizio']
            )
        )
        self.Session = sessionmaker(bind = self.engine)
        self.Base    = declarative_base()

        # Date of the data
        self.year          = year
        self.month         = month
        self.day           = day
        self.current_date  = date(year, month, day)

        # Tables
        self.Viewing     = VizioViewingFact(self.Base, self.year,
                                            self.month, self.day)
        self.Demographic = VizioDemographicDim(self.Base, self.year, self.month)
        self.Activity    = VizioActivityDim(self.Base)
        self.Location    = VizioLocationDim(self.Base)
        self.Network     = VizioNetworkDim(self.Base)
        self.Program     = VizioProgramDim(self.Base)
        self.Time        = VizioTimeDim(self.Base)
        self.FileInfo    = VizioFileInfo(self.Base)

        # Columns
        self.ViewingCols     = [col.key for col in self.Viewing.__table__.c]
        self.DemographicCols = [col.key for col in self.Demographic.__table__.c]
        self.ActivityCols    = [col.key for col in self.Activity.__table__.c]
        self.LocationCols    = [col.key for col in self.Location.__table__.c]
        self.NetworkCols     = [col.key for col in self.Network.__table__.c]
        self.ProgramCols     = [col.key for col in self.Program.__table__.c]
        self.TimeCols        = [col.key for col in self.Time.__table__.c]
        self.FileInfoCols    = [col.key for col in self.FileInfo.__table__.c]

        # Initialize tables
        self.Base.metadata.create_all(self.engine, checkfirst=True)

        # Initialize reference Tables
        if load_references:
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
            "{:05d}".format(int(x)) if pd.isnull(x) == False else x
            for x in zipcode_ref.zipcode
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
        self.call_signs_ref.columns = ['station_type',
                                       'station_dma',
                                       'network_affiliate',
                                       'call_sign',
                                       'station_name']
        self.call_signs_ref = self.call_signs_ref.drop_duplicates()

        # Program Table
        self.load_programs()

        # Time Table
        self.load_times()

        # Fileinfo Table
        self.load_fileinfo()

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

    ######### QUERIES to load lookup tables to match keys to metadata #########
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
        demographics.id = demographics.id.astype(int)
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
        activities.id = activities.id.astype(int)
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
        locations.id = locations.id.astype(int)
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
        networks.id = networks.id.astype(int)
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
        programs.id = programs.id.astype(int)
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
        times.id = times.id.astype(int)
        self.times = times


    @__db_session
    def load_fileinfo(self, file_name = None):
        fileinfo = []
        query = self.session.query(self.FileInfo)
        if file_name:
            query = query.filter_by(file_name = file_name)
        for row in query:
            fileinfo.append([row.id,
                             row.file_name,
                             row.data_date,
                             row.downloaded_date,
                             row.imported_date,
                             row.revised_date])

        fileinfo = pd.DataFrame(fileinfo,
                                 columns = ['id',
                                            'file_name',
                                            'data_date',
                                            'downloaded_date',
                                            'imported_date',
                                            'revised_date'])
        fileinfo.id = fileinfo.id.astype(int)
        if file_name and len(self.fileinfo):
            if len(self.fileinfo.loc[self.fileinfo.file_name == file_name]):
                for col in fileinfo.columns:
                    if col in self.fileinfo:
                        self.fileinfo.loc[self.fileinfo.file_name == file_name, col] = fileinfo[col].values[0]
                        fileinfo = self.fileinfo
            else:
                fileinfo = pd.concat([self.fileinfo, fileinfo])
        self.fileinfo = fileinfo

    ######### END of QUERIES  #########

    ######### Insertion modules #########
    def raw_insert(self, table_obj, pd_df):
        self.to_thread(self.raw_insert_func, table_obj, pd_df)


    def raw_insert_func(self, table_obj, pd_df):
        # use external shell script to do the insertion.
        def __put_placeholder(pd_df, columns):
            for col in columns:
                if col not in pd_df.columns:
                    pd_df[col] = [None for _ in range(len(pd_df))]
            return pd_df[columns]
        table_name = table_obj.__tablename__
        table_cols = [col.key for col in table_obj.__table__.c]
        filepath = './vizio_temp/%s_to_insert'%table_name
        unique_filepath = self.to_csv(__put_placeholder(pd_df, table_cols),
                                       filepath)
        os.system(
            './vizio_data_import_script.sh {file_name} {table_name}'.format(
                file_name  = unique_filepath,
                table_name = table_name)
        )
    ######### End of Insertion modules #########

    ######### Update activity modules #########
    def raw_update_activity(self, pd_df):
        self.to_thread(self.raw_update_activity_func, pd_df)


    def raw_update_activity_func(self, pd_df):
        unique_filepath = self.to_csv(pd_df, './vizio_temp/activity_to_update')
        os.system(
            './vizio_activity_update_script.sh {file_name}'.format(
                file_name = unique_filepath)
        )
    ######### End of Update activity modules #########

    ######### Update fileinfo module #########
    @__db_session
    def update_fileinfo(self, filepath, **kwargs):
        _, file_name = os.path.split(filepath)
        if file_name in self.fileinfo.file_name.unique():
            stmt = self.FileInfo.__table__.update(). \
                    where(self.FileInfo.__table__.c.file_name == file_name). \
                    values(**kwargs)
            self.session.execute(stmt)
        else:
            new_fileinfo = self.FileInfo(
                file_name = file_name,
                data_date = self.current_date,
                **kwargs
            )
            self.session.add(new_fileinfo)
        self.session.commit()
        self.load_fileinfo(file_name = file_name)

    ######### End of Update fileinfo module #########

    ######### Utilities #########
    def to_thread(self, target, *args):
        t = threading.Thread(
                target = target,
                args = args
            )
        t.start()
        self.threads.append(t)


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


    def to_csv(self, pd_df, filepath):
        # save locally to be used by shell script to run file upload to database.
        unique_filepath = filepath + '_' + uuid4().hex
        pd_df.to_csv(unique_filepath,
                     index = False,
                     header = False,
                     sep = '^',
                     na_rep = '\N')
        return unique_filepath


    def clean_up_temp(self):
        for file_name in os.listdir('./vizio_temp'):
            try:
                os.remove('./vizio_temp/' + file_name)
            except Exception as e:
                # most likely permission error, but wouldn't happen if run in home directory
                pass
    ######### End of Utilities #########
