from sqlalchemy import Column, Integer, String, DateTime, Interval
from sqlalchemy.dialects.mysql import TINYINT, TIMESTAMP, DATETIME, DATE, INTEGER

class VizioViewingFactMixin():
    id                    = Column(Integer, primary_key=True, autoincrement=True)
    demographic_key       = Column(Integer, nullable=False)
    location_key          = Column(Integer, nullable=True)
    network_key           = Column(Integer, nullable=True)
    program_key           = Column(Integer, nullable=True)
    time_key              = Column(Integer, nullable=False)
    program_time_at_start = Column(Integer, nullable=False) # milliseconds
    viewing_start_time    = Column(TIMESTAMP, nullable=False, index=True)
    viewing_end_time      = Column(TIMESTAMP, nullable=False, index=True)
    viewing_duration      = Column(Integer, nullable=False) # seconds


class VizioDemographicDimMixin():
    id                    = Column(Integer, primary_key=True, autoincrement=False)
    household_id          = Column(String(250), nullable=False)


class VizioActivityDimMixin():
    # Activity Id will match the Ids in DemographicDim
    id                    = Column(Integer, primary_key=True, autoincrement=True)
    household_id          = Column(String(250), nullable=False)
    last_active_date      = Column(DATE, nullable=False)


class VizioLocationDimMixin():
    id                    = Column(Integer, primary_key=True, autoincrement=True)
    zipcode               = Column(String(10), nullable=False)
    dma                   = Column(String(128), nullable=False) # dma_name
    timezone              = Column(String(30), nullable=True)
    tz_offset             = Column(TINYINT, nullable=True) # hours


class VizioNetworkDimMixin():
    id                    = Column(Integer, primary_key=True, autoincrement=True)
    call_sign             = Column(String(20), nullable=False)
    station_id            = Column(Integer, nullable=True) # tms
    station_dma            = Column(String(128), nullable=True) # dma_name
    station_name          = Column(String(250), nullable=True)
    network_affiliate     = Column(String(20), nullable=True)


class VizioProgramDimMixin():
    id                    = Column(Integer, primary_key=True, autoincrement=True)
    root_id               = Column(Integer, nullable=True) # tms
    series_id             = Column(Integer, nullable=True) # tms
    tms_id                = Column(String(250), nullable=True)
    program_name          = Column(String(250), nullable=False)
    program_start_time    = Column(DATETIME, nullable=True)
    # Datetime instead of Timestamp to allow null
    program_duration      = Column(Integer, nullable=True)


class VizioTimeDimMixin():
    id                    = Column(Integer, primary_key=True, autoincrement=True)
    time_slot             = Column(TINYINT, nullable=False) # 1-48
    date                  = Column(DATE, nullable=False)
    day_of_week           = Column(TINYINT, nullable=False)
    week                  = Column(TINYINT, nullable=False)
    quarter               = Column(TINYINT, nullable=False)
