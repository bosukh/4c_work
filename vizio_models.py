from sqlalchemy.schema import ForeignKeyConstraint, UniqueConstraint
from vizio_table_mixin import VizioViewingFactMixin, VizioDemographicDimMixin, \
                              VizioLocationDimMixin, VizioNetworkDimMixin, \
                              VizioProgramDimMixin, VizioTimeDimMixin, \
                              VizioActivityDimMixin, VizioFileInfoMixin


def VizioViewingFact(Base, year, month, day):
    month = "{:02d}".format(month)
    day = "{:02d}".format(day)

    ## Class declaration
    class VizioViewingFactObj(VizioViewingFactMixin, Base):

        __tablename__ = 'vizio_viewing_fact_{year}_{month}_{day}'.format(year=year, month=month, day=day)
        __table_args__ = (
            ForeignKeyConstraint(
                ['demographic_key'],
                ['vizio_demographic_dim_{year}_{month}.id'.format(year=year, month=month)]
            ),
            ForeignKeyConstraint(
                ['location_key'],
                ['vizio_location_dim.id']
            ),
            ForeignKeyConstraint(
                ['network_key'],
                ['vizio_network_dim.id']
            ),
            ForeignKeyConstraint(
                ['program_key'],
                ['vizio_program_dim.id']
            ),
            ForeignKeyConstraint(
                ['time_key'],
                ['vizio_time_dim.id']
            )
        )
    ## end of Class declaration

    return VizioViewingFactObj


def VizioDemographicDim(Base, year, month):
    month = "{:02d}".format(month)

    ## Class declaration
    class VizioDemographicDimObj(VizioDemographicDimMixin, Base):
        __tablename__ = 'vizio_demographic_dim_{year}_{month}'.format(year=year, month=month)
    ## end of Class declaration

    return VizioDemographicDimObj


def VizioActivityDim(Base):

    ## Class declaration
    class VizioActivityDimObj(VizioActivityDimMixin, Base):
        __tablename__ = 'vizio_activity_dim'
    ## end of Class declaration

    return VizioActivityDimObj


def VizioLocationDim(Base):

    ## Class declaration
    class VizioLocationDimObj(VizioLocationDimMixin, Base):

        __tablename__ = 'vizio_location_dim'
        __table_args__ = (
            UniqueConstraint('zipcode', 'dma'),
        )
    ## end of Class declaration

    return VizioLocationDimObj


def VizioNetworkDim(Base):

    ## Class declaration
    class VizioNetworkDimObj(VizioNetworkDimMixin, Base):
        __tablename__ = 'vizio_network_dim'
        __table_args__ = (
            UniqueConstraint('call_sign', 'network_affiliate'),
        )
    ## end of Class declaration

    return VizioNetworkDimObj


def VizioProgramDim(Base):

    ## Class declaration
    class VizioProgramDimObj(VizioProgramDimMixin, Base):
        __tablename__ = 'vizio_program_dim'
    ## end of Class declaration

    return VizioProgramDimObj


def VizioTimeDim(Base):

    ## Class declaration
    class VizioTimeDimObj(VizioTimeDimMixin, Base):
        __tablename__ = 'vizio_time_dim'
        __table_args__ = (
            UniqueConstraint('time_slot', 'date'),
        )
    ## end of Class declaration

    return VizioTimeDimObj


def VizioFileInfo(Base):

    ## Class declaration
    class VizioFileInfoObj(VizioFileInfoMixin, Base):
        __tablename__ = 'vizio_fileinfo'
        __table_args__ = (
            UniqueConstraint('file_name', ),
        )
    ## end of Class declaration

    return VizioFileInfoObj
