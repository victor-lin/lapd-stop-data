from pandas import read_csv, to_datetime
from pandas.tslib import Timestamp
from numpy import int64, float64

from collections import OrderedDict
from cx_Oracle import connect, makedsn
from contextlib import closing
from . import app


log = app.logger

INSERT_STR = 'INSERT INTO {} VALUES ({})'


def connect_db():
    """Return cx_Oracle connection object"""
    log.info('Connecting...')
    return connect(user=app.config['UNAME'],
                   password=app.config['PASSWD'],
                   dsn=makedsn(host=app.config['ORACLE_SERVER'],
                               port=app.config['ORACLE_PORT'],
                               sid=app.config['ORACLE_SID']))


def init_db():
    """Create database schema and insert rows"""
    log.info('Creating database schema')
    schema_string = "CREATE SCHEMA AUTHORIZATION {} {}"
    with connect_db() as con:
        with closing(con.cursor()) as cur:
            with app.open_resource('schema.sql') as f:
                cur.execute(schema_string.format(app.config['UNAME'],
                                                 f.read()))
            df_raw = read_csv(app.config['DATA'])
            df_raw['STOP_DATE'] = to_datetime(df_raw.STOP_DT + df_raw.STOP_TM,
                                              format='%m/%d/%Y%H:%M')
            for statement in get_insert_statements_stop(df_raw):
                cur.execute(statement)
        con.commit()


def convert_val(val):
    if type(val) is Timestamp:
        date = val.strftime('%m/%d/%Y %I:%M')
        return "TO_DATE('{}', 'mm/dd/yyyy hh:mi')".format(date)
    if type(val) is float64:
        return str(int(val))
    if type(val) is int64:
        return str(val)
    return "'{}'".format(val)


def get_insert_statements_stop(df_raw):
    """Return insert statement strings for PoliceStop table"""
    stop_dict = OrderedDict([('STOP_ID', 'FORM_REF_NBR'),
                             ('STOP_DATE', 'STOP_DATE'),
                             ('STOP_TYPE', 'STOP_TYPE'),
                             ('POST_STOP_ACTIVITY', 'POST_STOP_ACTV_IND'),
                             ('OFFICER1_ID', 'OFCR1_SERL_NBR'),
                             ('OFFICER2_ID', 'OFCR2_SERL_NBR')])
    df_stop = df_raw.groupby('FORM_REF_NBR').first().reset_index()[stop_dict.values()]
    df_stop.columns = stop_dict.keys()
    for i, row in df_stop.iterrows():
        row_vals = [convert_val(val) for val in row]
        yield INSERT_STR.format('PoliceStop', ','.join(row_vals))


def get_insert_statements_officer(df_raw):
    pass


def get_insert_statements_offender(df_raw):
    pass


def drop_tables():
    """Drop tables created for this project"""
    with connect_db() as con:
        with closing(con.cursor()) as cur:
            log.info('Dropping table "Offender"')
            cur.execute('DROP TABLE Offender')
            log.info('Dropping table "PoliceStop"')
            cur.execute('DROP TABLE PoliceStop')
            log.info('Dropping table "Officer"')
            cur.execute('DROP TABLE Officer')
        con.commit()
