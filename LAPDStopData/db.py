from pandas import read_csv, to_datetime
from pandas.tslib import Timestamp
from numpy import int64, float64, isnan

from collections import OrderedDict
from cx_Oracle import connect, makedsn, DatabaseError
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


def create_schema():
    """Create database schema"""
    with connect_db() as con:
        with closing(con.cursor()) as cur:
            with app.open_resource('schema.sql') as f:
                log.info('Creating database schema')
                schema_string = "CREATE SCHEMA AUTHORIZATION {} {}"
                try:
                    cur.execute(schema_string.format(app.config['UNAME'],
                                                     f.read()))
                except DatabaseError as db_error:
                    log.error(db_error)
        con.commit()


def populate(table):
    """Insert rows for specified table"""
    log.info('Inserting rows into table %s', table)
    with connect_db() as con:
        with closing(con.cursor()) as cur:
            log.info('Parsing data file...')
            df_raw = read_csv(app.config['DATA'])
            df_raw['STOP_DATE'] = to_datetime(df_raw.STOP_DT + df_raw.STOP_TM,
                                              format='%m/%d/%Y%H:%M')
            if table == 'Officer':
                statements = get_insert_statements_officer(df_raw)
            elif table == 'PoliceStop':
                statements = get_insert_statements_stop(df_raw)
            elif table == 'Offender':
                statements = get_insert_statements_offender(df_raw)
            else:
                raise Exception('{} not a valid table name'.format(table))
            log.info('Generating statements...')
            statements = list(statements)
            for i, statement in enumerate(statements):
                log.debug('Executing (%i/%i): "%s"',
                          i, len(statements), statement)
                cur.execute(statement)
        con.commit()


def convert_val(val):
    """Return Oracle-friendly string based on data type"""
    if type(val) is Timestamp:
        date = val.strftime('%m/%d/%Y %I:%M')
        return "TO_DATE('{}', 'mm/dd/yyyy hh:mi')".format(date)
    if type(val) in {float64, float}:
        if isnan(val):
            return 'NULL'
        return str(int(val))
    if type(val) is int64:
        return str(val)
    return "'{}'".format(val)


def get_insert_statements_officer(df_raw):
    """Return insert statements for Officer table"""
    officer_cols = OrderedDict([('OFFICER_ID', ('OFCR1_SERL_NBR',
                                                'OFCR2_SERL_NBR')),
                                ('DIV_ID', ('OFCR1_DIV_NBR',
                                            'OFCR2_DIV_NBR')),
                                ('DIV_NAME', ('DIV1_DESC',
                                              'DIV2_DESC'))])
    df_officer1 = df_raw.groupby('OFCR1_SERL_NBR').first().reset_index()[[o1 for o1, o2 in officer_cols.values()]]
    df_officer1.columns = officer_cols.keys()
    df_officer2 = df_raw.groupby('OFCR2_SERL_NBR').first().reset_index()[[o2 for o1, o2 in officer_cols.values()]]
    df_officer2.columns = officer_cols.keys()
    df_all = df_officer1.append(df_officer2)
    df_all = df_all.groupby('OFFICER_ID').first().reset_index()[officer_cols.keys()]
    for i, row in df_all.iterrows():
        row_vals = [convert_val(val) for val in row]
        yield INSERT_STR.format('Officer', ','.join(row_vals))


def get_insert_statements_stop(df_raw):
    """Return insert statements for PoliceStop table"""
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


def get_insert_statements_offender(df_raw):
    """Return insert statements for PoliceStop table"""
    pass


def drop_tables():
    """Drop tables created for this project"""
    with connect_db() as con:
        with closing(con.cursor()) as cur:
            try:
                log.info('Dropping table "Offender"')
                cur.execute('DROP TABLE Offender')
            except DatabaseError as db_error:
                log.error(db_error)
            try:
                log.info('Dropping table "PoliceStop"')
                cur.execute('DROP TABLE PoliceStop')
            except DatabaseError as db_error:
                log.error(db_error)
            try:
                log.info('Dropping table "Officer"')
                cur.execute('DROP TABLE Officer')
            except DatabaseError as db_error:
                log.error(db_error)
        con.commit()
