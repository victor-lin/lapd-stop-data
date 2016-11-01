from pandas import read_csv, to_datetime
from numpy import float64, isnan

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
        cur = con.cursor()
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
        cur = con.cursor()
        log.info('Parsing data file...')
        df_raw = read_csv(app.config['DATA'])
        df_raw['STOP_DATE'] = to_datetime(df_raw.STOP_DT + df_raw.STOP_TM,
                                          format='%m/%d/%Y%H:%M')
        log.info('Getting insert parameters...')
        if table == 'Officer':
            num_cols = 3
            params = get_insert_params_officer(df_raw)
        elif table == 'PoliceStop':
            num_cols = 6
            params = get_insert_params_stop(df_raw)
        elif table == 'Offender':
            num_cols = 4
            params = get_insert_params_offender(df_raw)
        else:
            raise Exception('{} not a valid table name'.format(table))
        log.info('Preparing statements...')
        cur.prepare(INSERT_STR.format(table,
                                      ','.join([':' + str(x + 1)
                                                for x in range(num_cols)])))
        log.info('Executing statements...')
        cur.executemany(None, list(params))
        log.info('Committing DB')
        con.commit()


def add_constraints():
    """Add PK and FK constraints"""
    with connect_db() as con:
        cur = con.cursor()
        with app.open_resource('constraints.sql') as f:
            log.info('Creating database schema')
            statements = f.read().split(';')[:-1]
            for statement in statements:
                log.debug('Executing: "%s"', statement)
                try:
                    cur.execute(statement)
                except DatabaseError as db_error:
                    log.error(db_error)
        con.commit()


def convert_val(val):
    """Return value based on data type"""
    if type(val) in {float64, float}:
        if isnan(val):
            return None
        return int(val)
    return val


def get_insert_params_officer(df_raw):
    """Return ordered iterable of values for each row in Officer"""
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
        yield [convert_val(val) for val in row]


def get_insert_params_stop(df_raw):
    """Return ordered iterable of values for each row in PoliceStop"""
    stop_cols = OrderedDict([('STOP_ID', 'FORM_REF_NBR'),
                             ('STOP_DATE', 'STOP_DATE'),
                             ('STOP_TYPE', 'STOP_TYPE'),
                             ('POST_STOP_ACTIVITY', 'POST_STOP_ACTV_IND'),
                             ('OFFICER1_ID', 'OFCR1_SERL_NBR'),
                             ('OFFICER2_ID', 'OFCR2_SERL_NBR')])
    df_stop = df_raw.groupby('FORM_REF_NBR').first().reset_index()[stop_cols.values()]
    df_stop.columns = stop_cols.keys()
    for i, row in df_stop.iterrows():
        yield [convert_val(val) for val in row]


def get_insert_params_offender(df_raw):
    """Return ordered iterable of values for each row in Offender"""
    offender_cols = OrderedDict([('OFFENDER_ID', 'STOP_NBR'),
                                 ('GENDER', 'PERSN_GENDER_CD'),
                                 ('ETHNICITY', 'DESCENT_DESC'),
                                 ('STOP_ID', 'FORM_REF_NBR')])
    df_offender = df_raw[offender_cols.values()]
    df_offender.columns = offender_cols.keys()
    for i, row in df_offender.iterrows():
        yield [convert_val(val) for val in row]


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
