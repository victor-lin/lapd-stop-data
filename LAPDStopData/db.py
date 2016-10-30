from pandas import read_csv, DataFrame
from collections import OrderedDict
from cx_Oracle import connect, makedsn
from contextlib import closing
from . import app


log = app.logger


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
            # TODO: fix data types in df_raw

            # for statement in get_insert_statements_stop(df_raw):
            #     cur.execute(statement)
        con.commit()


def get_insert_statements_stop(df_raw):
    """Return insert statement strings for PoliceStop table"""

    # TODO: concat `STOP_DT` and `STOP_TM` -> `STOP_DATE`

    stop_dict = OrderedDict([('STOP_ID', 'FORM_REF_NBR'),
                             # ('STOP_DATE', 'STOP_DATE'),
                             ('STOP_TYPE', 'STOP_TYPE'),
                             ('POST_STOP_ACTIVITY', 'POST_STOP_ACTV_IND'),
                             ('OFFICER1_ID', 'OFCR1_SERL_NBR'),
                             ('OFFICER2_ID', 'OFCR2_SERL_NBR')])
    df_stop = df_raw.groupby('FORM_REF_NBR').first().reset_index()[stop_dict.values()]
    df_stop.columns = stop_dict.keys()
    for i, row in df_stop.iterrows():
        yield 'INSERT INTO PoliceStop VALUES ({})'.format(','.join([str(v) for v in row]))


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
