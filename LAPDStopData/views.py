from contextlib import closing

from flask import render_template
from . import app
from .db import connect_db, init_db, drop_tables


log = app.logger


@app.route('/')
def start():
    return render_template('start.html')


@app.route('/test')
def test():
    with connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT table_name FROM user_tables')
            results = cur.fetchall()
    return render_template('test.html', results=results)


@app.route('/query')
def get_query():
    pass


@app.route('/_initdb/', methods=['POST'])
def load_db():
    init_db()
    return "initialized database"


@app.route('/_droptables/', methods=['POST'])
def teardown():
    drop_tables()
    return "dropped tables"
