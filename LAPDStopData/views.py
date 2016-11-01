from contextlib import closing

from flask import render_template
from . import app, db


log = app.logger


@app.route('/')
def start():
    return render_template('start.html')


@app.route('/test')
def test():
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT table_name FROM user_tables')
            results = cur.fetchall()
    return render_template('test.html', results=results)


@app.route('/query')
def get_query():
    pass


@app.route('/_create_schema/', methods=['POST'])
def create_schema_view():
    db.create_schema()
    return ""


@app.route('/_add_constraints/', methods=['POST'])
def add_constraints_view():
    db.add_constraints()
    return ""


@app.route('/_populate/<table_name>/', methods=['POST'])
def populate_view(table_name):
    db.populate(table_name)
    return ""


@app.route('/_drop_tables/', methods=['POST'])
def drop_tables_view():
    db.drop_tables()
    return ""
