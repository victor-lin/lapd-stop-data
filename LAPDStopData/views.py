from contextlib import closing

from flask import render_template
from . import app, db


log = app.logger


def get_area_arrests():
    with db.connect_db() as con:
        cur = con.cursor()
        cur.execute("""  SELECT o.div_name, COUNT(ps.stop_id)
                           FROM officer o
                           JOIN policestop ps
                             ON o.officer_id = ps.officer1_id
                       GROUP BY o.div_name""")
        results = cur.fetchall()
        return {div_name: count for div_name, count in results}


@app.route('/')
def start():
    return render_template('start.html')


@app.route('/test')
def test():
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT * FROM officer')
            results = cur.fetchall()
    return render_template('test.html', results=results)


@app.route('/filter_data')
def filter_data():
    return render_template('filter_data.html')


@app.route('/figures')
def figures():
    div_count_data = get_area_arrests()
    return render_template('figures.html',
                           div_count_data=div_count_data)


@app.route('/results')
def results():
    return render_template('results.html')


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
