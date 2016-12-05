from contextlib import closing

from flask import render_template
from . import app, db


log = app.logger


def get_area_arrests():
    """
    Return
    ------
    { division name : stop count }
    """
    with db.connect_db() as con:
        cur = con.cursor()
        cur.execute("""  SELECT o.div_name, COUNT(*)
                           FROM officer o
                           JOIN policestop ps
                             ON o.officer_id = ps.officer1_id
                       GROUP BY o.div_name""")
        results = cur.fetchall()
        return dict(results)

def get_stop_type_info():
    with db.connect_db() as con:
        cur = con.cursor()
        cur.execute("""  SELECT stop_type, COUNT(stop_type) AS counts 
                           FROM policestop 
                       GROUP BY stop_type 
                       ORDER BY counts DESC""")
        results = cur.fetchall()
        return {stop_type: count for stop_type, count in results}


def get_area_race_data():
    """
    Return
    ------
    {
        ethnicity: { division name : count }
    }
    """
    with db.connect_db() as con:
        cur = con.cursor()
        cur.execute("""  SELECT ofcr.div_name,
                                o.ethnicity,
                                COUNT(*) AS n
                           FROM Offender o
                           JOIN PoliceStop ps
                                ON ps.stop_id = o.stop_id
                           JOIN Officer ofcr
                                ON ofcr.officer_id = ps.officer1_id
                       GROUP BY o.ethnicity, ofcr.div_name""")
        results = cur.fetchall()
        cur.execute("""SELECT DISTINCT ethnicity
                         FROM Offender""")
        ethnicities = [x[0] for x in cur.fetchall()]
    div_counts = get_area_arrests()
    divisions = sorted(div_counts.keys(),
                       key=lambda x: div_counts[x],
                       reverse=True)
    # use top 26 divisions, aggregate the rest in 'other'
    top_divisions = divisions[:26]
    data = {ethnicity: {'other': 0} for ethnicity in ethnicities}
    for division, ethnicity, count in results:
        if division in top_divisions:
            data[ethnicity][division] = count
        else:
            data[ethnicity]['other'] += count
    return top_divisions + ['other'], ethnicities, data


@app.route('/')
def start():
    return render_template('start.html')


@app.route('/test')
def test():
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT column_name FROM all_tab_cols WHERE table_name = \'OFFICER\'')
            table_head = cur.fetchall()
            cur.execute('SELECT * FROM OFFICER')
            table_data = cur.fetchall()
    return render_template('test.html', table_head=table_head[::-1], table_data=table_data)


@app.route('/filter_data')
def filter_data():
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT column_name FROM all_tab_cols WHERE table_name = \'OFFICER\'')
            table_head = cur.fetchall()
            cur.execute('SELECT * FROM OFFICER')
            table_data = cur.fetchall()
    return render_template('filter_data.html', table_head=table_head[::-1], table_data=table_data)


@app.route('/figures')
def figures():
    div_count_data = get_area_arrests()
    divisions, ethnicities, area_race_data = get_area_race_data()
    return render_template('figures.html',
                           # 1
                           div_count_data=div_count_data,
                           # 2
                           area_race_data=area_race_data,
                           divisions=divisions,
                           ethnicities=enumerate(ethnicities),
                           num_ethnicities=len(ethnicities),
                           num_divisions=len(divisions))

@app.route('/stop_type_bar_chart')
def stop_type_bar_chart():
    stop_type_data = get_stop_type_info()
    return render_template('stoptype.html',stop_type_data= stop_type_data)


@app.route('/area_race')
def area_race():
    divisions, ethnicities, area_race_data = get_area_race_data()
    return render_template('area_race.html',
                           area_race_data=area_race_data,
                           divisions=divisions,
                           ethnicities=enumerate(ethnicities),
                           num_ethnicities=len(ethnicities))


@app.route('/results')
def results():
    return render_template('results.html')


@app.route('/_get_tuple_count', methods=['POST'])
def get_tuple_count():
    """
    Return
    ------
    { table name : tuple count }
    """
    with db.connect_db() as con:
        cur = con.cursor()
        cur.execute("""  SELECT table_name, num_rows
                           FROM user_tables
                          WHERE (table_name = 'OFFICER' OR
                                 table_name = 'OFFENDER' OR
                                 table_name = 'POLICESTOP')
                       ORDER BY num_rows DESC""")
        results = cur.fetchall()
    return dict(results)


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
