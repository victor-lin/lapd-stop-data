from contextlib import closing
from collections import OrderedDict

from flask import render_template, request
from . import app, db


log = app.logger

counter = 0

def get_ethnicity_distribution():
    """
    Return
    ------
    { ethnicity : stop count }
    """
    with db.connect_db() as con:
        cur = con.cursor()
        cur.execute("""  SELECT o.ethnicity,
                                COUNT(*) AS n
                           FROM offender o
                       GROUP BY o.ethnicity
                       ORDER BY n DESC""")
        results = cur.fetchall()
        return OrderedDict(results)


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
    tuple_counts = get_tuple_count()
    return render_template('start.html', tuple_counts=tuple_counts)


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

@app.route('/filter_data/officer')
def filter_officer():
    sql_query = 'SELECT * FROM OFFICER'
    constraints = ''
    selected_regions = request.args.getlist('Region')
    if selected_regions:
        if selected_regions[0] != 'All':
            sql_query += ' WHERE '
            for reg in selected_regions:
                sql_query += 'DIV_NAME = \''
                sql_query += reg
                sql_query += '\' OR '
            sql_query = sql_query[:-4]
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT column_name FROM all_tab_cols WHERE table_name = \'OFFICER\'')
            table_head = cur.fetchall()
            cur.execute(sql_query)
            table_data = cur.fetchall()
            cur.execute('SELECT DISTINCT DIV_NAME FROM OFFICER')
            regions = cur.fetchall()
    return render_template('officer.html', table_head=table_head[::-1],
                           table_data=table_data, regions=regions)

@app.route('/filter_data/offender')
def filter_offender():
    global counter
    sql_query = 'SELECT * FROM OFFENDER'
    selected_gender = request.args.get('Gender')
    selected_ethnicity = request.args.getlist('Ethnicity')
    if request.args.get('get_next_results'):
        counter += 1
    elif request.args.get('get_previous_results'):
        counter -= 1
    else:
        counter = 0
    if selected_gender and selected_ethnicity:
        if (selected_gender != 'All' and selected_ethnicity[0] != 'ALL'):
            sql_query += ' WHERE '
            sql_query += 'GENDER = \''
            sql_query += selected_gender
            sql_query += '\' AND '
            for eth in selected_ethnicity:
                sql_query += 'ETHNICITY = \''
                sql_query += eth
                sql_query += '\' AND'
            sql_query = sql_query[:-4]
        elif (selected_gender != 'All' and selected_ethnicity[0] == 'ALL'):
            sql_query += ' WHERE '
            sql_query += 'GENDER = \''
            sql_query += selected_gender
            sql_query += '\' '
        elif (selected_gender == 'All' and selected_ethnicity[0] != 'ALL'):
            for eth in selected_ethnicity:
                sql_query += ' WHERE '
                sql_query += 'ETHNICITY = \''
                sql_query += eth
                sql_query += '\' AND '
            sql_query = sql_query[:-4]
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT column_name FROM all_tab_cols WHERE table_name = \'OFFENDER\'')
            table_head = cur.fetchall()
            cur.execute('SELECT DISTINCT ETHNICITY FROM OFFENDER')
            ethnicities = cur.fetchall()
            cur.execute(sql_query)
            for x in xrange(counter):
                cur.fetchmany(numRows=5000)
            table_data = cur.fetchmany(numRows=5000)
    return render_template('offenders.html', table_head=table_head[::-1], ethnicities=ethnicities,
                           table_data=table_data, counter=counter)

@app.route('/filter_data/police_stops')
def filter_police_stops():
    global counter
    sql_query = 'SELECT * FROM POLICESTOP'
    begin_month = request.args.get('begin-month')
    end_month = request.args.get('end-month')
    selected_type = request.args.get('Type')
    if request.args.get('get_next_results'):
        counter += 1
    elif request.args.get('get_previous_results'):
        counter -= 1
    else:
        counter = 0
    if begin_month and end_month and selected_type:
        sql_query += ' WHERE '
        sql_query += 'STOP_DATE BETWEEN TO_DATE(\''
        sql_query += begin_month
        sql_query += '\', \'YYYY-MON-DD\') AND TO_DATE(\''
        sql_query += end_month
        sql_query += '\', \'YYYY-MON-DD\') + 1 - (1/(24*60*60))'
        if(selected_type != 'All'):
            sql_query += 'AND STOP_TYPE = \''
            sql_query += selected_type
            sql_query += '\''
    log.info(sql_query)
        
    with db.connect_db() as con:
        log.info('connected')
        with closing(con.cursor()) as cur:
            log.info('querying stuff')
            cur.execute('SELECT column_name FROM all_tab_cols WHERE table_name = \'POLICESTOP\'')
            table_head = cur.fetchall()
            cur.execute(sql_query)
            for x in xrange(counter):
                cur.fetchmany(numRows=5000)
            table_data = cur.fetchmany(numRows=5000)
    return render_template('police_stops.html', table_head=table_head[::-1],
                           table_data=table_data, counter=counter)


# FIGURES

@app.route('/figures/ethnicity')
def figures_ethnicity():
    eth_count_data = get_ethnicity_distribution()
    ethnicities = eth_count_data.keys()
    return render_template('ethnicity.html',
                           eth_count_data=eth_count_data,
                           ethnicities=ethnicities,
                           num_ethnicities=len(ethnicities))


@app.route('/figures/location')
def figures_location():
    div_count_data = get_area_arrests()
    divisions = div_count_data.keys()
    return render_template('location.html',
                           div_count_data=div_count_data,
                           divisions=divisions,
                           num_divisions=len(divisions))


@app.route('/figures/other')
def figures_other():
    # 1
    stop_type_data = get_stop_type_info()
    # 2
    divisions, ethnicities, area_race_data = get_area_race_data()
    return render_template('other.html',
                           # 1
                           stop_type_data=stop_type_data,
                           # 2
                           area_race_data=area_race_data,
                           divisions=divisions,
                           ethnicities=enumerate(ethnicities),
                           num_ethnicities=len(ethnicities),
                           num_divisions=len(divisions))


@app.route('/results')
def results():
    return render_template('results.html')


@app.route('/_get_tuple_count', methods=['POST'])
def get_tuple_count():
    """
    Return
    ------
    OD{ table name : tuple count }
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
    return OrderedDict(results)


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


@app.errorhandler(500)
def internal_error(exception):
    app.logger.error(exception)
    return render_template('500.html'), 500


@app.errorhandler(404)
def page_not_found(exception):
    return render_template('404.html'), 404
