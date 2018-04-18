#!/usr/bin/python

import sys
sys.path.append('/home/joon/tabla.db/measurements')
import measure
import json
import psycopg2
import csv
from sql_gen import SQLGenerator

if __name__ == '__main__':
    try:
        conn = psycopg2.connect(dbname='benchmarks', user='postgres', host='/tmp/', password='postgres')
    except psycopg2.Error as e:
        print("[EXCEPTION] unable to conenct to database")
        print(e.pgerror)
        exit()
    cur = conn.cursor()

    config = measure.read_config('/home/joon/tabla.db/measurements/config_set2.json')

    csv_headers = ['bench', 'tablesize', 'pagecount']
    csv_rows = []

    for cfg in config:
        sql = SQLGenerator(cfg)
        cur.execute(sql.create_table())

        csv_row = {}
        with open(cfg['filename'], 'r') as f:
            if cfg['bench'] == 'lrmf':
                cur.copy_expert("COPY " + sql.tablename + " (row, col, val) FROM STDIN CSV", f)
            else:
                cur.execute('alter table {} alter column features set storage plain;'.format(sql.tablename))
                cur.copy_expert("COPY " + sql.tablename + " (y, features) FROM STDIN CSV", f)
            conn.commit()
        cur.execute("select * from pg_relpages('{}');".format(sql.tablename))
        numpage = cur.fetchone()[0]
        cur.execute("select pg_relation_size('{}');".format(sql.tablename))
        tablesize = cur.fetchone()[0]
        cur.execute("select * from pgstattuple('{}');".format(sql.tablename))
        cur.execute(sql.drop_table(sql.tablename))
        conn.commit()

        csv_row['bench'] = sql.tablename
        csv_row['tablesize'] = tablesize
        csv_row['pagecount'] = numpage
        csv_rows.append(csv_row)

    with open('/home/joon/tabla.db/measurements/pagenums.csv', 'w') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=csv_headers)
        csvwriter.writeheader()
        for row in csv_rows:
            csvwriter.writerow(row)
