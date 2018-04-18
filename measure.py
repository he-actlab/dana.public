#!/usr/bin/python

import json
import csv
import os
import os.path
import time
import subprocess
import psycopg2
import sys
from subprocess import Popen, PIPE
from datetime import datetime
from glob import glob
sys.path.append('/home/joon/tabla.db/measurements/scripts/')
from sql_gen import SQLGenerator, madlib_func_lookup,\
    madlibtable_name_lookup, madlibtable_summary_name_lookup

measurement_dir = '/home/joon/tabla.db/measurements'


def read_config(filename):
    with open(filename, 'r') as f:
        fstream = f.read()
    return json.loads(fstream)


def gen_datafiles(config):
    ''' Generates datafiles. We assume that if data filename is present in
    config json file, it means the file actually exists in the system and
    there is no need to generate a new datafile.
    '''
    if not os.path.isdir('./datasets'):
        os.mkdir('./datasets')
    for cfg in config:
        print 'looking for dataset for ' + cfg['bench'] + '...',
        if 'filename' in cfg:
            print('Datafile ({:s}) found, moving on to next config...'.format(cfg['filename']))
            continue
        filename = './datasets/{}_{}_{}.txt'.format(cfg['bench'], cfg['n_vectors'], cfg['n_features'])
        cfg['filename'] = filename

        # if synthetic dataset already exists, continue to next config
        if os.path.isfile(filename):
            print('synthetic datafile ({:s}) already exists, moving on to next config...'.format(cfg['filename']))
            continue
    print('all datafiles generated!')


def get_segcount():
    basedir = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/'
    datadirs = glob(basedir + 'dbfast[0-9]*/demoDataDir[0-9]*/')
    return len(datadirs)


def get_pagesize(conn, cur):
    sql = 'SELECT 1 INTO test;' + \
          "SELECT pg_total_relation_size('test');"
    cur.execute(sql)
    conn.commit()
    pagesize = cur.fetchone()[0]
    cur.execute('DROP TABLE test;')
    conn.commit()
    return pagesize


def gen_filename(conn, cur):
    ''' result CSV file format: segcount-pagesize-%b%d-%H%M%S '''
    segcount = get_segcount()
    pagesize = get_pagesize(conn, cur)
    page = {8192: '8kb',
            16384: '16kb',
            32768: '32kb'}
    return str(segcount) + 'seg' + page[pagesize] + '-' + time.strftime("%b%d-%H%M%S") + '.csv'


def table_exists(cur, tablename):
    sql = "SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='{:s}');"
    cur.execute(sql.format(tablename))
    return cur.fetchone()[0]


def drop_madlib_tables(conn, cur, sql):
    ''' Drop all madlib output tables for the given benchmark '''
    print('[{}] dropping madlib tables...'.format(str(datetime.now())))
    madlib_table = sql.tablename + madlibtable_name_lookup[sql.bench]
    if table_exists(cur, madlib_table):
        cur.execute(sql.drop_table(madlib_table))
    madlib_summary_table = sql.tablename + madlibtable_summary_name_lookup[sql.bench]
    if table_exists(cur, madlib_summary_table):
        cur.execute(sql.drop_table(madlib_summary_table))
    print('[{}] done dropping madlib tables'.format(str(datetime.now())))
    conn.commit()
    return


def measure_io(conn, cur, tablename, db):
    ''' Measure disk IO time for cold cache '''
    stmt = 'explain analyze select count(*) from {}'.format(tablename)
    cur.execute(stmt)
    conn.commit()
    info_string = cur.fetchall()
    if db == 'greenplum':
        io = info_string[14][0].split(' ')[2]
        print io
    elif db == 'postgres':
        io = info_string[3][0].split(' ')[2]
    return io


def search_duration_file():
    basedir = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/'
    datadirs = glob(basedir + 'dbfast[0-9]*/demoDataDir[0-9]*/')
    qddir = glob(basedir + 'qddir/demoDataDir-1/')
    datadirs = datadirs + qddir
    for ddir in datadirs:
        if os.path.isfile(ddir + 'duration.txt'):
            return ddir + 'duration.txt'


def search_trans_file():
    basedir = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/'
    datadirs = glob(basedir + 'dbfast[0-9]*/demoDataDir[0-9]*/')
    qddir = glob(basedir + 'qddir/demoDataDir-1/')
    datadirs = datadirs + qddir
    for ddir in datadirs:
        if os.path.isfile(ddir + 'trans.txt'):
            return ddir + 'trans.txt'
        

def connect_to_database(db, measurement_dir, csv_rows, csv_headers):
    if db == 'greenplum':
        print 'connecting to greenplum!'
        try:
            conn = psycopg2.connect(dbname='template1',
                                    user='postgres',
                                    host='/tmp/',
                                    password='',
                                    port=15432)
            cur = conn.cursor()
        except psycopg2.Error as e:
            print("[EXCEPTION] unable to connect to database")
            print(e.pgerror)
            # filename = gen_filename(conn, cur)
            filename = 'ssd_' + time.strftime("%b-%d_%H-%M") + '.csv'
            write_to_file(filename, measurement_dir, csv_rows, csv_headers)
            exit()
    elif db == 'postgres':
        try:
            conn = psycopg2.connect(dbname='benchmarks',
                                    user='postgres',
                                    host='/tmp/')
            cur = conn.cursor()
        except psycopg2.Error as e:
            print("[EXCEPTION] unable to connect to database")
            print(e.pgerror)
            # filename = gen_filename(conn, cur)
            filename = 'ssd_' + time.strftime("%b-%d_%H-%M") + '.csv'
            write_to_file(filename, measurement_dir, csv_rows, csv_headers)
            exit()
    return conn, cur


def disconnect_from_database(conn, cur):
    print 'disconnecting!'
    cur.close()
    conn.close()


def flush_os_cache():
    if os.geteuid() != 0:
        subprocess.call(['sudo', 'sh', '-c', "echo 1 > /proc/sys/vm/drop_caches"])


def restart_database(db):
    if db == 'greenplum':
        p = Popen(['gpstop', '-r'], stdin=PIPE)
        p.stdin.write('y\n')
        # subprocess.call(['sudo', 'pkill', '-9', 'postgres'])
        # time.sleep(1)
        # p = Popen(['gpstart'], stdin=PIPE)
        # p.stdin.write('y\n')
    elif db == 'postgres':
        print 'restarting postgres!'
        pg_ctl = '/usr/local/pgsql/bin/pg_ctl'
        pg_datadir = '/usr/local/pgsql/data'
        pg_logfile = '/home/postgres/logfile'
        cmd = pg_ctl + ' -D ' + pg_datadir + ' -l ' + pg_logfile + ' restart'
        # Popen(['sudo', 'su', '-c', cmd, 'postgres'], stdin=PIPE)

        subprocess.call(['sudo', 'su', '-c', cmd, 'postgres'])


def write_to_file(filename, measurement_dir, csv_rows, csv_headers):
    filename = measurement_dir + '/results/time/' + filename
    with open(filename, 'w') as csvfile:
        csvwriter = csv.DictWriter(csvfile, fieldnames=csv_headers)
        csvwriter.writeheader()
        for row in csv_rows:
            csvwriter.writerow(row)


def main():
    if len(sys.argv) != 4:
        print('Usage: ./measure.py [nvme | ssd | hdd] [config.json file] [greenplum | postgres]')
        sys.exit()

    disk = sys.argv[1]
    if disk not in ['nvme', 'ssd', 'hdd']:
        print('Usage: ./measure.py [nvme | ssd | hdd] [config.json file] [greenplum | postgres]')
        sys.exit()

    conf_file = sys.argv[2]
    config = read_config(conf_file)
    gen_datafiles(config)

    db = sys.argv[3]
    print db

    # csv_headers = ['bench', 'total_cold', 'uda_cold', 'compute_cold',
    #                'data_cold', 'io_cold', 'total_hot', 'uda_hot',
    #                'compute_hot', 'data_hot', 'io_hot']
    csv_headers = ['bench', 'total_cold', 'uda_cold', 'compute_cold',
                   'data_cold', 'io_cold']
    csv_rows = []

    # connection and cursor
    conn, cur = connect_to_database(db, measurement_dir, csv_rows, csv_headers)

    print('*' * 32)
    for i, cfg in enumerate(config):
        print('[Bench {:d}] Running benchmark {:s}: '.format(i, cfg['bench']))
        sql = SQLGenerator(cfg, db)
        tablename = sql.tablename
        try:
            csv_row = {}
            csv_row['bench'] = tablename

            ##############
            # Cold Cache #
            ##############
            print('(1) Cold cache run...')

            # Copy data from datafile to postgres table
            if not table_exists(cur, sql.tablename):
                start = datetime.now()
                print('[{}] creating table ({})...'.format(str(start), sql.tablename))
                print(sql.create_table())
                cur.execute(sql.create_table())
                if cfg['bench'] != 'lrmf':
                    cur.execute('alter table {} alter column features set storage plain;'.format(sql.tablename))
                elif cfg['bench'] == 'lrmf':
                    # column val's storage should be plain?
                    cur.execute('alter table {} alter column val set storage plain;'.format(sql.tablename))
                conn.commit()
                stop = datetime.now()
                elapsed = stop - start
                print('[{}] done creating table. Elapsed: {}'.format(str(datetime.now()), elapsed.__str__()))
                with open(cfg['filename'], 'r') as f:
                    print('[{}] copying data from datafile ({}) to table...'.format(str(datetime.now()), cfg['filename']))
                    if cfg['bench'] == 'lrmf':
                        start = datetime.now()
                        cur.copy_expert("COPY " + tablename + " (row, col, val) FROM STDIN CSV", f)
                        conn.commit()
                        stop = datetime.now()
                        elapsed = stop - start
                    else:
                        start = datetime.now()
                        cur.copy_expert("COPY " + tablename + " (y, features) FROM STDIN CSV", f)
                        conn.commit()
                        stop = datetime.now()
                        elapsed = stop - start
                    print('[{}] done copying data. Elapsed: {}'.format(str(datetime.now()), elapsed.__str__()))

            #continue # just to copy data
        
            # # The following actions are done only for cold cache.
            # # Must close connection before restarting.
            # disconnect_from_database(conn, cur)
            # print '[{}] After disconnect db, sleep for 5 sec...'.format(str(datetime.now()))
            # time.sleep(5)
            # print '[{}] done sleeping!'.format(str(datetime.now()))
            # restart_database(db)
            # print '[{}] After restart db, sleep for 5 sec...'.format(str(datetime.now()))
            # time.sleep(5)
            # print '[{}] done sleeping!'.format(str(datetime.now()))
            # conn, cur = connect_to_database(db, measurement_dir, csv_rows, csv_headers)

            # # OS buffer cache before cache flush
            # subprocess.call(['free', '-h'])
            # #flush_os_cache()
            # # buffer cache after flush
            # subprocess.call(['free', '-h'])

            # measure disk IO time
            io = float(measure_io(conn, cur, tablename, db))
            csv_row['io_cold'] = io

            # run madlib
            start = datetime.now()
            print('[{}] Running madlib function {}()...'.format(str(start), madlib_func_lookup[sql.bench]))
            print sql.madlib_func()
            cur.execute(sql.madlib_func())
            conn.commit()
            stop = datetime.now()
            elapsed = stop - start
            print('[{}] Done running madlib function. Elapsed: {}'.format(str(stop), elapsed.__str__()))
            if db == 'greenplum':
                exectime_str = cur.fetchall()[11][0]
            elif db == 'postgres':
                exectime_str = cur.fetchall()[2][0]
            exectime = float(exectime_str.split()[2])

            # Get the uda, compute, and data time from duration.txt
            if db == 'greenplum':
                # if cfg['bench'] == 'lrmf':  # lrmf only uses master node for some reason
                #     transfile = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/trans.txt'
                # else:
                #     transfile = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/trans.txt'
                #transfile = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/trans.txt'
                transfile = search_trans_file()
                if transfile is None:  # safeguard
                    transfile = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/trans.txt'
                with open(transfile, 'r') as f:
                    lastline = f.read().splitlines()[-1].split(',')
                    uda_cumulative = float(lastline[0]) / 1000.0
                    compute_cumulative = float(lastline[1]) / 1000.0
                duration_file = search_duration_file()
                with open(duration_file, 'r') as f:
                    durations = f.read().splitlines()[-1].split(',')
                    udatime = float(durations[0]) / 1000.0  # us to ms
                    computetime = float(durations[1]) / 1000.0  # us to ms
            elif db == 'postgres':
                #cmd = 'tail -1 /usr/local/pgsql/data/duration.txt'
                cmd = 'tail -1 /home/postgres/duration.txt'
                p = Popen(['sudo', 'su', '-c', cmd, 'postgres'], stdout=PIPE)
                lastline = p.stdout.read()
                durations = lastline.split(',')
                udatime = float(durations[0]) / 1000.0  # us to ms
                computetime = float(durations[1]) / 1000.0  # us to ms
                # delete trans.txt for setting 2 runs
                print '[INFO] Deleting trans.txt file...'
                Popen(['sudo', 'rm', '/usr/local/pgsql/data/trans.txt'])

            # greenplum is weird
            if db == 'greenplum':
                udatime += uda_cumulative
                computetime += compute_cumulative
                print('deleting duration.txt file...' + duration_file)
                os.remove(duration_file)

            # lrmf only runs for 1 epoch, so we need to multiply by
            # however many epochs it's supposed to run for
            # if cfg['bench'] == 'lrmf':
            #     print '[DEBUG] lrmf epoch multiply: ' + str(sql.max_iter)
            #     exectime *= sql.max_iter
            #     udatime *= sql.max_iter
            #     computetime *= sql.max_iter

            data_cold = exectime - (udatime + computetime) - io
            csv_row['total_cold'] = exectime
            csv_row['uda_cold'] = '{:.2f}'.format(udatime)
            csv_row['compute_cold'] = '{:.2f}'.format(computetime)
            csv_row['data_cold'] = '{:.2f}'.format(data_cold)
            print ''

            #############
            # Hot Cache #
            #############
            # print('(2) Hot cache run...')
            # # madlib complains if madlib output tables already exist
            # drop_madlib_tables(conn, cur, sql)

            # # measure disk IO time
            # io = float(measure_io(conn, cur, tablename, db))
            # csv_row['io_hot'] = io

            # # run madlib
            # start = datetime.now()
            # print('[{}] Running madlib function {}()...'.format(str(start), madlib_func_lookup[sql.bench]))
            # cur.execute(sql.madlib_func())
            # conn.commit()
            # stop = datetime.now()
            # elapsed = stop - start
            # print('[{}] Done running madlib function. Elapsed: {}'.format(str(stop), elapsed.__str__()))

            # if db == 'greenplum':
            #     exectime_str = cur.fetchall()[11][0]
            # elif db == 'postgres':
            #     exectime_str = cur.fetchall()[2][0]
            # exectime = float(exectime_str.split()[2])

            # # Get the uda, compute, and data time from duration.txt
            # if db == 'greenplum':
            #     if cfg['bench'] == 'lrmf':  # lrmf only uses master node for some reason
            #         transfile = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/trans.txt'
            #     else:
            #         transfile = '/home/joon/gpdb-5.1.0/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/trans.txt'
            #     with open(transfile, 'r') as f:
            #         lastline = f.read().splitlines()[-1].split(',')
            #         uda_cumulative = float(lastline[0]) / 1000.0
            #         compute_cumulative = float(lastline[1]) / 1000.0
            #     duration_file = search_duration_file()
            #     with open(duration_file, 'r') as f:
            #         durations = f.read().splitlines()[-1].split(',')
            #         udatime = float(durations[0]) / 1000.0  # us to ms
            #         computetime = float(durations[1]) / 1000.0  # us to ms
            # elif db == 'postgres':
            #     #cmd = 'tail -1 /usr/local/pgsql/data/duration.txt'
            #     cmd = 'tail -1 /home/postgres/duration.txt'
            #     p = Popen(['sudo', 'su', '-c', cmd, 'postgres'], stdout=PIPE)
            #     lastline = p.stdout.read()
            #     durations = lastline.split(',')
            #     udatime = float(durations[0]) / 1000.0  # us to ms
            #     computetime = float(durations[1]) / 1000.0  # us to ms
            #     # delete trans.txt for setting 2 runs
            #     print '[INFO] Deleting trans.txt file...'
            #     Popen(['sudo', 'rm', '/usr/local/pgsql/data/trans.txt'])

            # # greenplum is weird
            # if db == 'greenplum':
            #     udatime += uda_cumulative
            #     computetime += compute_cumulative
            #     print('deleting duration.txt file...' + duration_file)
            #     print('')
            #     os.remove(duration_file)

            # # lrmf only runs for 1 epoch, so we need to multiply by
            # # however many epochs it's supposed to run for
            # # if cfg['bench'] == 'lrmf':
            # #     exectime *= sql.max_iter
            # #     udatime *= sql.max_iter
            # #     computetime *= sql.max_iter

            # data_hot = exectime - (udatime + computetime) - io
            # csv_row['total_hot'] = exectime
            # csv_row['uda_hot'] = '{:.2f}'.format(udatime)
            # csv_row['compute_hot'] = '{:.2f}'.format(computetime)
            # csv_row['data_hot'] = '{:.2f}'.format(data_hot)

            csv_rows.append(csv_row)
            drop_madlib_tables(conn, cur, sql)
            conn.commit()
            print('*' * 32)
        except psycopg2.Error as e:
            print("[EXCEPTION] unable to execute query")
            print(e.pgerror)
            filename = gen_filename(conn, cur)
            write_to_file(filename, measurement_dir, csv_rows, csv_headers)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        except KeyboardInterrupt:
            print('Keyboard interrupt')
            write_to_file(disk, measurement_dir, csv_rows, csv_headers)
            try:
                sys.exit(0)
            except SystemExit:
                os._exit(0)
        # finally:
        #     csv_rows.append(csv_row)
        #     # drop madlib and data tables
        #     drop_madlib_tables(conn, cur, sql)
        #     # cur.execute(sql.drop_table(sql.tablename))
        #     conn.commit()
        #     print('*' * 32)
        #     # csv_rows.append(csv_row)

    filename = gen_filename(conn, cur)
    write_to_file(filename, measurement_dir, csv_rows, csv_headers)


if __name__ == '__main__':
    main()
