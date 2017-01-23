import sys
import pymysql
import datetime
import json
import tempfile
import os as _os
from boto3.session import Session
from datetime import datetime

unlink = _os.unlink


# --- Config S3 Keys
with open('settings.json', 'r') as s:
    s3conf = json.load(s)

session = Session(
    aws_access_key_id=s3conf['s3']['s3KeyId'], 
    aws_secret_access_key=s3conf['s3']['s3SecKey'],
    region_name=s3conf['s3']['region'])

_s3 = session.resource('s3')
bucket = 'artear-files'
# --- Config S3 Keys


# --- Config RDS SQL
with open('rds_config.json', 'r') as r:
    rdsconf = json.load(r)
# --- Config RDS SQL

day = datetime.now().strftime('%d')
month = datetime.now().strftime('%m')
year = datetime.now().strftime('%Y')

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super(DatetimeEncoder, obj).default(obj)
        except TypeError:
            return str(obj)


#Params CONN SQL
def connMysql(rds_host, name, password, db):
    
    print "Definiendo conn..."
    
    conn = pymysql.connect(
        host=rds_host, 
        user=name, 
        passwd=password, 
        db=db, 
        connect_timeout=5,
        charset='utf8',
        use_unicode=True,
        cursorclass=pymysql.cursors.DictCursor
    )

    return conn

print "Starting...."

def handler(event, context):

    for product, param in rdsconf.items():

        rds_host=param['host']
        name=param['username']
        password=param['password']
        db=param['db']

        try:
            conn = connMysql(rds_host, name, password, db)
        except Exception as e:
            print "--- ERROR!" + str(e)

        with conn.cursor() as cur:
            print "-- Ejecutando query ...."
            try:
                cur.execute("SELECT * FROM slow_log WHERE start_time BETWEEN DATE_SUB(NOW(), INTERVAL 1 DAY) AND NOW() AND query_time >= 2 AND db NOT LIKE 'mysql' ORDER BY start_time DESC")
                print "-- Query ejecutada... "
            except Exeception as e:
                print e
            finally:
                conn.close()

            print "--- Obteniendo result"
            result = cur.fetchall()
            
            if not result:
                print "--- NOT RESULT"
                break
            
            print "-- Result obtenido... "

            data = unicode(json.dumps(result, cls=DatetimeEncoder, encoding='utf-8'))
            
            print "-- TO JSON ... "
            json_data = json.loads(data)

            print "Leyendo data ...."
            temp = tempfile.NamedTemporaryFile(delete=False)
        
            fileLog=product + '_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.log'


            print "--- Writing File... "
            temp.write('/usr/sbin/mysqld, Version: 5.1.49-3-log ((Debian)). started with: \nTcp port: 3306  Unix socket: /var/run/mysqld/mysqld.sock \nTime                 Id Command    Argument\n\n')

            for row in json_data:

                row['year'] = row['start_time'][2:4]
                row['month'] = row['start_time'][5:7]
                row['day'] = row['start_time'][8:10]
                row['time'] = row['start_time'][11:]

                hours = int(row['query_time'][0:1])
                minutes = int(row['query_time'][2:4])
                seconds = int(row['query_time'][5:])
                row['query_time_f'] = hours * 3600 + minutes * 60 + seconds

                hours = int(row['lock_time'][0:1])
                minutes = int(row['lock_time'][2:4])
                seconds = int(row['lock_time'][5:])
                row['lock_time_f'] = hours * 3600 + minutes * 60 + seconds

                if not row['sql_text'].endswith(';'):
                    row['sql_text'] += ';'
                
                temp.write('# Time: {year}{month}{day} {time}'.format(**row))
                temp.write('\n# User@Host: {user_host}'.format(**row))
                temp.write('\n# Query_time: {query_time_f}  Lock_time: {lock_time_f} Rows_sent: {rows_sent}  Rows_examined: {rows_examined}'.format(**row))
                temp.write('\nuse {db};'.format(**row))
                temp.write('\n')
                temp.write(row['sql_text'])
                temp.write('\n')
        
            print "--- End File... "
            temp.close()
            print "--- Uploading S3..."
            _s3.meta.client.upload_file(temp.name, bucket, 'slow-query/' + product + '/' + year + '/' + month + '/' + fileLog)
            print "--- Upload S3"
            temp.unlink(temp.name)
            cur.close()
        #conn.close()