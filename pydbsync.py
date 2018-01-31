import pymysql
# from DBUtils.PooledDB import PooledDB
import json
import base64
import sys
import pika
import time
import datetime
import logging
import threading, multiprocessing
from logging.handlers import RotatingFileHandler
from config import dbname_rewrite, binary_columns, rabbitmq_conn_info, db_corpmod, db_info, timestamp_columns, \
    log_file_prefix, log_level

log_level_dict = {'info': logging.INFO, 'debug': logging.DEBUG, 'warn': logging.WARNING, 'error': logging.ERROR}

# log_filename = log_file_prefix % threading.current_thread().name
log_filename = log_file_prefix % multiprocessing.current_process().name
logger = logging.getLogger(__name__)
logger.setLevel(log_level_dict.get(log_level.lower()))
# formatter = logging.Formatter('%(asctime)s [%(levelname)-7s] %(threadName)s >> %(message)s')
formatter = logging.Formatter('%(asctime)s [%(levelname)-6s] %(processName)s >> %(message)s')
handler = RotatingFileHandler(log_filename, maxBytes=1*1024*1024*1024, backupCount=50)
handler.setFormatter(formatter)
logger.addHandler(handler)

# operation code
ROW_TYPE = {
    'insert': 1,
    'update': 2,
    'delete': 3,
    'table-create': 4,
    'table-drop': 4,
    'table-alter': 4,
    'database-create': 5,
    'database-drop': 5,
    'database-alter': 5,
    'int': 9,  # exception (rename?)  # http://maxwells-daemon.io/dataformat
}


class DBHelper(object):
    def __init__(self, dbinfo):
        self.dbinfo = dbinfo
        self.conn = self.__get_connection(dbinfo)

    def __get_connection(self, dbinfo):
        dbconn = None
        while True:
            try:
                dbconn = pymysql.Connect(**dbinfo)
            except pymysql.Error as e:
                print("MySQL.Error: {0}".format(e))
                if e.args[0] == 2003:
                    time.sleep(2)
                else:
                    sys.exit(2)
            else:
                return dbconn

    def query(self, sql, value=None):
        """
        :param sql: sql tempate to execute
        :param value: sql values to give
        :return: -1: execute fail, process next
                 -2: exit
                 other: rows_affected
        """
        count = 0
        try:
            cur = self.conn.cursor()
            if value:
                count = cur.execute(sql, value)
            else:
                count = cur.execute(sql)
            cur.close()
        except pymysql.Error as e:

            # try again with new connection
            if e.args[0] in (2013, 2003):
                print("MySQL.Error: {0} (retry)".format(e))
                time.sleep(1)
                self.conn = self.__get_connection(self.dbinfo)
                # there's should no dead loop here
                count = self.query(sql, value)
            elif e.args[0] in (1205, 1213):
                # deadlock found, or lock wait timeout
                print("MySQL.Error: {0} (retry)".format(e))
                print("  %s [%s]" % (sql, value))
                time.sleep(2)
                # deadlock should not always here, or it will be dead loop
                count = self.query(sql, value)
            else:
                print("MySQL.Error: {0} (skip)".format(e))
                print("  %s [%s]" % (sql, value))
                # print and skip
                return -1
        except KeyboardInterrupt:
            # requeue
            return -2

        return count

# param = b'{"database":"d_ec_crm","table":"t_eccrm_detail","type":"update","ts":1512817054,"xid":65734725,"commit":true,"data":{"f_crm_id":1133176658,"f_create_time":"2017-12-08 18:26:34","f_modify_time":"2017-12-09 18:57:34","f_contact_time":"2017-12-09 18:56:21","f_cs_guid":"0","f_tmp_id":0,"f_name":"\xe9\x9f\xa6\xe4\xbb\x95\xe5\x85\xb0","f_mobile":"15676198604","f_phone":"","f_title":"","f_fax":"","f_qq":"","f_msn":"","f_email":"","f_company":"","f_company_addr":"","f_company_url":"","f_crm_classid":0,"f_company_province":450000,"f_company_city":450100,"f_corp_id":5413956,"f_contact_num":0,"f_status":1,"f_first_letter":"W","f_call":"","f_company_id":0,"f_memo":"","f_crea_userid":6247932,"f_user_id":5414285,"f_add_plan":0,"f_vocation":0,"f_gender":2,"f_step":0},"old":{"f_modify_time":"2017-12-08 18:26:34"}}'
# row = json.loads(param.decode())
# print(d)

template = {
    ROW_TYPE['insert']: ['REPLACE INTO `{0}`.`{1}` ', '(`{0}`) VALUES({1});'],
    ROW_TYPE['update']: ['REPLACE INTO `{0}`.`{1}` ', '(`{0}`) VALUES({1});'],
    # 'update': 'UPDATE `{db_name}`.`{tb_name}` SET {2} WHERE {3}',
    ROW_TYPE['delete']: ['DELETE FROM `{0}`.`{1}` ', 'WHERE {0} LIMIT 1;'],
}

def gen_query_time(datetime_str):
    try:
        datetime_std = datetime.datetime.strptime(datetime_str.strip("'").strip('"'), "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(e)
        # return 0
    else:
        datetime_utc = datetime_std + datetime.timedelta(hours=8)
        return datetime_utc.strftime("%Y-%m-%d %H:%M:%S")

class RowDataSQL(object):
    def __init__(self, row):
        self.ERROR = False
        self.row = row
        self.row_db = row['database'].lower()
        self.row_tb = row['table'].lower()
        self.row_type = ROW_TYPE.get(row['type'], 9)

        if self.row_type < 4:  # dml
            self.row_data = row['data']
            self.template_part1, self.row_value = self.init_dml_tmpl_part1()
            # update: row['old']
        elif self.row_type in (4, 5):  # normal ddl
            self.row_data = row['sql']
        else:
            logger.warning('Unsupported row format: {%s}(skip)', row)
            self.ERROR = True  # skip

    def generate_ddl_statement(self, dbtable):
        """
        add database to statement
        :return ddl statement with db name
        """
        alter_sql = self.row_data.replace('`', '').replace('\r', ' ').replace('\n', ' ').lower()
        # column name `__#alibaba_rds_row_id#__` not compatiable

        if dbtable not in alter_sql:
            # alter t1 xxx: not with db.t1
            table = ' %s ' % self.row_tb
            alter_sql = alter_sql.replace(table, dbtable, 1)

        return alter_sql

    def init_dml_tmpl_part1(self):
        """
        generate insert/update/delete sql template with %s
        :param row_db_old is used to convert binary column value
        :param binary_columns is predefined

        :return template part1, and values used for later
        """
        row_value = []
        template_part1 = ''
        if self.row_type in (1, 2):  # insert or delete
            row_column = []
            template_param = ['%s'] * len(self.row_data)
            for row_c, row_v in self.row_data.items():
                row_column.append(row_c)

                is_binary = binary_columns.get(self.row_db, {}).get(self.row_tb, [])
                is_timestamp = timestamp_columns.get(self.row_db, {}).get(self.row_tb, [])
                if row_c in is_binary:
                    logger.info("column [%s.%s.%s] is binary", self.row_db, self.row_tb, row_c)
                    row_value.append(base64.b64decode(row_v))
                elif row_c in is_timestamp:
                    logger.info("column [%s.%s.%s] is timestamp: +8", self.row_db, self.row_tb, row_c)
                    row_value.append(gen_query_time(row_v))
                else:
                    row_value.append(row_v)

            template_part1 = template[self.row_type][1].format(
                '`,`'.join(row_column),
                ','.join(template_param)
            )
        elif self.row_type == 3:  # delete
            template_param = []
            for row_c, row_v in self.row_data.items():
                if row_v is not None:
                    template_param.append('`{0}`=%s'.format(row_c))
                else:
                    template_param.append('`{0}` is null /* %s */'.format(row_c))  # a is None

                is_binary = binary_columns.get(self.row_db, {}).get(self.row_tb, [])
                is_timestamp = timestamp_columns.get(self.row_db, {}).get(self.row_tb, [])
                if row_c in is_binary:
                    row_value.append(base64.b64decode(row_v))
                elif row_c in is_timestamp:
                    row_value.append(gen_query_time(row_v))
                else:
                    row_value.append(row_v)

            template_part1 = template[self.row_type][1].format(
                ' AND '.join(template_param)
            )
        else:
            # this code will never be reached
            print('/* Unsupported row format: {0} */'.format(self.row))
            return 0, 0
        return template_part1, tuple(row_value)

    def generate_dml_template(self, row_db_new):
        """
        :
        """
        template_part0 = template[self.row_type][0].format(row_db_new, self.row_tb)  # use row_db_new converted

        # template_part1, row_value = self.generate_dml_tmpl_part1()  # use row_db from row(json)

        return template_part0 + self.template_part1


class MaxwellSync(object):
    def __init__(self, exchange_bind, queue_route):
        # will be reset later if server gone away
        # retry when fail
        self.dbconn_cur = DBHelper(db_info)
        self.exchange_name = exchange_bind[0]
        self.exchange_type = exchange_bind[1]
        self.queue_name = queue_route[0]
        self.queue_bind_key = queue_route[1]

        self.corpmod = db_corpmod
        self.corpmod_ids = set([x if x % 4 == db_corpmod else db_corpmod for x in range(128)])

    # replay mode
    def process_data(self, row):
        """
        :param row: dict row data
        :return: -1: skip this message
                 1: success
                 -2: fail, requeue  # not used
        """
        rowdata_sql = RowDataSQL(row)
        if rowdata_sql.ERROR:
            # print("Unsupport sql statement: %s (skip)" % row)
            return -1

        ## ddl statement
        if rowdata_sql.row_type == 4:  # and rowdata_sql.row_db in dbname_rewrite.keys():

            dbtable = ' %s.%s ' % (rowdata_sql.row_db, rowdata_sql.row_tb)
            alter_sql = rowdata_sql.generate_ddl_statement(dbtable)

            dbtable_mod = ' %s.%s ' % (dbname_rewrite.get(rowdata_sql.row_db, rowdata_sql.row_db), rowdata_sql.row_tb)
            alter_sql.replace(dbtable, dbtable_mod, 1)
            self.dbconn_cur.query(alter_sql)
            return 1

        ## dml statement

        # convert database name: d_ec_crm0 -> d_ec_crm
        logger.debug("dbname rewrite from [%s] to [%s]", rowdata_sql.row_db, dbname_rewrite.get(rowdata_sql.row_db, rowdata_sql.row_db))
        row_db_new = dbname_rewrite.get(rowdata_sql.row_db, rowdata_sql.row_db)

        """
        # 如果这段代码要修改row_data的内容，必须在 RowDataSQL 初始化 init_dml_tmpl_part1() 之前调用
        # sp.1
        if rowdata_sql.row_data.get('__#alibaba_rds_row_id#__', 0):
            logger.info("column `__#alibaba_rds_row_id#__` in %s", rowdata_sql.row_data)
            if rowdata_sql.row_tb in ['t_crm_chglog', 't_plan_log_step']:
                rowdata_sql.row_data['f_id'] = rowdata_sql.row_data.pop('__#alibaba_rds_row_id#__')
                # del rowdata_sql.row_data['__#alibaba_rds_row_id#__']
            else:
                del rowdata_sql.row_data['__#alibaba_rds_row_id#__']

        # sp.2
        
        if rowdata_sql.row_tb == 't_eccrm_detail':
            if rowdata_sql.row_data['f_company_province'] is None:
                logger.warning('f_company_province is null: %s', rowdata_sql.row_data['f_crm_id'])
                rowdata_sql.row_data['f_company_province'] = '0'
                print(rowdata_sql.row_data['f_company_province'])
            if rowdata_sql.row_data['f_company_city'] is None:
                logger.warning('f_company_city is null: %s', rowdata_sql.row_data['f_crm_id'])
                rowdata_sql.row_data['f_company_city'] = '0'
        """

        sql_str = rowdata_sql.generate_dml_template(row_db_new)
        logger.debug("# statement sql: %s  param: %s", sql_str, rowdata_sql.row_value)
        ret = self.dbconn_cur.query(sql_str, rowdata_sql.row_value)
        return ret
                
        return 1

    # called outside
    def binlog_sync(self):
        logger.info("connect to rabbitmq server [%s], vhost=%s", rabbitmq_conn_info.get('host'), rabbitmq_conn_info.get('vhost', '/'))
        credentials = pika.PlainCredentials(rabbitmq_conn_info.get('user', 'guest'),
                                            rabbitmq_conn_info.get('password', 'guest')
        )
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbitmq_conn_info.get('host'),
                port=rabbitmq_conn_info.get('port', 5672),
                virtual_host=rabbitmq_conn_info.get('vhost', '/'),
                credentials=credentials
            )
        )
        channel = connection.channel()

        # exchange_name = 'maxwell.crm' + str(self.corpmod)
        # exchange_other = 'maxwell.AE'
        logger.info("declare mq exchange [%s], type=[%s]", self.exchange_name, self.exchange_type)
        channel.exchange_declare(exchange=self.exchange_name,
                                 exchange_type=self.exchange_type,
                                 durable=True,
                                 # arguments={'alternate-exchange': exchange_other}
        )

        """
        channel.exchange_declare(exchange=exchange_other, exchange_type='topic', durable=True)  # alternative exchange
        channel.queue_declare(queue='ae_other', durable=True)
        channel.queue_bind(exchange=exchange_other,
                           queue='ae_other',
                           routing_key='d_ec_crm.*')
        """
        logger.info("declare queue name=[%s]", self.queue_name)
        channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-mode': 'lazy'})

        for key in self.queue_bind_key:
            logger.info("bind routing_key [%s] to queue [%s]", key, self.queue_name)
            channel.queue_bind(exchange=self.exchange_name,
                               queue=self.queue_name,
                               routing_key=key)

        # consume callback, internal
        def callback(ch, method, properties, body):
            # print(" [x] Received %s" % body)
            logger.debug("Received message: %s", body)
            try:
                data_row = json.loads(body.decode('utf-8'))
                self.process_data(data_row)

                if ret == -2:  # requeue
                    logger.warning("message data: %s (requeue)", data_row)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    # return
            except ValueError as e:
                logger.error("proces Error: %s(skip)", e)
                logger.error("  received data: %s", body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error("proces Error: %s(skip)", e)
                logger.error("  message data: %s", data_row)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_qos(prefetch_count=50)
        channel.basic_consume(callback, queue=self.queue_name, no_ack=False)

        # print(' [*] Waiting for messages. To exit press CTRL+C')
        logger.info("start comsuming")
        channel.start_consuming()
