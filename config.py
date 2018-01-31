# target db
db_info = {
    'host': 'xxxxxxxx.mysql.rds.aliyuncs.com',
    'port': 3306,
    'user': 'dbuser',
    'password': 'dbpass',
    'autocommit': True,
    'charset': 'utf8mb4',
}

db_corpmod = 0

log_file_prefix = 'maxwell_%s.log'
log_level = 'info'

# rabbimq
rabbitmq_conn_info = {
    'host': 'rabbitmq_host',
    'port': 5672,
    'user': 'admin',
    'password': 'admin',
    'vhost': '/crm' + str(db_corpmod)
}


rabbitmq_queue_bind2 = [
    {
        'exchange': ['maxwell.crm' + str(db_corpmod), 'topic'],
        'queues': {
            'default': ['*.*'],
        }
    }
]

# rabbitmq_sharding
rabbitmq_queue_bind = [
    # 'default': ['dbtest.*'],  # this is default
    # 'dbtest.t_eccrm_detail', 'dbtest.t_crm_relation'
    {
        'exchange': ['maxwell.AE', 'topic'],
        'queues': {
            # 'default': ['dbtest.*'],
            'default': ['*.*'],
        }
    },
    # maxwell not suport alternative exchange yet, so use policy
    # rabbitmqctl set_policy -p '/crm1' --apply-to 'exchanges' AE "^maxwell.crm1$" '{"alternate-exchange":"maxwell.AE"}'
    {
        'exchange': ['maxwell.crm' + str(db_corpmod), 'topic'],
        'queues': {
            't1': ['dbtest.t1'],
            't2_t3': ['dbtest.t2', 'dbtest.t3'],
            'mydb': ['mydb.*']
        }
    },
]

# source db
binary_columns = {
    'mydb1': {
        't2': ['f_account', 'f_password'],
        't3': ['f_name', 'f_email']
    },
}

timestamp_columns = {
    'dbtest': {
        't4': ['f_createtime', 'f_updatetime'],
        't5': ['f_time'],
    },
    'mydb': {
        't1': ['f_createtime'],
    }
}

## if dbname_rewrite is set, rollback mode, else replay mode
dbname_rewrite = {}

dbname_rewrite2 = {
    'dbtest0': 'dbtest',
    'dbtest1': 'dbtest',
    'dbtest2': 'dbtest',
    'dbtest3': 'dbtest'
}
