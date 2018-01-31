# 基于MySQL binlog增量数据同步方案(maxwell+rabbimt+pydbsync)


应用场景：同 http://seanlook.com/2017/09/05/mysql-binlog-subscribe-simple-for-dba/ ，但更灵活：
> 实时同步部分表到另外一个数据库实例  
> 比如在轨迹迁库时，将当天表的数据同步到新库，模拟阿里云dms数据传输的功能，相当于在测试环境演练，减少失误。  
> 另外还可以从新库反向同步增量数据到老库，解决测试环境多项目测试引起数据库冲突的问题。  
> 
> 正式切库时的回滚措施  
> 比如轨迹数据独立项目，切换期间数据写向新库，但如果切换失败需要回滚到老库，就需要把这段时间新增的数据同步回老库（启动消费程序），这就不需要程序段再考虑复杂的回滚设计。
>
> 数据库闪回  
> 关于数据库误操作的闪回方案，见文章MySQL根据离线binlog快速闪回 。binlog2sql的 -B 选项可以将sql反向组装，生产回滚sql。如果需要完善的闪回功能，要进一步开发，提高易用性。
> 
> binlog搜索功能  
> 目前组内一版的binlog搜索功能，是离线任务处理的方式，好处是不会占用太大空间，缺点是处理时间较长。通过实时binlog解析过滤的方式，入ES可以快速搜索。需要进一步开发完善。
> 结合graylog可以实现阿里云RDS类似的数据追踪功能

rabbitmq介绍：http://seanlook.com/2018/01/06/rabbitmq-introduce/

maxwell介绍：http://seanlook.com/2018/01/13/maxwell-binlog/

数据已经生成，要完成 MySQL binlog 增量数据同步，还差一个消费者程序，将rabbitmq里面的消息取出来，在目标库重放：


目前这个增量程序重放动作是：
- binlog里面 insert 和 update 行，都变成 replace into 
- binlog里面 delele ，变成 delete ignore xxx limit 1
- alter/create，原封不动

所以如果表上没有主键或者唯一索引，是非常难搞定的，原本的update变成 replace into 多插入一条数据。当然如果把 update 事件改成 `update tables set f1=v1,f2=v2 where f1=v1,f2=vv2 limit 1` 也没毛病。

使用python3，安装rabbitmq 的python客户端即可：`pip install pika`
<!-- more -->

- **config.py**  
  增量程序的配置文件
  - db_info: 指定要写入的目标db
  - rabbitmq_conn_info: 增量数据的来源，rabbitmq连接信息 
  - rabbitmq_queue_bind: 指定怎么划分队列  
    默认共用一个队列，按照范例的的格式，根据表的binlog变更量，来划分队列
  - binary_columns: 指定有哪些是二进制列，因为需要根据提供的信息 base64_decode 成真实数据
    ```
    select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE from information_schema.`COLUMNS` 
    where DATA_TYPE in ('binary', 'varbinary', 'blob', 'bit', 'tinyblob', 'mediumblob', 'longblob')
    ```
    没有则留空
  - timestamp_columns: 指定哪些是timestamp类型的列，因为要处理时区的问题
    ```
    select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE from information_schema.`COLUMNS` where DATA_TYPE = 'timestamp'
    ```
    没有则留空
  - dbname_rewrite: 是否修改同步前后的 database name。
    没有修改则留空
    
- **mysql_sync.py**
  启动同步。可以用多线程，或多进程
  默认线程/进程数与 `rabbitmq_queue_bind` 指定的队列数相同。

- **pydbsync.py**
  通用的增量同步的核心程序。
  - `binlog_consumer.py` 是做some分库过程中用的核心程序，因为要对来自binlog的数据，根据 f_some_id 取模，插入到不同的 d_ec_someX 上，需要许多的特殊处理。


---

原文连接地址：http://seanlook.com/2018/01/14/rabbitmq-maxwell-consumer/

---