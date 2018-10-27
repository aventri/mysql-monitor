MySQL Monitor
=============

MySQL Monitor is an application which continuously watches a MySQL database for data updates and publishes information 
about those changes to a message queue on RabbitMQ.  It does this by connecting as a slave to the database and 
transforming the events which come through the replication log into JSON messages which are pushed to a 
publish/subscribe queue.

We built this system at [Aventri](https://www.aventri.com/) to support highly scalable, near real-time data processing by multiple 
systems that rely on a central database. This application effectively lets you write the equivalent of database triggers 
in any programming language or system without adding any load to your master database.  For example, make API calls triggered by data updates, or 
perform data transformations concurrently with multiple workers.

Requirements
------------

We've included a docker configuration for easy setup and deployment.  The `docker-compose.yaml` file will help you
run a complete local environment for demonstration and testing.

The application has been tested with MySQL 5.6 and 5.7. Support for MySQL versions is dependent on the 
[python-mysql-replication](https://github.com/noplay/python-mysql-replication) library.

The MySQL database the monitor connects to must have binary logging turned on.  The binary log
format must be ROW.  For example, 

    ; my.conf
    server-id=1
    log_bin=binlog
    binlog_format=ROW
    innodb_flush_log_at_trx_commit=1

The database must also have a user with slave replication permissions.  This is the user to set in `config.cfg`.

    GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'user'@'host';

### Application Dependencies

- Python 2.7
- [python-mysql-replication](https://github.com/noplay/python-mysql-replication)
- [pika](https://github.com/pika/pika)

Running the Monitor
-------------------

The `setup.sh` script will copy `config-example.cfg` to `config.cfg` if it doesn't already exist. Set the MySQL and 
RabbitMQ connection parameters.  Then run `python mysql_monitor.py`.

Building from `docker/monitor/Dockerfile` will do this for you.

How It Works
------------

The MySQL Monitor is able to handle a high volume of data changes by separating its work into multiple processes.
The "monitor" process listens to MySQL's binlog and pushes information about each event into an inter-process queue. 
The "processor" picks up the events and transforms each into one or more JSON messages which are pushed into the next 
inter-process queue. The "amqp" process keeps a persistent connection to RabbitMQ and enqueues the JSON messages.

The "amqp" process keeps an optional time-delay buffer used to remove duplicate messages.  This can reduce the volume 
of messages when the same rows are repeatedly updated in a short period of time.

In the environment the monitor was originally built for, we've watched it easily handle hundreds of messages per second. 
The de-duping buffer reduced message volume by at least 10%.

The system will periodically save it's stated to the `data` directory. This allows the monitor to continue from the 
last binlog position it processed after being restarted.  The current binlog position is only recorded after the related 
messages are sent to RabbitMQ.  This guarantees no data is lost of the connection to RabbitMQ is dropped.

Messages
--------------

### Heartbeat

    {
        "timestamp": 1539710711, 
        "type": "heartbeat"
    }

### Insert row

    {
        "timestamp": 1539710711, 
        "binlog_timestamp": 1539710709, 
        "type": "row_insert"
        "table_name": "employee", 
        "keys": {
            "id": 2760
        }, 
        "values": {
            "fname": "abc", 
            "lname": "def", 
            "id": 2760
        },
    }

### Update row

    {
        "timestamp": 1539710711, 
        "binlog_timestamp": 1539710709, 
        "type": "row_update",
        "table_name": "employee", 
        "keys": {
            "id": 1903
        }, 
        "before_values": {
            "fname": "Ldurlqgfqv"
        },
        "after_values": {
            "fname": "Ldurlqgfqvsdf"
        }
    }

### Delete row

    {
        "timestamp": 1539710711, 
        "binlog_timestamp": 1539710709, 
        "type": "row_delete", 
        "table_name": "employee", 
        "keys": {
            "id": 5
        }
    }
    