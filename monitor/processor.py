import datetime
from ConfigParser import RawConfigParser
from multiprocessing import Process, Queue
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from monitor.amqp import Amqp, BufferedAmqp, Message


class Processor(Process):
    """
    Handle database events as they stream to us from the MySQL binlog.
    Database changes are evaluated and sent to a queue.
    """

    def __init__(self, config, event_queue):
        """
        :param RawConfigParser config: Application configuration
        :param Queue event_queue: Queue to pass binlog events into this process
        """
        super(Processor, self).__init__(name=type(self).__name__)
        self.config = config
        self.amqp = None                # type: Amqp
        self.event_queue = event_queue  # type: Queue
        self.message_queue = Queue()    # For passing messages to the AMQP processes

    def run(self):
        self.amqp = BufferedAmqp(self.config, self.message_queue)
        try:
            # self.amqp.daemon = False
            self.amqp.start()
            while True:
                self.process_event(self.event_queue.get())
        except KeyboardInterrupt:
            pass
        finally:
            self.amqp.join()

    def process_event(self, package):
        """
        Handle a single binlog event. These are sent from the main loop in dataservices_monitor.py

        :param BinlogPackage package:
        """
        if isinstance(package.event, WriteRowsEvent):
            self._process_write_rows(package)
        elif isinstance(package.event, UpdateRowsEvent):
            self._process_update_rows(package)
        elif isinstance(package.event, DeleteRowsEvent):
            self._process_delete_rows(package)

    def _process_write_rows(self, package):
        """
        Handle a write rows binlog event

        :param BinlogPackage package:
        """
        event = package.event
        i = 0
        count = len(event.rows)
        for row in event.rows:
            i += 1
            values = row['values']
            for col in values:
                values[col] = self._sanitize_value(values[col])

            message = Message(
                package.id() + ':' + str(i),
                {
                    "type": "row_insert",
                    "table_name": event.table,
                    "keys": self._get_primary_keys(event, row, 'values'),
                    "values": values,
                    "binlog_timestamp": event.timestamp
                }
            )
            if i == count:
                # Last item, save state after message is queued
                message.binlog_filename = package.filename
                message.binlog_position = package.position
            self.message_queue.put(message)

    def _process_update_rows(self, package):
        """
        Handle an update rows binlog event

        :param BinlogPackage package:
        """
        event = package.event
        i = 0
        count = len(event.rows)
        for row in event.rows:
            i += 1
            before_values = {}
            after_values = {}
            for col in row["before_values"].keys():
                if row["before_values"][col] != row["after_values"][col]:
                    before_values[col] = self._sanitize_value(row["before_values"][col])
                    after_values[col] = self._sanitize_value(row["after_values"][col])

            message = Message(
                package.id() + ':' + str(i),
                {
                    "type": "row_update",
                    "table_name": event.table,
                    "keys": self._get_primary_keys(event, row, 'after_values'),
                    "before_values": before_values,
                    "after_values": after_values,
                    "binlog_timestamp": event.timestamp
                }
            )
            if i == count:
                # Last item, save state after message is queued
                message.binlog_filename = package.filename
                message.binlog_position = package.position
            self.message_queue.put(message)

    def _process_delete_rows(self, package):
        """
        Handle a delete rows binlog event

        :param BinlogPackage package:
        """
        event = package.event
        i = 0
        count = len(event.rows)
        for row in event.rows:
            i += 1
            message = Message(
                package.id() + ':' + str(i),
                {
                    "type": "row_delete",
                    "table_name": event.table,
                    "keys": self._get_primary_keys(event, row, 'values'),
                    "binlog_timestamp": event.timestamp
                }
            )
            if i == count:
                # Last item, save state after message is queued
                message.binlog_filename = package.filename
                message.binlog_position = package.position
            self.message_queue.put(message)

    @staticmethod
    def _sanitize_value(value):
        if isinstance(value, datetime.datetime) or isinstance(value, datetime.date) or isinstance(value, datetime.time):
            value = value.isoformat()
        elif isinstance(value, datetime.timedelta):
            value = str(value)
        return value

    @staticmethod
    def _get_primary_keys(event, row, row_key):
        """
        Get a dictionary of primary key: value for an event / row
        :param event:
        :param row:
        :param row_key:
        :return: dict
        """
        keys = {}
        if hasattr(event, 'primary_key'):
            if type(event.primary_key) is list or type(event.primary_key) is tuple:
                for key in event.primary_key:
                    if key in row[row_key]:
                        keys[key] = row[row_key][key]
            else:
                if event.primary_key in row[row_key]:
                    keys[event.primary_key] = row[row_key][event.primary_key]

        # Convert any complex values to simple types
        for key, value in keys.iteritems():
            if not isinstance(value, (int, unicode, str)):
                try:
                    keys[key] = str(value)
                except TypeError:
                    continue
                except UnicodeEncodeError:
                    continue

        return keys


class BinlogPackage(object):
    """Container for a binlog event and meta data"""
    def __init__(self, event, filename, position):
        """
        :param event: WriteRowsEvent, UpdateRowsEvent, or DeleteRowsEvent from the pymysqlreplication lib
        :param str filename: Binlog filename
        :param int position: Binlog file position
        """
        self.event = event
        self.filename = filename
        self.position = position

    def id(self):
        return self.filename + ':' + str(self.position)
