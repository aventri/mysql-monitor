import ConfigParser, json, logging, os.path, sys
from multiprocessing import Queue
from pymysql import err as pymysqlerror
from pymysql.constants import ER as pymysqlconst
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from monitor.processor import BinlogPackage, Processor


def load_config():
    if sys.path[0]:
        config_path = os.path.realpath(sys.path[0] + '/')
        data_path = os.path.realpath(sys.path[0] + '/data')
    else:
        # On OS X, if the project root is a symlink, realpath doesn't resolve correctly
        config_path = os.path.realpath(os.path.dirname(__file__) + '/conf')
        data_path = os.path.realpath(os.path.dirname(__file__) + '/../data')

    config = ConfigParser.RawConfigParser(
        {'host': '127.0.0.1',
         'user': 'admin',
         'password': 'admin',
         'ssl': False,
         'ca_certs': data_path + '/cacert.pem',
         'vhost': '/',
         'exchange': 'mysql-monitor',
         'exchange_type': 'fanout',
         'routing_key': 'monitor',
         'state_path': data_path + '/state.json',
         'delay': 30}
    )
    file_path = config_path + '/config.cfg'
    config.read(file_path)

    return config


class Monitor(object):
    def __init__(self, config):
        self._config = config   # type: ConfigParser.RawConfigParser
        self._state = State(self._config.get('monitor', 'state_path'))
        self._binlog_read_attempts = 0
        self._processor = None  # type: Processor
        self._queue = None      # type: Queue

    def run(self):
        """
        Start the monitor
        """
        self._setup_processors()
        self._binlog_read_attempts += 1
        try:
            self._listen()

        except pymysqlerror.InternalError as e:
            code, msg = e.args
            if code == pymysqlconst.MASTER_FATAL_ERROR_READING_BINLOG:
                # Possibly the binlog we're requesting was purged or we're asking for some
                # other invalid position; fallback to full log
                logging.error(
                    "Failed to read from %s position %d: %s",
                    self._state.binlog_filename,
                    self._state.binlog_position,
                    msg
                )
                # Try again from beginning
                if self._binlog_read_attempts < 2:
                    self._state.binlog_position = None
                    self._state.binlog_filename = None
                    self._listen()
            else:
                raise

        except KeyboardInterrupt:
            pass

        finally:
            if self._processor:
                self._processor.join()

    def _setup_processors(self):
        self._queue = Queue()
        self._processor = Processor(self._config, self._queue)
        # self._processor.daemon = False
        self._processor.start()

    def _listen(self):
        """
        Listen indefinitely to the MySQL binlog replication stream
        """
        binlog_stream = self._get_binlog_stream()
        try:
            for event in self._read_stream(binlog_stream):
                # For optimization only send the events we know we'll care to process
                if isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                    try:
                        # Reference the rows so they are calculated before queue
                        # (Queueing loses the internal data which can't be pickled)
                        assert event.rows
                        # Remove unpickleable properties from event because it's passed through a multiprocessing queue
                        del event._ctl_connection, event.packet, event.table_map, event.table_id, event.columns
                        e = BinlogPackage(event, binlog_stream.log_file, binlog_stream.log_pos)
                        self._queue.put(e)
                    except ValueError as e:
                        logging.error("Binlog event ValueError: " + e.message)
        finally:
            binlog_stream.close()

    @staticmethod
    def _read_stream(stream):
        """A way to use the stream iterator and continue if a decode exception is thrown"""
        stream_iterator = iter(stream)
        while True:
            try:
                yield next(stream_iterator)
            except UnicodeDecodeError:
                logging.warning("UnicodeDecodeError. Skipping to next binlog position")
                pass
            except pymysqlerror.DatabaseError as e:
                logging.error("DatabaseError: " + str(e))
                break
            except Exception:
                logging.exception("Error reading binlog stream")
                raise

    def _get_binlog_stream(self):
        mysql_settings = {
            'host': self._config.get('mysql', 'host'),
            'port': self._config.getint('mysql', 'port'),
            'user': self._config.get('mysql', 'user'),
            'passwd': self._config.get('mysql', 'password')
        }
        if self._state.binlog_filename is None or self._state.binlog_position is None:
            logging.info("Connecting to MySQL binlog (from beginning)...")
            binlog_stream = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=int(self._config.get('mysql', 'server_id')),
                blocking=True,
                only_schemas=[self._config.get('mysql', 'database')]
            )
        else:
            logging.info(
                "Connecting to MySQL binlog (continuing from %s position %d)",
                self._state.binlog_filename,
                self._state.binlog_position
            )
            binlog_stream = BinLogStreamReader(
                connection_settings=mysql_settings,
                server_id=int(self._config.get('mysql', 'server_id')),
                resume_stream=True,
                blocking=True,
                only_schemas=[self._config.get('mysql', 'database')],
                log_file=self._state.binlog_filename,
                log_pos=self._state.binlog_position
            )

        return binlog_stream


class State(object):
    """
    Application state persistence
    """
    def __init__(self, path):
        """
        Read and write application state to a file path. The binlog_filename and binlog_position values
        will automatically be read from the state file.
        :param path: File path
        """
        self._path = path
        self.binlog_filename = None
        self.binlog_position = None
        self.load()

    def load(self):
        """
        Read state from disk
        """
        if not os.path.exists(self._path):
            return

        f = open(self._path, 'r')
        state_str = f.read()
        f.close()
        if len(state_str):
            state = json.loads(state_str)
            self.binlog_filename = state['binlog_file']
            self.binlog_position = state['binlog_position']

    def save(self):
        """
        Save state to disk
        """
        state = {
            'binlog_file': self.binlog_filename,
            'binlog_position': self.binlog_position
        }
        f = open(self._path, 'w')
        json.dump(state, f)
        f.flush()
        f.close()

