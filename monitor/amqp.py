import ConfigParser, json, logging, socket, time
import pika
import pika.exceptions
from multiprocessing import Process, Queue
from Queue import Empty
from ssl import CERT_OPTIONAL

from monitor.delayqueue import *
import monitor.core


class Message(object):
    """
    Data for a message to be pushed to RabbitMQ
    """
    def __init__(self, message_id, data, binlog_filename=None, binlog_position=None):
        """
        :param str message_id:
        :param dict data: Message data to be serialized into a JSON string
        :param str binlog_filename:
        :param int binlog_position:
        """
        self.message_id = message_id
        self.data = data
        self.binlog_filename = binlog_filename
        self.binlog_position = binlog_position
        # String version of the data, excluding timestamps, is used to test for uniqueness in delayqueue.py
        data_filtered = {k: v for k, v in data.iteritems() if k not in ['timestamp', 'binlog_timestamp']}
        self.unique_data_str = json.dumps(data_filtered, sort_keys=True)


class Amqp(Process):
    """
    Generic queue object that should be sub-classed for specific queues
    """

    def __init__(self, config, config_section, message_process_queue):
        """
        :param ConfigParser.RawConfigParser config: Application configuration 
        :param str config_section: Configuration section which should be looked at for connection info
        :param Queue message_process_queue: Inter-process queue where messages to be sent are pushed for this process to handle
        """
        super(Amqp, self).__init__(name=type(self).__name__)
        self._config = config  # type: ConfigParser.RawConfigParser
        self._config_section = config_section
        self._retry_count = 0  # On message delivery failure keep track of retry attempts
        self._message_process_queue = message_process_queue  # type: Queue
        self._last_sent_time = 0.0
        self._state = monitor.core.State(config.get('monitor', 'state_path'))
        self._amqp_exchange = config.get(self._config_section, 'exchange')
        self._amqp_exchange_type = config.get(self._config_section, 'exchange_type')
        self._amqp_routing_key = config.get(self._config_section, 'routing_key')
        self._connection = None
        self._channel = None
        # Confirmed delivery will throw warning if there are no client queues connected to the exchange
        self._confirm_queued = False

    def __del__(self):
        if self._connection:
            self._connection.close()

    def run(self):
        try:
            self.connect()
            while True:
                try:
                    message = self._message_process_queue.get(False)  # type: Message
                    self._publish(message)
                except Empty:
                    self._heartbeat()
                    time.sleep(0.5)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(e.message)

    def connect(self):
        logging.info("Connecting to %s...", self._config_section)
        parameters = pika.ConnectionParameters(
            host=self._config.get(self._config_section, 'host'),
            port=self._config.getint(self._config_section, 'port'),
            ssl=self._config.getboolean(self._config_section, 'ssl'),
            ssl_options={
                'ca_certs': self._config.get(self._config_section, 'ca_certs'),
                'cert_reqs': CERT_OPTIONAL
            },
            virtual_host=self._config.get(self._config_section, 'vhost'),
            credentials=pika.PlainCredentials(
                self._config.get(self._config_section, 'user'), self._config.get(self._config_section, 'password')
            ),
            connection_attempts=5,
            retry_delay=5
        )
        self._connection = pika.BlockingConnection(parameters)
        channel_number = 1
        self._channel = self._connection.channel(channel_number)
        # self._channel.confirm_delivery()
        self._setup_channel()

    def _setup_channel(self):
        logging.info("Configuring AMQP exchange...")
        self._channel.exchange_declare(
            exchange=self._amqp_exchange,
            exchange_type=self._amqp_exchange_type,
            passive=False,
            durable=True,
            auto_delete=False
        )

    def _publish(self, message):
        """
        Write a message to the connected RabbitMQ exchange
        :param Message message:
        """
        data = message.data
        message_id = message.message_id
        # Append UNIX timestamp to every message
        timestamp = int(round(time.time()))
        data['timestamp'] = timestamp
        # Set last sent time now to avoid stacking up heartbeat messages if connection is closed
        self._last_sent_time = time.time()

        try:
            published = self._channel.basic_publish(
                self._amqp_exchange,
                self._amqp_routing_key,
                json.dumps(data),
                pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=2,
                    message_id=message_id
                ),
                mandatory=self._confirm_queued
            )

            # Confirm delivery or retry
            if published:
                self._retry_count = 0
                # Save state often, but not for every message.
                # In production we may process hundreds per second.
                if message.binlog_filename and timestamp % 2 == 0:
                    self._state.binlog_filename = message.binlog_filename
                    self._state.binlog_position = message.binlog_position
                    self._state.save()
            else:
                logging.warning("Message publish to queue could not be confirmed.")
                raise EnqueueException("Message publish to queue could not be confirmed.")

        except (EnqueueException, pika.exceptions.AMQPChannelError, pika.exceptions.AMQPConnectionError,
                pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed, pika.exceptions.UnexpectedFrameError,
                pika.exceptions.UnroutableError, socket.timeout) as e:
            self._retry_count += 1
            if self._retry_count < 5:
                logging.warning(
                    "Reconnecting to %s and sending message again (Attempt # %d)",
                    self._config_section, self._retry_count
                )
                if self._connection.is_open:
                    try:
                        self._connection.close()
                    except:
                        pass
                self.connect()
                self._publish(message)
            else:
                raise e

    def _heartbeat(self):
        """
        Send a heartbeat message through RabbitMQ if we've been inactive for a time.

        This is necessary because our connections to Rabbit time out when quiet for too long.  This may be fixed in the
        latest pre-release updates to the pika library.  The proper solution is for pika to internally use the
        heartbeat feature of RabbitMQ.  This method is a workaround, although it also lets our clients on the other
        side of queues see that we're up and running.

        See https://github.com/pika/pika/issues/418
        See https://stackoverflow.com/questions/14572020/handling-long-running-tasks-in-pika-rabbitmq
        """
        if time.time() - self._last_sent_time > 30:
            self._publish(Message('hb-' + str(time.time()), {"type": "heartbeat"}))


class BufferedAmqp(Amqp):
    """
    Amqp database updates as they are analyzed by an instance of Processor
    into a fanout queue for subscribers.

    Includes a configurable time delay buffer. This is useful to allow time for a slave DB to write updates
    before a worker processes a queue message.
    """
    def __init__(self, config, message_process_queue):
        """
        :param ConfigParser.RawConfigParser config: Application configuration 
        :param Queue message_process_queue: Queue where messages to be sent are pushed for this process to handle
        """
        super(BufferedAmqp, self).__init__(config, 'amqp', message_process_queue)
        self.buffer = None  # type: MessageDelayQueue
        if config.get('monitor', 'delay') and int(config.get('monitor', 'delay')) > 0:
            self.buffer_delay = int(config.get('monitor', 'delay'))
        else:
            self.buffer_delay = 0

    def run(self):
        try:
            self.connect()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logging.error(e.message)
            return

        if self.buffer_delay:
            self.buffer = MessageDelayQueue(self.buffer_delay)
        try:
            # Loop indefinitely to process queues
            self._publish_from_queues()
        except KeyboardInterrupt:
            # Flush whatever is left in queues
            self._flush_queues()

    def _publish_from_queues(self):
        """
        Loop indefinitely on the process queues and publish messages
        """
        while True:
            message_q_empty = False
            buffer_empty = False
            # First pick off the in-bound inter-process message queue
            try:
                message = self._message_process_queue.get(False)  # type: Message
                if self.buffer:
                    self.buffer.put(message)
                else:
                    self._publish(message)
            except Empty:
                message_q_empty = True

            # Then check the delay buffer
            if self.buffer:
                try:
                    message = self.buffer.pop()  # type: Message
                    self._publish(message)
                except (MessageDelayQueueEmpty, MessageDelayQueueNotReady):
                    # Nothing to do
                    buffer_empty = True

            if message_q_empty and buffer_empty:
                self._heartbeat()
                time.sleep(0.5)

    def _flush_queues(self):
        """
        Process whatever is left in the queue
        """
        if self.buffer:
            while True:
                try:
                    message = self.buffer.pop(True)  # type: Message
                    self._publish(message)
                except MessageDelayQueueEmpty:
                    break
        while True:
            try:
                message = self._message_process_queue.get(False)
                self._publish(message)
            except Empty:
                break


class EnqueueException(Exception):
    pass

