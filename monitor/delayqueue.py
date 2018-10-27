from datetime import datetime, timedelta
import json

import monitor.amqp


class MessageDelayQueueEmpty(Exception):
    """Raised by `get()` if queue is empty"""
    pass


class MessageDelayQueueNotReady(Exception):
    """raised by `get()` if queue is not empty, but it's not yet time for next item"""
    pass


class MessageDelayQueue(object):
    """
    A queue that maintains a *unique* set of messages to be retrieved after a time delay.
    Message.data, excluding timestamps, is considered for uniqueness.
    """
    def __init__(self, delay = 0):
        self.queue = []
        self.queue_set = set()
        self.delay = delay

    def put(self, message):
        """
        :param monitor.amqp.Message message:
        """
        if message.unique_data_str not in self.queue_set:
            self.queue_set.add(message.unique_data_str)
            self.queue.append((datetime.utcnow() + timedelta(seconds=self.delay), message))

    def pop(self, skip_delay=False):
        """
        :return: monitor.amqp.Message
        """
        if not len(self.queue):
            raise MessageDelayQueueEmpty
        if not skip_delay and datetime.utcnow() < self.queue[0][0]:
            raise MessageDelayQueueNotReady
        message = self.queue.pop(0)[1]  # type: monitor.amqp.Message
        self.queue_set.remove(message.unique_data_str)
        return message
