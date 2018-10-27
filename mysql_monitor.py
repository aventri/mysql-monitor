#!/usr/bin/env python

import logging, time
from monitor.core import load_config, Monitor


def main():
    config = load_config()
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        level=getattr(logging, config.get('monitor', 'log_level').upper(), 10),
    )
    delay = config.get('monitor', 'delay_start')
    if delay:
        logging.debug("Waiting %s seconds to start...", delay)
        time.sleep(float(delay))
    monitor = Monitor(config)
    monitor.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
