import pandas as pd
from datetime import datetime
import os

from apscheduler.schedulers.background import BackgroundScheduler
from ib_insync import *

def __init__():
    
    client_id = 19
    ib = IB()
    ib.connect('127.0.0.1', 7498, clientId=client_id)
    contract = Forex('EURUSD')

def tick():




if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()