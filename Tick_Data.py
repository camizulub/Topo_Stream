import pandas as pd
from datetime import datetime, timedelta 
import os
import sys
import time
from apscheduler.schedulers.background import BackgroundScheduler
from ib_insync import *

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=17)
contract = Future("ES", "201909", "GLOBEX")

days= {0: 'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 6:'Sunday'}

def empty_df():
    global df
    df= pd.DataFrame(columns=['time','bid', 'bidSize', 'ask', 'askSize'])

def onPendingTickers(ticker):
    """When the request of real time data is updated this function stores the date of the returned data,
    and creates a new Data Frame for 5 secs time bars"""

    global df, df2
    df2 = util.df(ticker)
    df = df.append(df2.loc[:,['time','bid', 'bidSize', 'ask', 'askSize']])

def recorder():
    global ticker
    ticker = ib.reqTickByTickData(contract, 'BidAsk')
    ib.pendingTickersEvent += onPendingTickers

def cancelation():
    ib.cancelTickByTickData(ticker.contract, 'BidAsk')

def savedata(now):
    df.to_csv('C:/Users/MiloZB/Desktop/ES_{}_{}_{}.csv'.format(days[now.weekday()],now.hour,now.minute))

def current_time():
    global now
    now = datetime.now()
    #Market Close
    if ((now.weekday() == 0)|(now.weekday() == 1)|(now.weekday() == 2) |(now.weekday() == 3)) & (now.hour == 15) & (now.minute == 14) & (now.second == 0):
        sun_open.pause()
        cancelation()
        savedata(now)
    #Market Open
    elif ((now.weekday() == 0)|(now.weekday() == 1)|(now.weekday() == 2) |(now.weekday() == 3)) & (now.hour == 15) & (now.minute == 31) & (now.second == 0):
        empty_df()
        recorder()
    #Market Friday Close
    elif (now.weekday()==4) & (now.hour == 15) & (now.minute == 59):
        cancelation()
        savedata(now)
        time_job.pause()
        try:
            sys.exit()
        except:
            print('Market is closed: Please press Ctrl + C to exit')

if __name__ == '__main__':

    empty_df()
    current_time()
    scheduler = BackgroundScheduler()
    time_job = scheduler.add_job(current_time, 'interval', seconds=1)
    sun_open = scheduler.add_job(recorder, 'cron', day_of_week=1, hour=14, minute=30) #Market Sunday Open
    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            ib.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()