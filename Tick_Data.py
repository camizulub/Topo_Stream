import pandas as pd
from datetime import datetime, timedelta 
import sys
import os
from apscheduler.schedulers.background import BackgroundScheduler
from ib_insync import *

class Topo:

    def __init__(self):
        self.clientid = input('\tID: ')
        self.ticket = input('\tTicket: ')
        self.last = input('\tContract Expiration: ')
        self.exch = input('\tExchange: ')
        self.contract = Future(self.ticket, self.last, self.exch)
        self.days= {0: 'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday', 4:'Friday', 6:'Sunday'}

    def connect(self):
        ib.connect('127.0.0.1', 7497, clientId=self.clientid)

    def empty_df(self):
        self.df= pd.DataFrame(columns=['time','bid', 'bidSize', 'ask', 'askSize'])

    def onPendingTickers(self, ticker):
        """When the request of real time data is updated this function stores the date of the returned data,
        and creates a new Data Frame for 5 secs time bars"""

    #global df, df2
        self.df2 = util.df(ticker)
        self.df = self.df.append(self.df2.loc[:,['time','bid', 'bidSize', 'ask', 'askSize']])

    def recorder(self):
        #global ticker
        self.ticker = ib.reqTickByTickData(self.contract, 'BidAsk')
        ib.pendingTickersEvent += self.onPendingTickers

    def cancelation(self):
        ib.cancelTickByTickData(self.ticker.contract, 'BidAsk')

    def savedata(self):
        self.df.to_csv('C:/Users/MiloZB/Desktop/ES_{}_{}_{}.csv'.format(self.days[self.now.weekday()],self.now.hour,self.now.minute))

    def current_time(self):
        self.now = datetime.now()
        #Market Intraday Close
        if ((self.now.weekday() == 0)|(self.now.weekday() == 1)|(self.now.weekday() == 2) |(self.now.weekday() == 3)) & (self.now.hour == 8) & (self.now.minute == 16) & (self.now.second == 20):
            sun_open.pause()
            self.cancelation()
            self.savedata()
        #Market Intraday Open
        elif ((self.now.weekday() == 0)|(self.now.weekday() == 1)|(self.now.weekday() == 2) |(self.now.weekday() == 3)) & (self.now.hour == 8) & (self.now.minute == 16) & (self.now.second == 50):
            self.empty_df()
            self.recorder()
        #Market Friday Close
        elif (self.now.weekday()==2) & (self.now.hour == 8) & (self.now.minute == 17):
            self.cancelation()
            self.savedata()
            ib.disconnect()
            time_job.pause()
            try:
                sys.exit()
            except:
                print('Market is closed: Please press Ctrl + C to exit')

if __name__ == '__main__':

    ib = IB()
    juan = Topo()

    juan.connect()
    juan.empty_df()

    scheduler = BackgroundScheduler()
    time_job = scheduler.add_job(juan.current_time, 'interval', seconds=1)
    sun_open = scheduler.add_job(juan.recorder, 'cron', day_of_week=2, hour=8, minute=16) #Market Sunday Open
    scheduler.start()

    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            ib.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
