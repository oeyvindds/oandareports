import numpy as np
import os
import oandapyV20
import oandapyV20.endpoints.instruments as instruments
import oandapyV20.endpoints.orders as orders
from time import sleep
from dotenv import load_dotenv

class MovingAverageTrader:

    def __init__(self, short_timeframe=50, long_timeframe=200, instrument='EUR_USD', granularity='S5', volume = 1000):
        self.params = {'granularity': granularity, 'count': long_timeframe}
        self.accountID = os.environ['ACCOUNT_ID']
        self.client = oandapyV20.API(access_token=os.environ['TOKEN'])
        self.short_timeframe = short_timeframe
        self.long_timeframe = long_timeframe
        self.instrument = instrument
        self.granularity = granularity
        self.volume = volume
        self.stock = 0

    def get_candles(self):
        output = instruments.InstrumentsCandles(instrument=self.instrument, params=self.params)
        candles = self.client.request(output)

        return candles

    def create_trade(self, position):
        data = {"order": {"units": position, "instrument": self.instrument, "timeInForce": "FOK", "type": "MARKET",
                          "positionFill": "DEFAULT"}}
        r = orders.OrderCreate(self.accountID, data=data)
        self.client.request(r)

        return r.response

    def create_average(self, instrument_list):
        short_list = []
        long_list = []
        # TODO: Make more Pythonic
        for i, r in enumerate(instrument_list['candles'][-self.long_timeframe:]):
            long_list.append(instrument_list['candles'][i]['mid']['c'])
        for y, r in enumerate(instrument_list['candles'][-self.short_timeframe:]):
            short_list.append(instrument_list['candles'][y]['mid']['c'])

        return np.array(short_list).astype(np.float).mean(), np.array(long_list).astype(np.float).mean()

n = MovingAverageTrader()
load_dotenv()

while 1 == 1:
    candle_list = n.get_candles()
    short_list, long_list = n.create_average(candle_list)

    if short_list > long_list:
        if n.stock < n.volume:
            n.create_trade((n.volume / 10))
            n.stock += (n.volume / 10)
            print('Just bought {} {}'.format(n.volume / 10, n.instrument))

    if long_list > short_list:
        if n.stock > - n.volume:
            n.create_trade((- n.volume / 10))
            n.stock -= (n.volume / 10)
            print('Just sold {} {}'.format(n.volume / 10, n.instrument))

    print('As of now we own {} {}'.format(n.stock, n.instrument))
    sleep(5)
