import os
import dask.dataframe as dd
from luigi import Task, build
from ..helperfiles.task import TargetOutput
from pandas.plotting import register_matplotlib_converters
from ..tools.historicrates import GetHistoricRates
import plotly.express as px

register_matplotlib_converters()


class TradeDistributionReport(Task):

    # Set output location
    # output = TargetOutput(os.getenv('local_location') + '/image')
    # Placeholder for plot
    fig = object


    def run(self):
        def absUnits(units):
            return abs(units)

        def buyUnits(units):
            if (units > 0):
                return units
            else:
                return 0

        def sellUnits(units):
            if (units < 0):
                return abs(units)
            else:
                return 0

        def tradeType(units):
            if (units >= 0):
                return "buy"
            else:
                return "sell"


        dsk = dd.read_parquet(os.getenv("local_location") + "trading_history/*.parquet", columns=["time", "type", "instrument", "units"])
        dsk["time"] = dsk["time"].astype("M8[D]")
        dsk['type'] = dsk['type'].astype('str')
        dsk['instrument'] = dsk['instrument'].astype('str')
        dsk["day"] = dsk["time"].dt.day_name()
        # get only rows with ORDER_FILL
        dsk = dsk[(dsk['type'] == 'ORDER_FILL')]

        #TEMP
        dsk = dsk[(dsk['instrument'] == 'USD_THB') | (dsk['instrument'] == 'EUR_USD')]


        dsk['units'] = dsk['units'].astype('int')
        # Find the buy and sell rows
        dsk['buy_units'] = dsk.apply(lambda x: buyUnits(x['units']), axis=1)
        dsk['sell_units'] = dsk.apply(lambda x: sellUnits(x['units']), axis=1)
        dsk['action'] = dsk.apply(lambda x: tradeType(x['units']), axis=1)

        dsk['units'] = dsk.apply(lambda x: absUnits(x['units']), axis=1)
        dsk.set_index('day')

        dsk = dsk[["day", "type", "instrument", "units", "buy_units", "sell_units", "action"]]

        self.trade_distribution(dsk)


    def trade_distribution(self, dsk):
        # plotly is having trouble with dask datframes but it will accept an array
        df = dsk.compute().values

        # plot the data
        # TODO: accpet instruments as a Parameter
        fig = px.bar(df, x=0, y=3, color=6, barmode="group", facet_row=2, facet_col=0,
                     category_orders={0: ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                                      2: ["USD_THB","EUR_USD",
                                                     # "BCO_USD",
                                                     # "EUR_CAD",
                                                     # "EUR_USD",
                                                     # "USD_SEK",
                                                     # "USD_NOK",
                                                     # "USD_THB",
                                                     # "XCU_USD"
                                                     ]})

        # Save the image
        #fig.write_image(file=os.getenv("local_location") + "images/" + "trade_distribution.png", format='png')

        # return the figure to the caller
        self.fig = fig
