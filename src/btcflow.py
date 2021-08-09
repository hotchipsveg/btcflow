#!/usr/bin/env python3
import argparse
from json import dumps
import logging
from logging.handlers import TimedRotatingFileHandler
import requests
import signal
import sys
from websocket import WebSocketApp

from txflow import TxFlow


signal.signal(signal.SIGINT, lambda sig, _: sys.exit(0))

dumpop = lambda a: dumps({'op': 'unconfirmed_%s' % a})


LOGGER = logging.getLogger(__name__)
FH = TimedRotatingFileHandler(
    __file__.replace('py', 'log'),
    when='d', interval=1, backupCount=15)

LOG_FMT = """
%(levelname)s %(asctime)s %(process)d %(filename)s:%(lineno)s> %(message)s'
"""

FH.setFormatter(logging.Formatter(LOG_FMT.strip()))
LOGGER.addHandler(FH)


class WebSignal(object):

    def __init__(self, signature, plt):
        self.logger = LOGGER
        self.handler_cls = TxFlow
        self.handler = None
        self.signature = signature
        self.plot_args = dict(plt=plt,
                              interp=self.signature['Interp'],
                              color=self.signature['Color'])

    def update_ticker(self, ccy='USD'):
        r = requests.get(self.signature['TickerUrl'])
        if r.ok:
            ticker = r.json()[ccy]
            self.logger.info('Ticker: %s', ticker)
            self.logger.info('Bbo: [%s,%s]', ticker['sell'], ticker['buy'])
            return ticker
        self.logger.warning('Ticker request failed with reason: %s', r.reason)
        return {}

    def on_update(self, handler):
        self.plot_args['ticker'] = self.update_ticker().get('last')
        handler.draw(self.plot_args)
        handler.encoding()

    def start(self):
        self.handler = self.handler_cls(self.signature,
                                        self.on_update, self.logger)
        self._sub(self.signature['StartArg'])

    def _sub(self, arg=None):
        on_err = lambda ws, error='Conn closed': (
            _ for _ in ()).throw(ConnectionResetError(error))
        while True:
            try:
                ws = WebSocketApp(
                    self.signature['Url'], on_error=on_err, on_close=on_err,
                    on_message=lambda ws, msg: (self.handler and
                                                self.handler.on_msg(msg)),
                    on_open=lambda ws: ws.send(arg))
                if self.signature['HttpProxyHost']:
                    ws.run_forever(
                        http_proxy_host=self.signature['HttpProxyHost'],
                        http_proxy_port=self.signature['HttpProxyPort'],)
                else:
                    ws.run_forever()
            except ConnectionResetError as error:
                self.logger.warning('Reconnecting [%s]', error)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='arg')
    parser.add_argument('--top', type=str, default=30)
    parser.add_argument('--bins', type=int, default=16)
    parser.add_argument('--min_qty', type=float, default=1)
    parser.add_argument('--pop_hours', type=int, default=1)
    parser.add_argument('--sort', type=str, default='net')
    parser.add_argument('--range', type=int, default=1000)
    parser.add_argument('--interp', type=str, default='bicubic')
    parser.add_argument('--color', type=str, default='hot')
    parser.add_argument('--loglevel', type=str, default='INFO')
    parser.add_argument('--ticker_url', type=str,
                        default='https://blockchain.info/ticker')
    parser.add_argument('--url', type=str,
                        default='wss://ws.blockchain.info/inv')
    parser.add_argument('--start_arg', type=str, default=dumpop('sub'))
    parser.add_argument('--stop_arg', type=str, default=dumpop('unsub'))
    parser.add_argument('--http_proxy_host', type=str, default='')
    parser.add_argument('--http_proxy_port', type=int, default=3128)
    parser.add_argument('--agg', type=str, default='TKAgg')
    ns = parser.parse_args()
        
    sig = dict((''.join([w.title() for w in k.split('_')]), v)
               for k, v in ns._get_kwargs())
    LOGGER.setLevel(getattr(logging, ns.loglevel))
    LOGGER.debug(sig)
    print(sig)

    import matplotlib
    matplotlib.use(ns.agg)
    from matplotlib import pyplot as plt
    plt.ion()
    signal = WebSignal(sig, plt)
    signal.start()