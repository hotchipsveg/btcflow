from bisect import bisect_left
from datetime import datetime as dt
import math
from operator import sub

from txq import TxQ


def plot_matrix(img, ts, buckets):
    print('%s: %s - %.2f' % (dt.now(), ts, img['ticker']))
    plt = img['plt']
    plt.clf()
    plt.figure(1)
    plt.suptitle('BTC = %.2f' % img['ticker'])
    ax = plt.gca()
    ax.set_title(ts)
    ax.set_aspect('equal')
    ax.yaxis.set_ticks_position('right')
    img = ax.imshow(
        [[*map(lambda a: a > 1 and math.log(a) or 0, m)] for m in buckets],
        cmap=plt.get_cmap(img['color']),
        interpolation=img['interp'])
    plt.pause(1)
    return img


class TxFlow(TxQ):
    """
    Bins addresses based on avg transaction size and displays heat plot
    of transaction volume with rows representing receiving bins and
    columns representing sending bins
    SE = transfers between small accounts
    NW = transfers between large accounts
    Above SE - NW diagonal = accumulation
    Below SE - NW diagonal = distribution    
    """
    
    def __init__(self, sig, update_cb, logger):
        super(TxFlow, self).__init__(sig, update_cb, logger)
        bins = sig['Bins']
        self.buckets = [[0] * bins for _ in range(bins)]
        self.bins = []
        self.qty = []

    def binner(self, key_groups, keys, vals):
        if not self.bins or len(keys) != self.bins[-1]:
            # adjust bin qtys
            self.logger.debug('Binning ...')
            bin_sz = len(keys)
            n = self.sig['Bins'] - 1
            base = math.exp(math.log(.5*len(keys)) / n)
            self.bins = [pow(base, i) for i in range(n)]
            self.bins.append(bin_sz)
            self.logger.info('Bins [%s]', self.bins)
            self.qty = [-round(vals[int(bin - 1)]) for bin in self.bins]
            self.logger.info('Qty [%s]', self.qty)
        return [[bisect_left(self.bins, keys.index(k)) for k in some_keys]
                for some_keys in key_groups]

    def bin_tx(self, tx, mult=1):
        if not tx['prev'] or not tx['out']:
            self.logger.debug('Prev [%s] Out [%s]', tx['prev'], tx['out'])
            return

        # calculate address weights 
        transp = lambda a: [*zip(*a)]
        tx_prev, tx_out = map(lambda a: transp(tx[a].items()),
                              ('prev', 'out'))
        self.logger.info('Values [%s] [%s]', tx_prev[1], tx_out[1])
        wt_prev, wt_out = [[i * mult / sum(v) for i in v]
                           for v in (tx_prev[1], tx_out[1])]
        self.logger.info('Weights [%s] [%s]', wt_prev, wt_out)

        if mult > 0:
            # bin addresses based on avg transaction size
            volume = sorted([(k, sub(*self.tx[k]) / sum(self.count[k]))
                             for k in self.count],
                            key=lambda a: a[1])
            tx['bin_prev'], tx['bin_out'] = self.binner(
                (tx_prev[0], tx_out[0]), *transp(volume))
        
        # distribute transaction volume over buckets
        bin_prev, bin_out = tx['bin_prev'], tx['bin_out']
        self.logger.debug('Bins prev [%r] out [%s]', bin_prev, bin_out)
        for b_prev, v_prev in zip(bin_prev, tx_prev[1]):
            for wt, b_out in zip(wt_out, bin_out):
                self.buckets[b_out][b_prev] += wt * v_prev

    def on_tx(self):
        Q = super(TxFlow, self).on_tx()
        if self.Q:
            self.bin_tx(self.Q[-1])
        for tx in Q:
            self.bin_tx(tx, -1)
        return Q

    def draw(self, plot_args):
        for b in self.buckets:
            self.logger.info(b)
        plot_matrix(plot_args, self.last_ts, self.buckets)
