from ast import literal_eval as leval
from collections import defaultdict, deque
from datetime import datetime as dt, timedelta
from dateutil import tz
from functools import reduce


def datetime_from_epoch_ns(epoch_ns):
    return dt.utcfromtimestamp(epoch_ns / 1e9).replace(tzinfo=tz.tzutc())


epoch2dt = lambda a=0: datetime_from_epoch_ns(a * 1e9)
replaced = lambda a, d: reduce(lambda b, c: b.replace(*c), d.items(), a)


class TxQ(object):
    """
    Deserializes blockchain msgs and maintains rolling Q of transactions 
    """
    
    EDICT = dict(true='True', false='False', null='None')
    SATOSHI = 1e-8
    
    def __init__(self, sig, update_cb, logger):
        self.sig = sig
        self.timedelta = timedelta(hours=sig['PopHours'])
        self.Q = deque()

        # rolling set of hashes seen
        self.hash = set()
        # rolling mapping address => send / recv qty
        self.tx = defaultdict(lambda: [0, 0])
        # rolling mapping address => send / recv count
        self.count = defaultdict(lambda: [0, 0])

        self.addr_tag = defaultdict(set) 
        self.ts = defaultdict(epoch2dt)

        key_func = dict(
            gross=lambda a: a[1][0] - a[1][1],
            net=lambda a: -abs(sum(a[1])),
            max=lambda a: -max(a[1][1], -a[1][0]),
            count=lambda a: -sum(self.count[a[0]]))
        self.sorted = lambda a: sorted(a, key=key_func[sig['Sort']])
        self.update_cb = update_cb
        self.logger = logger

    def on_msg(self, msg):
        # deserialize msg
        q = leval(replaced(msg, self.EDICT))
        xhash = q.get('x', {}).get('hash')
        # if hash not yet seen, process transactions
        if xhash and (xhash not in self.hash):
            self.hash.add(xhash)
            tx = self.parse_tx(q['x'])
            if tx:
                for k in tx['prev']:
                    self.tx[k][0] -= tx['prev'][k]
                    self.count[k][0] += 1
                for k in tx['out']:
                    self.tx[k][1] += tx['out'][k]
                    self.count[k][1] += 1
                self.on_tx()
                self.update_cb(self)

    def on_tx(self):
        # remove expired transactions from Q
        Q = []
        if self.Q:
            ts = self.Q[-1]['ts'] - self.timedelta
            keys = set()
            while self.Q and self.Q[0]['ts'] < ts:
                tx = self.Q.popleft()
                for k in tx['prev']:
                    self.tx[k][0] += tx['prev'][k]
                    self.count[k][0] -= 1
                    keys.add(k)
                for k in tx['out']:
                    self.tx[k][1] -= tx['out'][k]
                    self.count[k][1] -= 1
                    keys.add(k)
                self.hash.remove(tx['hash'])
                Q.append(tx)
            for k in keys:
                if not sum(self.count[k]):
                    del self.tx[k]
                    del self.count[k]
        return Q

    def add_tx(self, daddr, tx, ts):
        # update send / recv dictionary
        if 'addr' in tx:
            key = tx['addr']
            daddr[key] += tx['value'] * self.SATOSHI
            if 'addr_tag' in tx:
                self.addr_tag[key].add(tx['addr_tag'])
            self.ts[key] = max(
                *[key in self.ts and (self.ts[key], ts) or (ts,)])
        elif tx['value']:
            self.logger.warning('No addr [%s]', tx)

    def parse_tx(self, tx):
        self.logger.info('Q-size [%s]', len(self.Q))
        self.last_ts = 'time' in tx and epoch2dt(tx['time'])
        self.logger.info('Time [%s] hash [%s]', self.last_ts, tx['hash'])

        # append transaction to Q
        dout = defaultdict(float)
        dprev = defaultdict(float)
        for t in tx['out']:
            self.add_tx(dout, t, self.last_ts)
        for t in tx['inputs']:
            if 'prev_out' in t:
                self.add_tx(dprev, t['prev_out'], self.last_ts)
        tx = dict(ts=self.last_ts, hash=tx['hash'], out=dout, prev=dprev)

        if self.sig['MinQty']:
            for d in (tx['out'], tx['prev']):
                for k, v in [*d.items()]:
                    if (v < self.sig['MinQty'] and
                        k not in self.tx and
                        k not in self.addr_tag):
                        del d[k]
        if tx['out'] or tx['prev']:
            self.Q.append(tx)
            return tx
        return None

    def encoding(self, _=None):
        # log and display top addresses
        keys = [k for k, _ in self.sorted(self.tx.items())[:self.sig['Top']]]
        keys = set(keys + [k for k in self.addr_tag if k in self.tx])
        items = self.sorted([(k, self.tx[k]) for k in keys])
        trunk = lambda a, sz=35: len(a) < sz and a or '%s...' % a[:sz - 1]
        fmt = '%s %37s %s %8d %+16.8f %+16.8f = %+16.8f%s%s'
        for k, v in items:
            star = ' *' [k in self.Q[-1]['prev'] or k in self.Q[-1]['out']]
            args = (self.ts[k], trunk(k), '<>' [sum(v) < 0],)
            args += (sum(self.count[k]), v[0], v[1], sum(v),)
            args += (star, self.addr_tag.get(k, ''),)
            self.logger.info(fmt, *args)
            print(fmt % args)