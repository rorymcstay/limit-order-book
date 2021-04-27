import unittest
import heapq
import math
import queue
from datetime import datetime
from enum import Enum

class Side(Enum):
    Buy = 1
    Sell = 2

class OrdType(Enum):
    Market = 1
    Limit = 2

class ExecType(Enum):
    New = 1
    Trade = 2
    Reject = 3
    UnsolicitedCancel = 4

def almost_equal(num1, num2):
    return math.isclose(num1, num2)

def less_than(num1, num2):
    return not math.isclose(num1, num2) and num1 < num2

def greater_than(num1, num2):
    return not math.isclose(num1, num2) and num1 > num2

class Order:
    def __init__(self, **kwargs):
        self.price = kwargs.get('Price')
        self.size = kwargs.get('Size')
        self.ordType = kwargs['OrdType']
        self.side = kwargs.get('Side')
        self.orderid = kwargs.get('OrderID')
        self.arrivalTime = datetime.now()

        self.cumQty = 0
        self.leavesQty = self.size
        self.lastQty = 0
        self.lastPrice = 0

    def __lt__(self, other):
        if self.ordType == other.orderType and self.ordType == OrdType.Market:
            if self.size == other.size:
                retVal =  self.arrivalTime > other.arrivalTime
            retVal =  self.size < other.size
        elif other.ordType == OrdType.Market:
            retVal =  False
        else:
            if almost_equal(self.price, other.price):
                if self.size == self.size:
                    retVal =  self.arrivalTime > other.arrivalTime
                retVal =  self.size < other.size
            retVal =  less_than(self.price, other.price) if self.side == Side.Buy else greater_than(self.price, other.price)
        return not retVal


    def __repr__(self):
        return f'Price={self.price} Size={self.size} OrdType={self.ordType.name} Side={self.side.name} LastPrice={self.lastPrice} LastQty={self.lastQty} LeavesQty={self.leavesQty} CumQty={self.cumQty} OrderID={self.orderid}'


class ExecReport:
    def __init__(self, order, **kwargs):
        self.order = order
        self.execType = kwargs.get('ExecType')

    def __repr__(self):
        return f'{self.execType.name}: {self.order}'

class EventType(Enum):
    OrderSingle = 1
    OrderAmend = 2
    TradingStatus = 3

class Event:
    eventType = None
    tradingStatus = None
    execReport = None
    orderNew = None
    def __init__(self, eventType):
        self.eventType = eventType


def read_params(chunks):
    params = {}
    for chunk in chunks:
        pair = chunk.split('=')
        name = pair[0]
        val = pair[1]
        if name == 'Price':
            val = float(val)
        elif name == 'Size':
            val = int(val)
        elif name == 'Side':
            val = Side[val]
        elif name == 'OrdType':
            val = OrdType[val]
        elif name == 'LeavesQty':
            val == int(val)
        elif name == 'CumQty':
            val == int(val)
        elif name == 'LastQty':
            val = int(val)
        elif name == 'LastPrice':
            val = float(val)
        else:
            raise Exception(f'Error, {name}={val} not recognised')
        params.update({name: val})
    return params

def read_order(string):
    chunks = string.split(' ')
    params = read_params(chunks)
    return Order(**params)

def read_event(string):
    topic = EventType[string.split(':')[0].strip()]
    event = Event(topic)
    if topic == EventType.OrderSingle:
        order = read_order(string.split(':')[1].strip())
        event.orderNew = order
    else:
        raise Exception(f'EventType {topic} not supported')
    return event

class OrderQueue():
    _data = []
    def get(self) -> Order:
        return heapq.heappop(self._data)
    def put(self, order: Order):
        heapq.heappush(self._data, order)
    def remove(self, order: Order):
        self._data.remove(order)
    def __iter__(self):
        while self._data:
            yield self.get()

def can_cross(buyOrder: Order, sellOrder: Order):
    cross = False
    crossPrice = -1
    crossQty = 0
    if buyOrder.ordType == OrdType.Market:
        cross = True
        crossPrice = sellOrder.price
    elif sellOrder.ordType == OrdType.Market:
        cross = True
        crossPrice == buyOrder.price
    if buyOrder.price > sellOrder.price:
        cross = True
        crossPrice = sellOrder.price
    crossQty = min(buyOrder.size, sellOrder.size)
    return cross, crossQty, crossPrice


class OrderBook:

    _buyOrders = OrderQueue()
    _sellOrders = OrderQueue()
    _execReports = queue.Queue()
    _orderIds = 1

    def _match(self):
        done = False
        for buyOrder, sellOrder in zip(self._buyOrders, self._sellOrders):
            canCross, crossQty, crossPrice = can_cross(buyOrder, sellOrder)
            if canCross:
                self._doTrade(buyOrder, crossQty, crossPrice)
                self._doTrade(sellOrder, crossQty, crossPrice)
            if buyOrder.leavesQty == 0:
                done = False
            else:
                self._buyOrders.put(buyOrder)
                done = True
            if sellOrder.leavesQty == 0:
                done = False
            else:
                self._sellOrders.put(sellOrder)
                done = True
            if done:
                break


    def _doTrade(self, order: Order, qty, price):
        order.cumQty += qty
        order.leavesQty -= qty
        order.lastQty = qty
        order.lastPrice = price
        execReport = ExecReport(order, execType=ExecType.Trade)
        self._execReports.put(execReport)

    def evaluate(self, event: Event):
        handler = getattr(self, f'on{event.eventType.name}')
        handler(event)
        self._match()

    def onOrderSingle(self, event):
        order = event.orderNew
        order.orderid = self._orderIds
        self._orderIds += 1
        if order.side == Side.Buy:
            self._buyOrders.put(order)
        else:
            self._sellOrders.put(order)
        execReport = ExecReport(order, ExecType=ExecType.New)
        self._execReports.put(execReport)


def getMsg(orderbook) -> str:
    try:
        return str(orderbook._execReports.get_nowait())
    except queue.Empty as ex:
        return 'NONE'

def putMsg(orderbook: OrderBook, message: str):
    event = read_event(message)
    orderbook.evaluate(event)

class TestOrderBook(unittest.TestCase):

    def test_orders_can_be_crossed(self):
        book = OrderBook()
        putMsg(book, "OrderSingle: Price=1.0 Size=100 OrdType=Limit Side=Sell")
        self.assertEqual(getMsg(book), "New: Price=1.0 Size=100 OrdType=Limit Side=Sell LastPrice=0 LastQty=0 LeavesQty=100 CumQty=0 OrderID=1")
        putMsg(book, "OrderSingle: Price=1.0 Size=100 OrdType=Limit Side=Buy")
        self.assertEqual(getMsg(book), "New: Price=1.0 Size=100 OrdType=Limit Side=Buy LastPrice=0 LastQty=0 LeavesQty=100 CumQty=0 OrderID=2")
        self.assertEqual(getMsg(book), "Trade: Price=1.0 Size=100 OrdType=Limit Side=Sell LastPrice=1.0 LastQty=100 LeavesQty=0 CumQty=100 OrderID=1")
        self.assertEqual(getMsg(book), "Trade: Price=1.0 Size=100 OrdType=Limit Side=Buy LastPrice=1.0 LastQty=100 LeavesQty=0 CumQty=100 OrderID=2")
        self.assertEqual(book.getDepth(Side.Buy), 0)
        self.assertEqual(book.getDepth(Side.Sell), 0)
        getMsg(book, 'NONE')


if __name__ == '__main__':
    unittest.main()
