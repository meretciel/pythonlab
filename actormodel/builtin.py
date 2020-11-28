
import multiprocessing as mp
import threading
import time
from os import path
from abc import abstractmethod, ABCMeta
import json

import sys
import logging
logging.basicConfig(
    stream=sys.stdout,
    format='%(asctime)s.%(msecs)d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.DEBUG)

ACTOR_MESSAGE_RETRIEVAL_INTERVAL_IN_SECOND = 0.01
MESSAGE_DISTRIBUTOR_WAIT_INTERVAL_IN_SECOND = 0.05

class Message:
    def __init__(self, sender, receiver, data):
        self.sender = sender
        self.receiver = receiver
        self.data = data

    def getSender(self):
        return self.sender

    def getReceiver(self):
        return self.receiver

    def getData(self):
        return self.data

    def __repr__(self):
        return "Message(sender={},receiver={},data={})".format(
            self.sender,self.receiver,self.data)

    @staticmethod
    def serialize(message):
        return json.dumps({
            "s" : message.getSender(),
            "r" : message.getReceiver(),
            "d" : message.getData()
        })

    @staticmethod
    def deserialize(strMessage):
        d = json.loads(strMessage)
        return Message(d['s'], d['r'], d['d'])

class ActorRef:
    def __init__(self, actorPath, context):
        self.path = actorPath
        self.messageDistributor = context.getMessageDistributor()

    def getPath(self):
        return self.path

    def __repr__(self):
        return "ActorRef(path={})".format(self.path)

    def __hash__(self):
        return self.path.__hash__()

    def __eq__(self, other):
        return self.getPath() == other.getPath()

    def send(self, receiverActorRef, data):
        m = Message(self.path, receiverActorRef.getPath(), data)
        self.messageDistributor.submitMessage(m)


class MessageDistributor:
    def __init__(self):
        self.pathToActorMailboxMap = dict()
        self.queue = mp.Manager().Queue()

    def register(self, actor):
        self.pathToActorMailboxMap[actor.getPath()] = actor.getMailbox()

    def unregister(self, actor):
        if actor.getPath() in self.pathToActorMailboxMap:
            self.pathToActorMailboxMap.pop(actor.getPath())

    def run(self):
        if not self.queue.empty():
            m = Message.deserialize(self.queue.get())
            logging.debug("[MessageDistribution] Distribute message {}".format(m))
            mailbox = self.pathToActorMailboxMap.get(m.getReceiver())
            if mailbox:
                mailbox.put(m)

    def submitMessage(self, message):
        assert isinstance(message, Message)
        self.queue.put(Message.serialize(message))

class Context:
    def __init__(self, path="/"):
        self.messageDistributor = MessageDistributor()
        self.actors = []
        self.processes = []
        self.backgroundThread = None
        self.stopEventOfBackgroundTread = None
        self.path = path

    def register(self, actor):
        self.actors.append(actor)
        self.messageDistributor.register(actor)


    def start(self):
        for actor in self.actors:
            p = mp.Process(target=actor.run)
            self.processes.append(p)
            p.start()

    def join(self):
        self.backgroundThread.join()
        [p.join for p in self.processes]

    def terminate(self):
        [p.terminate() for p in self.processes]

    @staticmethod
    def _startBackgroundTaskLoop(runnable, waitTimeInSecond, stopEvent):
        while not stopEvent.wait(waitTimeInSecond):
            runnable.run()


    def startBackgroundTaskLoop(self):
        self.stopEventOfBackgroundTread = threading.Event()
        self.backgroundThread = threading.Thread(target=Context._startBackgroundTaskLoop,
                             args=(self.messageDistributor,
                                   MESSAGE_DISTRIBUTOR_WAIT_INTERVAL_IN_SECOND,
                                   self.stopEventOfBackgroundTread))
        self.backgroundThread.start()

    def __enter__(self):
        logging.debug("__enter__ is called.")
        self.startBackgroundTaskLoop()
        self.start()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.debug("__exit__ is called.")
        self.stopEventOfBackgroundTread.set()
        self.backgroundThread.join()
        self.terminate()

    def getMessageDistributor(self):
        return self.messageDistributor

    def createActorRefByName(self, name):
        return ActorRef(path.join(self.path, name), context)

    def createActorRefByPath(self, path_in):
        return ActorRef(path_in, context)

    def createMailbox(self):
        return mp.Manager().Queue()

class Actor(metaclass=ABCMeta):
    def __init__(self, name, context):
        self.name = name
        self.mailbox = context.createMailbox()
        self.actorRef = context.createActorRefByName(name)
        self.lastSender = None
        self.messageDistributor = context.getMessageDistributor()
        context.register(self)
        self.context = context

    def getPath(self):
        return self.actorRef.getPath()

    def getActorRef(self):
        return self.actorRef

    def getMailbox(self):
        return self.mailbox

    def getLastSender(self):
        return self.lastSender

    def tryGetMessage(self):
        if not self.mailbox.empty():
            return self.mailbox.get()

    def run(self):
        while True:
            message = self.tryGetMessage()
            if message:
                self.lastSender = self.context.createActorRefByPath(message.getSender())
                self.onReceive(message.data)
            time.sleep(ACTOR_MESSAGE_RETRIEVAL_INTERVAL_IN_SECOND)

    @abstractmethod
    def onReceive(self, message):
        pass

def countDown(n):
    while n > 0:
        n -= 1

class Counter(Actor):
    def onReceive(self, message):
        logging.debug("[{}] Received {}".format(self.getPath(), message))
        startTime = time.time()
        countDown(message)
        endTime = time.time()
        self.getActorRef().send(self.getLastSender(), "finished")
        logging.debug("[{}] Time elapsed: {}".format(self.getPath(), endTime - startTime))

class ResultCollector(Actor):
    def __init__(self, startTime, n, name, context):
        super(ResultCollector, self).__init__(name, context)
        self.startTime = startTime
        self.count = 0
        self.n = n

    def onReceive(self, message):
        self.count += 1
        if self.count == self.n:
            endTime = time.time()
            timeElapsed = endTime - self.startTime
            logging.debug("Time elapsed (actor model): {}".format(timeElapsed))

if __name__ == '__main__':
    mp.set_start_method('fork')
    n = 200000000
    nWorkers = 5

    startTime = time.time()
    countDown(n)
    endTime = time.time()
    print("=" * 70)
    print("Time elapsed (single threaded): {}".format(endTime - startTime))
    print("=" * 70)

    startTime = time.time()
    countDown(n / nWorkers)
    endTime = time.time()
    print("=" * 70)
    print("Time elapsed (single threaded n / {}): {}".format(nWorkers, endTime - startTime))
    print("=" * 70)

    startTime = time.time()
    p = mp.Process(target=countDown, args=(n / nWorkers, ))
    p.start()
    p.join()
    endTime = time.time()
    print("=" * 70)
    print("Time elapsed (single threaded in child process): {}".format(endTime - startTime))
    print("=" * 70)

    startTime = time.time()
    threads = []
    for i in range(nWorkers):
        t = threading.Thread(target=countDown, args=(n / nWorkers,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    endTime = time.time()
    print("=" * 70)
    print("Time elapsed (multi-threaded): {}".format(endTime - startTime))
    print("=" * 70)

    startTime = time.time()
    context = Context()

    workers = []
    for i in range(nWorkers):
        workers.append(Counter("woker-" + str(i), context).getActorRef())

    resultCollector = ResultCollector(startTime, nWorkers, "actor-ResultCollector", context).getActorRef()

    with context as c:
        logging.debug("enter the with block")
        for worker in workers:
            resultCollector.send(worker, n / nWorkers)
        c.join()
