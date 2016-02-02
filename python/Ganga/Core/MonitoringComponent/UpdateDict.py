import threading
import time

from Ganga.GPIDev.Base.Proxy import stripProxy, getName

from contextlib import contextmanager

# Setup logging ---------------
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig

log = getLogger()
config = getConfig("PollThread")

# The JobAction class encapsulates a function, its arguments and its post result action
# based on what is defined as a successful run of the function.

class JobAction(object):

    def __init__(self, function, args=(), kwargs={},
                 success=(True, ),
                 callback_Success=lambda: None,
                 callback_Failure=lambda: None):
        self.function = function
        self.args = args

        self.kwargs = kwargs
        self.success = success
        self.callback_Success = callback_Success
        self.callback_Failure = callback_Failure
        self.thread = None
        self.description = ''

# Each entry for the updateDict_ts object (based on the UpdateDict class)
# is a _DictEntry object.
class _DictEntry(object):

    def __init__(self, backendObj, jobSet, entryLock, timeoutCounterMax):
        self.backendObj = backendObj
        self.jobSet = jobSet
        self.entryLock = entryLock
        self.timeoutCounterMax = timeoutCounterMax
        self.timeoutCounter = timeoutCounterMax - 0.01
        self.timeLastUpdate = 0.0

    def updateActionTuple(self):
        return self.backendObj, self.jobSet, self.entryLock

@contextmanager
def release_when_done(rlock):
    """
    A ``threading.Lock`` (or ``RLock`` etc.) object cannot be used in a context
    manager if the lock acquisition is non-blocking because the acquisition can
    fail and so the contents of the ``with`` block should not be run.

    To allow sensible ``release()`` behaviour in these cases, acquire the lock
    as usual with ``lock.acquire(False)`` and then wrap the required code with
    this context manager::

        if lock.acquire(blocking=False):
            with release_when_done(lock):
                #some code
                pass

    Attributes:
        rlock: A lock object which can have ``release()`` called on it,
    """
    try:
        yield rlock
    finally:
        rlock.release()

class UpdateDict(object):

    """
    This serves as the Update Table. Is is meant to be used 
    by wrapping it as a SynchronisedObject so as to ensure thread safety.
    """

    def __init__(self):
        self.table = {}

    def _runEntry(self, backendObj, backendCheckingFunction, jobList, jSet, Qin, timeoutMax=None):
        try:
            backend_name = getName(backendObj)
            jSetSize = len(jSet)
            log.debug("Lock acquire successful. Updating jSet %s with %s." %
                    ([stripProxy(x).getFQID('.') for x in jSet], [stripProxy(x).getFQID('.') for x in jobList]))
            jSet.update(jobList)
            # If jSet is empty it was cleared by an update action
            # i.e. the queue does not contain an update action for the
            # particular backend_name any more.
            if jSetSize:  # jSet not cleared
                log.debug("%s backend job set exists. Added %s to it." % (backend_name, [stripProxy(x).getFQID('.') for x in jobList]))
            else:
                Qin.put(JobAction(backendCheckingFunction, self.table[backend_name].updateActionTuple()))
                log.debug("Added new %s backend update action for jobs %s." %
                        (backend_name, [stripProxy(x).getFQID('.') for x in self.table[backend_name].updateActionTuple()[1]]))
        #except Exception as err:
        #    log.error("addEntry error: %s" % str(err))
        finally:
            pass

    def addEntry(self, backendObj, backendCheckingFunction, jobList, Qin, timeoutMax=None):
        if not jobList:
            return
        if timeoutMax is None:
            timeoutMax = config['default_backend_poll_rate']
        #log.debug("*----addEntry()")

        backend_name = getName(backendObj)
        if backend_name in self.table:
            backendObj, jSet, lock = self.table[backend_name].updateActionTuple()
        else:  # New backend.
            self.table[backend_name] = _DictEntry(backendObj, set(jobList), threading.RLock(), timeoutMax)
            # queue to get processed
            Qin.put(JobAction(backendCheckingFunction, self.table[backend_name].updateActionTuple()))
            log.debug("**Adding %s to new %s backend entry." % ([stripProxy(x).getFQID('.') for x in jobList], backend_name))
            return True

        # backend_name is in Qin waiting to be processed. Increase it's list of jobs
        # by updating the table entry accordingly. This will reduce the
        # number of update requests.
        # i.e. It's like getting a friend in the queue to pay for your
        # purchases as well! ;p
        log.debug("*: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" %
                (backend_name, lock._RLock__count, lock._is_owned(), [x.id for x in jobList], Qin.qsize()))
        try:
            if lock.acquire(False):

                try:
                    self._runEntry(backendObj, backendCheckingFunction, jobList, jSet, Qin, timeoutMax)
                #except Exception as err:
                #    log.warning("ERROR RUNNING ENTRY: %s" % str(err))
                finally:
                    pass

                log.debug("**: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" %
                        (backend_name, lock._RLock__count, lock._is_owned(), [stripProxy(x).getFQID('.') for x in jobList], Qin.qsize()))
                return True
        finally:
            pass

    def clearEntry(self, backend_name):
        if backend_name in self.table:
            entry = self.table[backend_name]
        else:
            log.error("Error clearing the %s backend. It does not exist!" % backend_name)

        entry.jobSet = set()
        entry.timeoutCounter = entry.timeoutCounterMax

    def timeoutCheck(self):
        for backend_name, entry in self.table.items():
            # timeoutCounter is reset to its max value ONLY by a successful update action.
            #
            # Initial value and subsequent resets by timeoutCheck() will set the timeoutCounter
            # to a value just short of the max value to ensure that it the timeoutCounter is
            # not decremented simply because there are no updates occuring.
            diff = (entry.timeoutCounter - entry.timeoutCounterMax)
            if diff*diff<0.01 and entry.entryLock.acquire(False):
                with release_when_done(entry.entryLock):
                    log.debug("%s has been reset. Acquired lock to begin countdown." % backend_name)
                    entry.timeLastUpdate = time.time()

                    # decrease timeout counter
                    if entry.timeoutCounter <= 0.0:
                        entry.timeoutCounter = entry.timeoutCounterMax - 0.01
                        entry.timeLastUpdate = time.time()
                        log.debug("%s backend counter timeout. Resetting to %s." % (backend_name, entry.timeoutCounter))
                    else:
                        _l = time.time()
                        entry.timeoutCounter -= _l - entry.timeLastUpdate
                        entry.timeLastUpdate = _l

    def isBackendLocked(self, backend_name):
        if backend_name in self.table:
            return bool(self.table[backend_name].entryLock._RLock__count)
        else:
            return False

    def releaseLocks(self):
        for backend_name in self.table.keys():
            if entry.entryLock._is_owned():
                entry.entryLock.release()


