import Queue
import threading
import time
import copy

from Ganga.Core.GangaThread import GangaThread
from Ganga.Core.GangaRepository import RegistryKeyError, RegistryLockError

from Ganga.Utility.threads import SynchronisedObject

import Ganga.GPIDev.Credentials as Credentials
from Ganga.Core.InternalServices import Coordinator

from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName, getRuntimeGPIObject

from Ganga.GPIDev.Lib.Job.Job import lazyLoadJobStatus, lazyLoadJobBackend

# Setup logging ---------------
from Ganga.Utility.logging import getLogger, log_unknown_exception

from Ganga.Core import BackendError
from Ganga.Utility.Config import getConfig

from Ganga.Core.MonitoringComponent.GangaMC_WorkerThread import MonitoringWorkerThread
from Ganga.Core.MonitoringComponent.UpdateDict import UpdateDict, JobAction
from Ganga.Core.MonitoringComponent.GangaMC_Functions import _handleError, _checkBackend, makeCredChecker

log = getLogger()

config = getConfig("PollThread")
THREAD_POOL_SIZE = config['update_thread_pool_size']
Qin = Queue.Queue()
ThreadPool = []

def _makeThreadPool(threadPoolSize=THREAD_POOL_SIZE, daemonic=False):
    for i in range(THREAD_POOL_SIZE):
        t = MonitoringWorkerThread(name="MonitoringWorker_%s" % i)
        ThreadPool.append(t)
        t.start()

if config['autostart_monThreads']:
    _makeThreadPool()

updateDict_ts = SynchronisedObject(UpdateDict())



def stop_and_free_thread_pool(fail_cb=None, max_retries=5):
    """
     Clean shutdown of the thread pool. 
     A failed attempt to stop all the worker threads is followed by a call to the supplied callback which
     decides if a new attempt is performed or not.
     Example for a decision callback:      
          def myfail_cb():
             resp = raw_input("The cleanup procedures did not complete yet. Do you want to wait more?[y/N]")
             return resp.lower()=='y'
    """

    def join_worker_threads(threads, timeout=3):
        for t in threads:
            if t.isAlive():
                t.join(timeout)

    for i in range(len(ThreadPool)):
        Qin.put(JobAction('stop'))

    join_worker_threads(ThreadPool)

    # clean shutdown of stalled threads
    while True:
        if not fail_cb or max_retries <= 0:
            break
        stalled = [t for t in ThreadPool if t.isAlive()]
        if not stalled:
            break
        if fail_cb():  # continue?
            join_worker_threads(stalled, timeout=3)
            max_retries -= 1
        else:
            break

    del ThreadPool[:]

# purge Qin

def _purge_actions_queue():
    """
    Purge Qin: consume the current queued actions 
    Note: the producer (i.e JobRegistry_Monitor) should be stopped before the method is called
    """
    # purge the queue
    for i in range(len(Qin.queue)):
        try:
            action = Qin.get_nowait()
            # let the *stop* action in queue, otherwise the worker threads will
            # fail to terminate
            if isType(action, JobAction) and action.function == 'stop':
                Qin.put(action)
        #except Queue.Empty:
        #    break
        finally:
            pass

class JobRegistry_Monitor(GangaThread):

    """Job monitoring service thread."""
    uPollRate = 1.0
    minPollRate = 1.0

    def __init__(self, registry):
        GangaThread.__init__(self, name="JobRegistry_Monitor")
        log.debug("Constructing JobRegistry_Monitor")
        self.setDaemon(True)
        self.registry = registry
        self.__sleepCounter = 0.0
        self.__updateTimeStamp = time.time()
        self.progressCallback = lambda x: None
        self.callbackHookDict = {}
        self.clientCallbackDict = {}
        self.alive = True
        self.enabled = False

        # run the monitoring loop continuosly (steps=-1) or just a specified
        # number of steps(>0)
        self.steps = -1
        self.activeBackends = {}
        self.updateJobStatus = None
        self.errors = {}

        self.__setupJobActions()

        # synch objects
        # main loop mutex
        self.__mainLoopCond = threading.Condition()
        # cleanup synch
        self.__cleanUpEvent = threading.Event()
        # asynch mon loop running synch
        self.__monStepsTerminatedEvent = threading.Event()
        # event to signal the break of job lists iterators
        self.stopIter = threading.Event()
        self.stopIter.set()
        self._runningNow = False


    def __setupJobActions(self):

        # Create the default backend update method and add to callback hook.
        self.makeUpdateJobStatusFunction()

        # Add credential checking to monitoring loop
        for _credObj in Credentials._allCredentials.itervalues():
            log.debug("Setting callback hook for %s" % getName(_credObj))
            self.setCallbackHook(self.makeCredCheckJobInsertor(_credObj), {}, True, timeout=config['creds_poll_rate'])

        # Add low disk-space checking to monitoring loop
        log.debug("Setting callback hook for disk space checking")
        self.setCallbackHook(self.diskSpaceCheckJobInsertor, {}, True, timeout=config['diskspace_poll_rate'])





    def isEnabled( self, useRunning = True ):
        if useRunning:
            return self.enabled or self.__isInProgress()
        else:
            return self.enabled

    def shouldExit(self):
        return self.enabled is False and self.alive is False

    def run(self):
        """
        Main monitoring loop
        """
        import thread
        from Ganga.Core.MonitoringComponent import monitoring_thread_id
        monitoring_thread_id = thread.get_ident()
        del thread

        log.debug("Starting run method")

        while self.alive:
            log.debug("Monitoring Loop is alive")
            # synchronize the main loop since we can get disable requests
            with self.__mainLoopCond:
                log.debug("Monitoring loop __mainLoopCond")
                log.debug('Monitoring loop lock acquired. Running loop')
                # we are blocked here while the loop is disabled
                while not self.enabled and self.steps <= 0:
                    log.debug("Not enabled")
                    self.__cleanUp()
                    if not self.alive:  # stopped?
                        return
                    ## same as threading.Condition().wait()
                    self.__mainLoopCond.wait()


		## WHOLE POINT OF THE WHILE TRUE LOOP!!!
                log.debug("Launching Monitoring Step")
                self.__monStep()

		self.__sleep_and_check()


        log.debug("Monitoring Cleanup")
        # final cleanup
        self.__cleanUp()

	
    def __sleep_and_check(self):

       # delay here the monitoring steps according to the
       # configuration
       while self.__sleepCounter > 0.0:
           log.debug("Wait Condition")
           self.progressCallback(self.__sleepCounter)
           for i in range(int(self.uPollRate * 20)):
               if self.enabled:
                   self.__mainLoopCond.wait(self.uPollRate * 0.05)
               else:
                   if not self.alive:  # stopped?
                       self.__cleanUp()
                   # disabled, break to the outer while
                   break
           self.__sleepCounter -= self.uPollRate
           if not self.enabled:
               break

       else:
           log.debug("Run on Demand")
           # run on demand?
           if self.steps > 0:
                # decrement the remaining number of steps to run
                self.steps -= 1
                # requested number of steps executed, disabling...
                if self.steps <= 0:
                    self.enabled = False
                    # notify the blocking call of runMonitoring()
                    self.__monStepsTerminatedEvent.set()


    def __monStep(self):
        """
        A single monitoring step in the monitoring loop
        Note:
        Internally the step does not block, it produces *actions* that are queued to be run 
        in the thread pool
        """
        if not self.callbackHookDict:
            log.error('No callback hooks registered')
            return

        for cbHookFunc in self.callbackHookDict.keys():

            log.debug("\n\nProcessing Function: %s" % cbHookFunc)

            if cbHookFunc in self.callbackHookDict:
                cbHookEntry = self.callbackHookDict[cbHookFunc][1]
            else:
                log.debug("Monitoring KeyError: %s" % str(cbHookFunc))
                continue

            log.debug("cbHookEntry.enabled: %s" % str(cbHookEntry.enabled))
            log.debug("(time.time() - cbHookEntry._lastRun): %s" % str((time.time() - cbHookEntry._lastRun)))
            log.debug("cbHookEntry.timeout: %s" % str(cbHookEntry.timeout))

            diff = time.time() - cbHookEntry._lastRun

            if cbHookEntry.enabled and (diff) >= cbHookEntry.timeout:
                log.debug("%s >= %s" % (diff,  cbHookEntry.timeout))
                log.debug("Running monitoring callback hook function %s(**%s)" %(cbHookFunc, cbHookEntry.argDict))
                #self.callbackHookDict[cbHookFunc][0](**cbHookEntry.argDict)
                try:
                    self.callbackHookDict[cbHookFunc][0](**cbHookEntry.argDict)
                #except Exception as err:
                #    log.debug("Caught Unknown Callback Exception")
                #    log.debug("Callback %s" % str(err))
                finally:
                    pass
                cbHookEntry._lastRun = time.time()

            log.debug("time since %s last called: %ss" % (cbHookFunc, str(time.time() - cbHookEntry._lastRun)))

        log.debug("\n\nRunning runClientCallbacks")
        self.runClientCallbacks()

        self.__updateTimeStamp = time.time()
        self.__sleepCounter = config['base_poll_rate']

    def runMonitoring(self, jobs=None, steps=1, timeout=60, _loadCredentials=False):
        """
        Enable/Run the monitoring loop and wait for the monitoring steps completion.
        Parameters:
          steps:   number of monitoring steps to run
          timeout: how long to wait for monitor steps termination (seconds)
          jobs: a registry slice to be monitored (None -> all jobs), it may be passed by the user so ._impl is stripped if needed
        Return:
          False, if the loop cannot be started or the timeout occured while waiting for monitoring termination
          True, if the monitoring steps were successfully executed  
        Note:         
          This method is meant to be used in Ganga scripts to request monitoring on demand. 
        """

        log.debug("runMonitoring")

        if not isType(steps, int) and steps < 0:
            log.warning("The number of monitor steps should be a positive (non-zero) integer")
            return False

        if not self.alive:
            log.error("Cannot run the monitoring loop. It has already been stopped")
            return False

        # we don not allow the user's request the monitoring loop while the
        # internal services are stopped
        if not Coordinator.servicesEnabled:
            log.error("Cannot run the monitoring loop."
                      "The internal services are disabled (check your credentials or available disk space)")
            return False

        # if the monitoring is disabled (e.g. scripts)
        if not self.enabled:
            # and there are some required cred which are missing
            # (the monitoring loop does not monitor the credentials so we need to check 'by hand' here)
            if _loadCredentials is True:
                _missingCreds = Coordinator.getMissingCredentials()
            else:
                _missingCreds = False
            if _missingCreds:
                log.error("Cannot run the monitoring loop. The following credentials are required: %s" % _missingCreds)
                return False
	
	self._actuallyRunMon(jobs, steps, timeout, _loadCredentials)

    def _actuallyRunMon(self, jobs, steps, timeout, _loadCredentials):

        with self.__mainLoopCond:
            log.debug('Monitoring loop lock acquired. Enabling mon loop')
            if self.enabled or self.__isInProgress():
                log.error("The monitoring loop is already running.")
                return False

            if jobs is not None:
                m_jobs = jobs

                # additional check if m_jobs is really a registry slice
                # the underlying code is not prepared to handle correctly the
                # situation if it is not
                from Ganga.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice
                if not isType(m_jobs, RegistrySlice):
                    log.warning('runMonitoring: jobs argument must be a registry slice such as a result of jobs.select() or jobs[i1:i2]')
                    return False

                self.registry = m_jobs
                #log.debug("m_jobs: %s" % str(m_jobs))
                self.makeUpdateJobStatusFunction()

            log.debug("Enable Loop, Clear Iterators and setCallbackHook")
            # enable mon loop
            self.enabled = True
            # set how many steps to run
            self.steps = steps
            # enable job list iterators
            self.stopIter.clear()
            # Start backend update timeout checking.
            self.setCallbackHook(updateDict_ts.timeoutCheck, {}, True)

            log.debug("Waking up Main Loop")
            # wake up the mon loop
            self.__mainLoopCond.notifyAll()

        log.debug("Waiting to execute steps")
        # wait to execute the steps
        self.__monStepsTerminatedEvent.wait()
        self.__monStepsTerminatedEvent.clear()

        log.debug("Test for timeout")
        # wait the steps to be executed or timeout to occur
        if not self.__awaitTermination(timeout):
            log.warning("Monitoring loop started but did not complete in the given timeout.")
            # force loops termination
            self.stopIter.set()
            return False
        return True

    def enableMonitoring(self):
        """
        Run the monitoring loop continuously
        """

        if not self.alive:
            log.error("Cannot start monitoring loop. It has already been stopped")
            return False

        with self.__mainLoopCond:
            log.debug('Monitoring loop lock acquired. Enabling mon loop')
            self.enabled = True
            # infinite loops
            self.steps = -1
            # enable job list iterators
            self.stopIter.clear()
            log.debug('Monitoring loop enabled')
            # Start backend update timeout checking.
            self.setCallbackHook(updateDict_ts.timeoutCheck, {}, True)
            self.__mainLoopCond.notifyAll()

        return True

    def disableMonitoring(self, fail_cb=None, max_retries=5, stopping=False):
        """
        Disable the monitoring loop
        """

        if not self.alive and not stopping:
            log.error("Cannot disable monitoring loop. It has already been stopped")
            return False

        was_enabled = self.enabled

        if self.enabled:
            log.debug("Disabling Monitoring Service")

            self.enabled = False

        with self.__mainLoopCond:
            log.debug('Monitoring loop lock acquired. Disabling mon loop')
            self.enabled = False
            self.steps = 0
            self.stopIter.set()

            log.debug('Monitoring loop disabled')
            # wake up the monitoring loop
            self.__mainLoopCond.notifyAll()

        if not stopping and was_enabled is True and self._runningNow:
            log.info("Some tasks are still running on Monitoring Loop")
            log.info("Please wait for them to finish to avoid data corruption")

        stop_and_free_thread_pool(fail_cb, max_retries)

        return True

    def stop(self, fail_cb=None, max_retries=5):
        """
        Stop the monitoring loop
        Parameters:
         fail_cb : if not None, this callback is called if a retry attempt is needed
        """

        self.disableMonitoring(fail_cb, max_retries, stopping=True)

        if not self.alive:
            log.warning("Monitoring loop has already been stopped")
            return False
        else:
            self.alive = False

        self.__mainLoopCond.acquire()
        try:
            # wake up the monitoring loop
            self.__mainLoopCond.notifyAll()
        except Exception as err:
            log.error("Monitoring Stop Error: %s" % str(err))
        finally:
            self.__mainLoopCond.release()
        # wait for cleanup
        self.__cleanUpEvent.wait()
        self.__cleanUpEvent.clear()

        stop_and_free_thread_pool(fail_cb, max_retries)
        ###log.info( 'Monitoring component stopped successfully!' )

        return True

    def __cleanUp(self):
        """
        Cleanup function ran in JobRegistry_Monitor thread to disable the monitoring loop
        updateDict_ts.timeoutCheck can hold timeout locks that need to be released 
        in order to allow the pool threads to be freed.
        """

        # cleanup the global Qin
        _purge_actions_queue()
        # release timeout check locks
        timeoutCheck = updateDict_ts.timeoutCheck
        if timeoutCheck in self.callbackHookDict:
            updateDict_ts.releaseLocks()
            self.removeCallbackHook(timeoutCheck)
        # wake up the calls waiting for cleanup
        self.__cleanUpEvent.set()

    def __isInProgress(self):
        return self.steps > 0 or Qin.qsize() > 0

    def __awaitTermination(self, timeout=5):
        """
         Wait for resources to be cleaned up (threads,queue)
         Returns:
             False, on timeout
        """
        while self.__isInProgress():
            time.sleep(self.uPollRate)
            timeout -= self.uPollRate
            if timeout <= 0:
                return False
        return True

    def setCallbackHook(self, func, argDict, enabled, timeout=0):
        class CallbackHookEntry(object):

            def __init__(self, argDict, enabled=True, timeout=0):
                self.argDict = argDict
                self.enabled = enabled
                # the frequency in seconds
                self.timeout = timeout
                # record the time when this hook has been run
                self._lastRun = 0
        func_name = getName(func)
        log.debug('Setting Callback hook function %s.' % func_name)
        log.debug('arg dict: %s' % str(argDict))
        if func_name in self.callbackHookDict:
            log.debug('Replacing existing callback hook function %s with %s' % (str(self.callbackHookDict[func_name]), func_name))
        self.callbackHookDict[func_name] = [func, CallbackHookEntry(argDict=argDict, enabled=enabled, timeout=timeout)]

    def removeCallbackHook(self, func):
        func_name = getName(func)
        log.debug('Removing Callback hook function %s.' % func_name)
        if func_name in self.callbackHookDict:
            del self.callbackHookDict[func_name]
        else:
            log.error('Callback hook function does not exist.')

    def enableCallbackHook(self, func):
        func_name = getName(func)
        log.debug('Enabling Callback hook function %s.' % func_name)
        if func_name in self.callbackHookDict:
            self.callbackHookDict[func_name][1].enabled = True
        else:
            log.error('Callback hook function does not exist.')

    def disableCallbackHook(self, func):
        func_name = getName(func)
        log.debug('Disabling Callback hook function %s.' % func_name)
        if func_name in self.callbackHookDict:
            self.callbackHookDict[func_name][1].enabled = False
        else:
            log.error('Callback hook function does not exist.')

    def runClientCallbacks(self):
        for clientFunc in self.clientCallbackDict:
            log.debug('Running client callback hook function %s(**%s).' % (clientFunc, self.clientCallbackDict[clientFunc]))
            clientFunc(**self.clientCallbackDict[clientFunc])

    def setClientCallback(self, clientFunc, argDict):
        log.debug('Setting client callback hook function %s(**%s).' % (clientFunc, argDict))
        if clientFunc in self.clientCallbackDict:
            self.clientCallbackDict[clientFunc] = argDict
        else:
            log.error("Callback hook function not found.")

    def removeClientCallback(self, clientFunc):
        log.debug('Removing client callback hook function %s.' % clientFunc)
        if clientFunc in self.clientCallbackDict:
            del self.clientCallbackDict[clientFunc]
        else:
            log.error("%s not found in client callback dictionary." % getName(clientFunc))

    def __defaultActiveBackendsFunc(self):
        log.debug("__defaultActiveBackendsFunc")
        active_backends = {}
        # FIXME: this is not thread safe: if the new jobs are added then
        # iteration exception is raised
        fixed_ids = self.registry.ids()
        #log.debug("Registry: %s" % str(self.registry))
        log.debug("Running over fixed_ids: %s" % str(fixed_ids))
        for i in fixed_ids:
            try:
                j = stripProxy(self.registry(i))

                job_status = lazyLoadJobStatus(j)
                backend_obj = lazyLoadJobBackend(j)
                backend_name = getName(backend_obj)

                if job_status in ['submitted', 'running'] or (j.master and (job_status in ['submitting'])):
                    if self.enabled is True and self.alive is True:
                        active_backends.setdefault(backend_name, [])
                        active_backends[backend_name].append(j)
            #except RegistryKeyError as err:
            #    log.debug("RegistryKeyError: The job was most likely removed")
            #    log.debug("RegError %s" % str(err))
            #except RegistryLockError as err:
            #    log.debug("RegistryLockError: The job was most likely removed")
            #    log.debug("Reg LockError%s" % str(err))
            finally:
                pass

        summary = '{'
        for backend, these_jobs in active_backends.iteritems():
            summary += '"' + str(backend) + '" : ['
            for this_job in these_jobs:
                #stripProxy(this_job)._getWriteAccess()
                summary += str(stripProxy(this_job).id) + ', '#getFQID('.')) + ', '
            summary += '], '
        summary += '}'
        log.debug("Returning active_backends: %s" % summary)
        return active_backends

    def _checkActiveBackends(self, activeBackendsFunc):

        log.debug("calling function _checkActiveBackends")
        activeBackends = activeBackendsFunc()

        summary = '{'
        for this_backend, these_jobs in activeBackends.iteritems():
            summary += '"' + this_backend + '" : ['
            for this_job in these_jobs:
                summary += str(stripProxy(this_job).getFQID('.')) + ', '
            summary += '], '
        summary += '}'
        log.debug("Active Backends: %s" % summary)

        for jList in activeBackends.values():

            #log.debug("backend: %s" % str(jList))
            backendObj = jList[0].backend
            b_name = getName(backendObj)
            if b_name in config:
                pRate = config[b_name]
            else:
                pRate = config['default_backend_poll_rate']

            # TODO: To include an if statement before adding entry to
            #       updateDict. Entry is added only if credential requirements
            #       of the particular backend is satisfied.
            #       This requires backends to hold relevant information on its
            #       credential requirements.
            global Qin
            updateDict_ts.addEntry(backendObj, self.__checkBackend, jList, Qin, pRate)
            summary = str([stripProxy(x).getFQID('.') for x in jList])
            log.debug("jList: %s" % str(summary))

    def __checkBackend(self, backendObj, jobListSet, lock):

	global updateDict_ts
        self._runningNow = True

        currentThread = threading.currentThread()
        # timeout mechanism may have acquired the lock to impose delay.
        lock.acquire()

        updateDict_ts.clearEntry(getName(backendObj))

        log.debug("[Update Thread %s] Lock acquired for %s" % (currentThread, getName(backendObj)))

        jobList_fromset = list(filter(lambda x: x.status in ['submitting', 'submitted', 'running'], jobListSet))
        log.debug("[Update Thread %s] Updating %s with %s." % (currentThread, getName(backendObj), [x.id for x in jobList_fromset]))

        try:
            _checkBackend(self, backendObj, jobListSet)
        #except Exception as err:
        #    log.debug("Monitoring Loop Error: %s" % str(err))
        finally:
            lock.release()
            self._runningNow = False
            log.debug("[Update Thread %s] Lock released for %s." % (currentThread, getName(backendObj)))
            updateDict_ts.clearEntry(getName(backendObj))

            log.debug("Finishing _checkBackend")
        return


    def makeUpdateJobStatusFunction(self, makeActiveBackendsFunc=None):
        log.debug("makeUpdateJobStatusFunction")
        if makeActiveBackendsFunc is None:
            makeActiveBackendsFunc = self.__defaultActiveBackendsFunc

        if self.updateJobStatus is not None:
            self.removeCallbackHook(self.updateJobStatus)
        self.updateJobStatus = self._checkActiveBackends
        self.setCallbackHook(self._checkActiveBackends, {'activeBackendsFunc': makeActiveBackendsFunc}, True)
        log.debug("Returning")
        return self.updateJobStatus

    def makeCredCheckJobInsertor(self, credObj):
        def credCheckJobInsertor():
            def cb_Success():
                self.enableCallbackHook(credCheckJobInsertor)

            def cb_Failure():
                self.enableCallbackHook(credCheckJobInsertor)
                _handleError('%s checking failed!' % getName(credObj), getName(credObj), False)

            log.debug('Inserting %s checking function to Qin.' % getName(credObj))
            _action = JobAction(function=makeCredChecker(credObj),
                                callback_Success=cb_Success,
                                callback_Failure=cb_Failure)
            self.disableCallbackHook(credCheckJobInsertor)
            try:
                Qin.put(_action)
            #except Exception as err:
            #    log.debug("makeCred Err: %s" % str(err))
            #    cb_Failure("Put _action failure: %s" % str(_action), "unknown", True )
            finally:
                pass
        return credCheckJobInsertor

    def diskSpaceCheckJobInsertor(self):
        """
        Inserts the disk space checking task in the monitoring task queue
        """
        def cb_Success():
            self.enableCallbackHook(self.diskSpaceCheckJobInsertor)

        def cb_Failure():
            self.disableCallbackHook(self.diskSpaceCheckJobInsertor)
            _handleError('Available disk space checking failed and it has been disabled!', 'DiskSpaceChecker', False)

        log.debug('Inserting disk space checking function to Qin.')
        _action = JobAction(function=Coordinator._diskSpaceChecker,
                            callback_Success=cb_Success,
                            callback_Failure=cb_Failure)
        self.disableCallbackHook(self.diskSpaceCheckJobInsertor)
        try:
            Qin.put(_action)
        #except Exception as err:
        #    log.debug("diskSp Err: %s" % str(err))
        #    cb_Failure()
        finally:
            pass

    def updateJobs(self):
        if time.time() - self.__updateTimeStamp >= self.minPollRate:
            self.__sleepCounter = 0.0
        else:
            self.progressCallback("Processing... Please wait.")
            log.debug("Updates too close together... skipping latest update request.")
            self.__sleepCounter = self.minPollRate


