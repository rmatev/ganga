import Queue
import threading

from Ganga.Core.GangaThread import GangaThread

from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName, getRuntimeGPIObject

# Setup logging ---------------
from Ganga.Utility.logging import getLogger

from Ganga.Core.MonitoringComponent.UpdateDict import JobAction

log = getLogger()

class MonitoringWorkerThread(GangaThread):

    def __init__(self, name):
        GangaThread.__init__(self, name)

    def run(self):
        self._execUpdateAction()

    # This function takes a JobAction object from the Qin queue,
    # executes the embedded function and runs post result actions.
    def _execUpdateAction(self):
        # DEBUGGING THREADS
        # import sys
        # sys.settrace(_trace)
        while not self.should_stop():
            log.debug("%s waiting..." % threading.currentThread())
            #setattr(threading.currentThread(), 'action', None)

            from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import Qin

            while not self.should_stop():
                try:
                    action = Qin.get(block=True, timeout=0.5)
                    break
                except Queue.Empty:
                    #pass
                    continue
                finally:
                    pass
            if self.should_stop():
                break

            #setattr(threading.currentThread(), 'action', action)
            log.debug("Qin's size is currently: %d" % Qin.qsize())
            log.debug("%s running..." % threading.currentThread())

            if not isType(action, JobAction):
                continue
            if action.function == 'stop':
                break

            log.debug("From Thread: %s" % self.name)
            log.debug("Running: ''%s'' with: '%s', '%s'" %(getName(action.function), str(action.args), str(action.kwargs)))

            try:
                result = action.function(*action.args, **action.kwargs)
            except Exception as err:
                log.debug("_execUpdateAction: %s" % str(err))
                action.callback_Failure()
            #finally:
            #    pass
            else:
                if result in action.success:
                    action.callback_Success()
                else:
                    action.callback_Failure()

