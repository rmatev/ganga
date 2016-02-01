import Queue
import threading
import time
import copy
from contextlib import contextmanager

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

log = getLogger()
config = getConfig("PollThread")


def resubmit_if_required(jobList_fromset):

    # resubmit if required
    for j in jobList_fromset:

        if not j.do_auto_resubmit:
            continue

        if len(j.subjobs) == 0:
            try_resubmit = j.info.submit_counter <= config['MaxNumResubmits']
        else:
            # Check for max number of resubmissions
            skip = False
            for s in j.subjobs:
                if s.info.submit_counter > config['MaxNumResubmits'] or s.status == "killed":
                    skip = True

                if skip:
                    continue

                num_com = len([s for s in j.subjobs if s.status in ['completed']])
                num_fail = len([s for s in j.subjobs if s.status in ['failed']])

                #log.critical('Checking failed subjobs for job %d... %d %s',j.id,num_com,num_fail)

                try_resubmit = num_fail > 0 and (float(num_fail) / float(num_com + num_fail)) < config['MaxFracForResubmit']

            if try_resubmit:
                if j.backend.check_auto_resubmit():
                    log.warning('Auto-resubmit job %d...' % j.id)
                    j.auto_resubmit()

    return


def get_jobs_in_bunches(jobList_fromset, blocks_of_size=5, stripProxies=True):
    """
    Return a list of lists of subjobs where each list contains
    a total number of jobs close to 'blocks_of_size' as possible
    whilst not splitting jobs containing subjobs
    Also strip the jobs of their proxies...
    """
    list_of_bunches = []
    temp_list = []

    for this_job in jobList_fromset:

        if stripProxies:
            temp_list.append(stripProxy(this_job))
        else:
            temp_list.append(this_job)

        count = 0
        for found_job in temp_list:
            sj_len = len(found_job.subjobs)
            if sj_len > 0:
                count += sj_len
            else:
                count += 1

        if count >= blocks_of_size:
            list_of_bunches.append(temp_list)
            temp_list = []

    if len(temp_list) != 0:
        list_of_bunches.append(temp_list)
        temp_list = []

    return list_of_bunches


# This function will be run by update threads
def _checkBackend(MC_Service, backendObj, jobListSet):

    log.debug("\n\n_checkBackend\n\n")

    alljobList_fromset = list(filter(lambda x: x.status in ['submitting', 'submitted', 'running'], jobListSet))
    masterJobList_fromset = list()

    # print masterJobList_fromset
    jobList_fromset = alljobList_fromset
    jobList_fromset.extend(masterJobList_fromset)
    # print jobList_fromset
    try:

        tested_backends = []

        for j in jobList_fromset:

            run_setup = False

            if backendObj is not None:
                if hasattr(backendObj, 'setup'):
                    stripProxy(j.backend).setup()
            else:
                if hasattr(j.backend, 'setup'):
                    stripProxy(j.backend).setup()

        if MC_Service.shouldExit():
            log.debug("NOT enabled, leaving")
            return

        block_size = config['numParallelJobs']
        all_job_bunches = get_jobs_in_bunches(jobList_fromset, blocks_of_size = block_size )

        bunch_size = 0
        for bunch in all_job_bunches:
            bunch_size += len(bunch)
        assert(bunch_size == len(jobList_fromset))

        all_exceptions = []

        for this_job_list in all_job_bunches:

            if MC_Service.shouldExit():
                log.debug("NOT enabled, breaking loop")
                break

            ### This tries to loop over ALL jobs in 'this_job_list' with the maximum amount of redundancy to keep
            ### going and attempting to update all (sub)jobs if some fail
            ### ALL ERRORS AND EXCEPTIONS ARE REPORTED VIA log.error SO NO INFORMATION IS LOST/IGNORED HERE!
            job_ids = ''
            for this_job in this_job_list:
                job_ids += ' %s' % str(this_job.id) 
            log.debug("Updating Jobs: %s" % job_ids)
            try:
                stripProxy(backendObj).master_updateMonitoringInformation(this_job_list)
            except Exception as err:
                #raise err
                log.debug("Err: %s" % str(err))
                ## We want to catch ALL of the exceptions
                ## This would allow us to continue in the case of errors due to bad job/backend combinations
                if err not in all_exceptions:
                    all_exceptions.append(err)
        if all_exceptions != []:
            for err in all_exceptions:
                log.error("Monitoring Error: %s" % str(err))
            ## We should be raising exceptions no matter what
            raise all_exceptions[0]

        resubmit_if_required(jobList_fromset)

    finally:
        pass
    #except BackendError as x:
    #    _handleError(x, x.backend_name, 0)
    #except Exception as err:
    #    #_handleError(err, getName(backendObj), 1)
    #    log.error("Monitoring Error: %s" % str(err))
    #    log.debug("Lets not crash here!")
    #    return


    return

def makeCredChecker(credObj):
    def credChecker():
        log.debug("Checking %s." % getName(credObj))
    try:
        s = credObj.renew()
    except Exception as msg:
        return False
    else:
        return s
    return credChecker

pre_caught_errors = {}

def _handleError(x, backend_name, show_traceback):
    global pre_caught_errors
    def log_error():
        log.error('Problem in the monitoring loop: %s', str(x))
        #if show_traceback:
        #    log.error("exception: ", exc_info=1)
        #    #log_unknown_exception()
        #    import traceback
        #    traceback.print_stack()
        if show_traceback:
            log_user_exception(log)
    bn = backend_name
    pre_caught_errors.setdefault(bn, 0)
    if pre_caught_errors[bn] == 0:
        log_error()
        if not config['repeat_messages']:
            log.info('Further error messages from %s handler in the monitoring loop will be skipped.' % bn)
    else:
        if config['repeat_messages']:
            log_error()
    pre_caught_errors[bn] += 1


######## THREAD POOL DEBUGGING ###########
def _trace(frame, event, arg):
    setattr(threading.currentThread(), '_frame', frame)


def getStackTrace():
    import inspect

    from Ganga.Core.MonitoringComponent.Local_GangaMC_Service import ThreadPool

    try:
        status = "Available threads:\n"

        for worker in ThreadPool:
            status = status + "  " + worker.getName() + ":\n"

            if hasattr(worker, '_frame'):
                frame = worker._frame
                if frame:
                    status = status + "    stack:\n"
                    for frame, filename, line, function_name, context, index in inspect.getouterframes(frame):
                        status = status + "      " + function_name + " @ " + filename + " # " + str(line) + "\n"

            status = status + "\n"
        ## CANNOT CONVERT TO A STRING!!!
        log.debug("Trace: %s" % str(status))
        return status
    except Exception, err:
        print("Err: %s" % str(err))
    finally:
        pass

