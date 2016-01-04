import commands, exceptions, random, re, sys, time

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Core import BackendError

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import getDatasets

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.Config import getConfig
configPanda = getConfig('Panda')

class ProdTransJediRTHandler(IRuntimeHandler):
    """Runtime handler for the ProdTrans application."""

    def master_prepare(self, app, appmasterconfig):
        """Prepare the master aspect of job submission.
           Returns: jobmasterconfig understood by Panda backend."""

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec
        from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_set_dataset_lifetime
        from GangaPanda.Lib.Panda.Panda import refreshPandaSpecs

        job = app._getParent()
        logger.debug('ProdTransJediRTHandler master_prepare() for %s',
                    job.getFQID('.'))

        # make sure we have the correct siteType
        #refreshPandaSpecs()

        job = app._getParent()
        masterjob = job._getRoot()

        job.backend.actualCE = job.backend.site
        job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']

        # make task
        taskParamMap = {}
        # Enforce that outputdataset name ends with / for container
        if not job.outputdata.datasetname.endswith('/'):
            job.outputdata.datasetname = job.outputdata.datasetname + '/'

        taskParamMap['taskName'] = job.outputdata.datasetname

        taskParamMap['currentPriority'] = app.priority
        taskParamMap['coreCount'] = app.core_count
        taskParamMap['uniqueTaskName'] = True
        taskParamMap['vo'] = 'atlas'
        taskParamMap['architecture'] = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release, cmtConfig=app.atlas_cmtconfig)
        taskParamMap['transUses'] = 'Atlas-%s' % app.atlas_release
        taskParamMap['transHome'] = app.home_package
        taskParamMap['transformation'] = app.transformation

        taskParamMap['jobParameters'] = [
            {'type':'constant',
             'value': '-j "" --sourceURL ${SURL}',
             },
            ]

        configSys = getConfig('System')
        gangaver = configSys['GANGA_VERSION'].lower()
        if not gangaver:
            gangaver = "ganga"

        taskParamMap['processingType'] = configPanda['processingType']
        if app.prod_source_label:
            taskParamMap['prodSourceLabel'] = app.prod_source_label
        else:
            taskParamMap['prodSourceLabel'] = configPanda['prodSourceLabelRun']

        taskParamMap['cloud'] = job.backend.requirements.cloud
        taskParamMap['site'] = job.backend.site
        if job.backend.requirements.noEmail:
            taskParamMap['noEmail'] = True
        if job.backend.requirements.skipScout:
            taskParamMap['skipScout'] = True
        #if not app.atlas_exetype in ["ATHENA", "TRF"]:
        #    taskParamMap['nMaxFilesPerJob'] = job.backend.requirements.maxNFilesPerJob
        if job.backend.requirements.disableAutoRetry:
            taskParamMap['disableAutoRetry'] = 1

        # log
        logDatasetName = re.sub('/$','.log/',job.outputdata.datasetname)
        taskParamMap['log'] = {'dataset': logDatasetName,
                               'container': logDatasetName,
                               'type':'template',
                               'param_type':'log',
                               'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName[:-1])
                               }

        # TODO: This should be fixed properly!
        #taskParamMap['jobParameters'] = app.job_parameters

        self.dbrelease = app.dbrelease
        if self.dbrelease != '' and self.dbrelease != 'LATEST' and self.dbrelease.find(':') == -1:
            raise ApplicationConfigurationError(None,"ERROR : invalid argument for DB Release. Must be 'LATEST' or 'DatasetName:FileName'")

        # validate dbrelease
        if self.dbrelease != "LATEST":
            self.dbrFiles,self.dbrDsList = getDBDatasets(self.job_options,'',self.dbrelease)

        # param for DBR
        if self.dbrelease != '':
            dbrDS = self.dbrelease.split(':')[0]
            # change LATEST to DBR_LATEST
            if dbrDS == 'LATEST':
                dbrDS = 'DBR_LATEST'
            dictItem = {'type':'template',
                        'param_type':'input',
                        'value':'--dbrFile=${DBR}',
                        'dataset':dbrDS,
                        }
            taskParamMap['jobParameters'] += [dictItem]

        inputMap = {}
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            tmpDict = {'type':'template',
                       'param_type':'input',
                       'value':'-i "${IN/T}"',
                       'dataset': ','.join(job.inputdata.dataset),
                       'expand':True,
                       'exclude':'\.log\.tgz(\.\d+)*$',
                       }
            #if options.inputType != '':
            #    tmpDict['include'] = options.inputType
            taskParamMap['jobParameters'].append(tmpDict)
            taskParamMap['dsForIN'] = ','.join(job.inputdata.dataset)
            inputMap['IN'] = ','.join(job.inputdata.dataset)
        else:
            # no input
            taskParamMap['noInput'] = True
            if job.backend.requirements.split > 0:
                taskParamMap['nEvents'] = job.backend.requirements.split
            else:
                taskParamMap['nEvents'] = 1
            taskParamMap['nEventsPerJob'] = 1
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': '-i "[]"',
                 },
                ]

        # if job.inputdata:
        #     m = re.search('(.*)\.(.*)\.(.*)\.(.*)\.(.*)\.(.*)',
        #                   job.inputdata.dataset[0])
        #     if not m:
        #         logger.error("Error retrieving run number from dataset name")
        #         #raise ApplicationConfigurationError(None, "Error retrieving run number from dataset name")
        #         runnumber = 105200
        #     else:
        #         runnumber = int(m.group(2))
        #     if jspec.transformation.endswith("_tf.py") or jspec.transformation.endswith("_tf"):
        #         jspec.jobParameters += ' --runNumber %d' % runnumber
        #     else:
        #         jspec.jobParameters += ' RunNumber=%d' % runnumber


        return taskParamMap

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        '''prepare the subjob specific configuration'''

        from pandatools import Client
        from pandatools import AthenaUtils
        from taskbuffer.JobSpec import JobSpec
        from taskbuffer.FileSpec import FileSpec

        job = app._getParent()
        masterjob = job._getRoot()

        logger.debug('AthenaJediRTHandler prepare called for %s', job.getFQID('.'))

#       in case of a simple job get the dataset content, otherwise subjobs are filled by the splitter

        return {}
    
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
allHandlers.add('ProdTrans', 'Jedi', ProdTransJediRTHandler)
