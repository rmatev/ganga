import commands, exceptions, random, re, sys, time
import os
import urllib

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Core import BackendError

from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import getDatasets
from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2outputdatasetname

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from Ganga.Utility.Config import getConfig
configPanda = getConfig('Panda')

def getLatestDBReleaseCaching():
    import tempfile
    import cPickle as pickle
    from pandatools import Client
    from GangaAtlas.Lib.Credentials.ProxyHelper import getNickname

    TMPDIR = tempfile.gettempdir()
    nickname = getNickname(allowMissingNickname=False)
    DBRELCACHE = '%s/ganga.latestdbrel.%s'%(TMPDIR,nickname)

    try:
        fh = open(DBRELCACHE)
        dbrelCache = pickle.load(fh)
        fh.close()
        if dbrelCache['mtime'] > time.time() - 3600:
            logger.debug('Loading LATEST DBRelease from local cache')
            return dbrelCache['atlas_dbrelease']
        else:
            raise Exception()
    except:
        logger.debug('Updating local LATEST DBRelease cache')
        atlas_dbrelease = Client.getLatestDBRelease(False)
        dbrelCache = {}
        dbrelCache['mtime'] = time.time()
        dbrelCache['atlas_dbrelease'] = atlas_dbrelease
        fh = open(DBRELCACHE,'w')
        pickle.dump(dbrelCache,fh)
        fh.close()
        return atlas_dbrelease

class ProdTransJediRTHandler(IRuntimeHandler):
    """Runtime handler for the ProdTrans application."""

    def master_prepare(self, app, appmasterconfig):
        """Prepare the master aspect of job submission.
           Returns: jobmasterconfig understood by Panda backend."""
        '''Prepare the master job'''

        from pandatools import Client
        from pandatools import MiscUtils
        from pandatools import AthenaUtils
        from pandatools import PsubUtils

        # create a random number for this submission to allow multiple use of containers
        self.rndSubNum = random.randint(1111,9999)

        job = app._getParent()
        logger.debug('AthenaJediRTHandler master_prepare called for %s', job.getFQID('.'))

        if job.backend.bexec and job.backend.nobuild:
            raise ApplicationConfigurationError(None,"Contradicting options: job.backend.bexec and job.backend.nobuild are both enabled.")

        if job.backend.requirements.rootver != '' and job.backend.nobuild:
            raise ApplicationConfigurationError(None,"Contradicting options: job.backend.requirements.rootver given and job.backend.nobuild are enabled.")

        # Switch on compilation flag if bexec is set or libds is empty
        if job.backend.bexec != '' or not job.backend.nobuild:
            app.athena_compile = True
            for sj in job.subjobs:
                sj.application.athena_compile = True
            logger.info('"job.backend.nobuild=False" or "job.backend.bexec" is set - Panda build job is enabled.')

        if job.backend.nobuild:
            app.athena_compile = False
            for sj in job.subjobs:
                sj.application.athena_compile = False
            logger.info('"job.backend.nobuild=True" or "--nobuild" chosen - Panda build job is switched off.')

        # check for auto datri
        if job.outputdata.location != '':
            if not PsubUtils.checkDestSE(job.outputdata.location,job.outputdata.datasetname,False):
                raise ApplicationConfigurationError(None,"Problems with outputdata.location setting '%s'" % job.outputdata.location)

        # validate application
        if not app.atlas_release and not job.backend.requirements.rootver and not app.atlas_exetype in [ 'EXE' ]:
            raise ApplicationConfigurationError(None,"application.atlas_release is not set. Did you run application.prepare()")

        ###self.runConfig = AthenaUtils.ConfigAttr(app.atlas_run_config)
        ###for k in self.runConfig.keys():
        ###    self.runConfig[k]=AthenaUtils.ConfigAttr(self.runConfig[k])
        ###if not app.atlas_run_dir:
        ###    raise ApplicationConfigurationError(None,"application.atlas_run_dir is not set. Did you run application.prepare()")

        ###self.rundirectory = app.atlas_run_dir
        self.cacheVer = ''
        ###if app.atlas_project and app.atlas_production:
        ###    self.cacheVer = "-" + app.atlas_project + "_" + app.atlas_production

        # handle different atlas_exetypes
        self.job_options = ''

        ###if self.job_options == '':
        ###    raise ApplicationConfigurationError(None,"No Job Options found!")
        ###logger.info('Running job options: %s'%self.job_options)

        # handle the output dataset
        if job.outputdata:
            if job.outputdata._name != 'DQ2OutputDataset':
                raise ApplicationConfigurationError(None,'Panda backend supports only DQ2OutputDataset')
        else:
            logger.info('Adding missing DQ2OutputDataset')
            job.outputdata = DQ2OutputDataset()

        # validate the output dataset name (and make it a container)
        job.outputdata.datasetname,outlfn = dq2outputdatasetname(job.outputdata.datasetname, job.id, job.outputdata.isGroupDS, job.outputdata.groupname)
        if not job.outputdata.datasetname.endswith('/'):
            job.outputdata.datasetname+='/'

        # add extOutFiles
        self.extOutFile = []
        for tmpName in job.outputdata.outputdata:
            if tmpName != '':
                self.extOutFile.append(tmpName)
        for tmpName in job.backend.extOutFile:
            if tmpName != '':
                self.extOutFile.append(tmpName)

        # use the shared area if possible
        ###tmp_user_area_name = app.user_area.name
        ###if app.is_prepared is not True:
        ###    from Ganga.Utility.files import expandfilename
        ###    shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
        ###    tmp_user_area_name = os.path.join(os.path.join(shared_path,app.is_prepared.name),os.path.basename(app.user_area.name))


        # job name
        jobName = 'ganga.%s' % MiscUtils.wrappedUuidGen()

        # make task
        taskParamMap = {}
        # Enforce that outputdataset name ends with / for container
        if not job.outputdata.datasetname.endswith('/'):
            job.outputdata.datasetname = job.outputdata.datasetname + '/'

        taskParamMap['taskName'] = job.outputdata.datasetname

        taskParamMap['uniqueTaskName'] = True
        taskParamMap['vo'] = 'atlas'
        taskParamMap['architecture'] = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release, cmtConfig=app.atlas_cmtconfig)
        if app.atlas_release:
            taskParamMap['transUses'] = 'Atlas-%s' % app.atlas_release
        else:
            taskParamMap['transUses'] = ''
        taskParamMap['transHome'] = 'AnalysisTransforms'+self.cacheVer#+nightVer

        configSys = getConfig('System')
        gangaver = configSys['GANGA_VERSION'].lower()
        if not gangaver:
            gangaver = "ganga"

        ###if app.atlas_exetype in ["ATHENA", "TRF"]:
        ###    taskParamMap['processingType'] = '{0}-jedi-athena'.format(gangaver)
        ###else:
        taskParamMap['processingType'] = '{0}-jedi-run'.format(gangaver)

        #if options.eventPickEvtList != '':
        #    taskParamMap['processingType'] += '-evp'
        taskParamMap['prodSourceLabel'] = 'user'
        if job.backend.site != 'AUTO':
            taskParamMap['cloud'] = Client.PandaSites[job.backend.site]['cloud']
            taskParamMap['site'] = job.backend.site
        elif job.backend.requirements.cloud != None and not job.backend.requirements.anyCloud:
            taskParamMap['cloud'] = job.backend.requirements.cloud
        if job.backend.requirements.excluded_sites != []:
            taskParamMap['excludedSite'] = expandExcludedSiteList( job )

        # if only a single site specifed, don't set includedSite
        #if job.backend.site != 'AUTO':
        #    taskParamMap['includedSite'] = job.backend.site
        #taskParamMap['cliParams'] = fullExecString
        if job.backend.requirements.noEmail:
            taskParamMap['noEmail'] = True
        if job.backend.requirements.skipScout:
            taskParamMap['skipScout'] = True
        ###if not app.atlas_exetype in ["ATHENA", "TRF"]:
        taskParamMap['nMaxFilesPerJob'] = job.backend.requirements.maxNFilesPerJob
        if job.backend.requirements.disableAutoRetry:
            taskParamMap['disableAutoRetry'] = 1

        # dataset names
        outDatasetName = job.outputdata.datasetname
        logDatasetName = re.sub('/$','.log/',job.outputdata.datasetname)
        # log
        taskParamMap['log'] = {'dataset': logDatasetName,
                               'container': logDatasetName,
                               'type':'template',
                               'param_type':'log',
                               'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName[:-1])
                               }

        taskParamMap['jobParameters'] = []

        # output
        # output files
        outMap = {}
        ###if app.atlas_exetype in ["ATHENA", "TRF"]:
        ###    outMap, tmpParamList = AthenaUtils.convertConfToOutput(self.runConfig, self.extOutFile, job.outputdata.datasetname, destination=job.outputdata.location)
        ###    taskParamMap['jobParameters'] += [
        ###        {'type':'constant',
        ###         'value': '-o "%s" ' % outMap
        ###        },
        ###    ]
        ###    taskParamMap['jobParameters'] += tmpParamList

        ###else:
        if job.outputdata.outputdata:

            for lfn, lfntype in zip(app.output_files,app.output_type):

                #print MiscUtils.makeJediJobParam(lfn,job.outputdata.datasetname,'output',hidden=True, destination=job.outputdata.location)
                #taskParamMap['jobParameters'] += MiscUtils.makeJediJobParam(lfn,job.outputdata.datasetname,'output',hidden=True, destination=job.outputdata.location)

                taskParamMap['jobParameters'] += [{'type':'template',
                        'param_type':'output',
                        'value':'--output%sFile=%s.%s._${{SN}}.pool.root' % (lfntype, lfntype, lfn),
                        'dataset': job.outputdata.datasetname,
                        }]


        # if app.atlas_exetype in ["ATHENA"]:
        #     # jobO parameter
        #     tmpJobO = self.job_options
        #     # replace full-path jobOs
        #     for tmpFullName,tmpLocalName in AthenaUtils.fullPathJobOs.iteritems():
        #         tmpJobO = re.sub(tmpFullName,tmpLocalName,tmpJobO)
        #     # modify one-liner for G4 random seeds
        #     if self.runConfig.other.G4RandomSeeds > 0:
        #         if app.options != '':
        #             tmpJobO = re.sub('-c "%s" ' % app.options,
        #                              '-c "%s;from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" ' \
        #                              % app.options,tmpJobO)
        #         else:
        #             tmpJobO = '-c "from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" '
        #         dictItem = {'type':'template',
        #                     'param_type':'number',
        #                     'value':'${RNDMSEED}',
        #                     'hidden':True,
        #                     'offset':self.runConfig.other.G4RandomSeeds,
        #                     }
        #         taskParamMap['jobParameters'] += [dictItem]
        # elif app.atlas_exetype in ["TRF"]:
        #     # replace parameters for TRF
        #     tmpJobO = self.job_options
        #     # output : basenames are in outMap['IROOT'] trough extOutFile
        #     tmpOutMap = []
        #     for tmpName,tmpLFN in outMap['IROOT']:
        #         tmpJobO = tmpJobO.replace('%OUT.' + tmpName,tmpName)
        #     # replace DBR
        #     tmpJobO = re.sub('%DB=[^ \'\";]+','${DBR}',tmpJobO)

        ###if app.atlas_exetype in ["TRF"]:
        ###    taskParamMap['useLocalIO'] = 1

        # build
        # if job.backend.nobuild:
        #     taskParamMap['jobParameters'] += [
        #         {'type':'constant',
        #          'value': '-a {0}'.format(os.path.basename(self.inputsandbox)),
        #          },
        #     ]
        # else:
        #     taskParamMap['jobParameters'] += [
        #         {'type':'constant',
        #          'value': '-l ${LIB}',
        #          },
        #     ]

        #
        # input
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            if job.backend.requirements.nFilesPerJob > 0 and job.inputdata.number_of_files == 0 and job.backend.requirements.split > 0:
                job.inputdata.number_of_files = job.backend.requirements.nFilesPerJob * job.backend.requirements.split

        if job.inputdata and job.inputdata._name == 'DQ2Dataset' and job.inputdata.number_of_files != 0:
            taskParamMap['nFiles'] = job.inputdata.number_of_files
        elif job.backend.requirements.nFilesPerJob > 0 and job.backend.requirements.split > 0:
            # pathena does this for some reason even if there is no input files
            taskParamMap['nFiles'] = job.backend.requirements.nFilesPerJob * job.backend.requirements.split
        if job.backend.requirements.nFilesPerJob > 0:
            taskParamMap['nFilesPerJob'] = job.backend.requirements.nFilesPerJob

        if job.backend.requirements.nEventsPerFile > 0:
            taskParamMap['nEventsPerFile'] = job.backend.requirements.nEventsPerFile

        if not job.backend.requirements.nGBPerJob in [ 0,'MAX']:
            try:
                if job.backend.requirements.nGBPerJob != 'MAX':
                    job.backend.requirments.nGBPerJob = int(job.backend.requirements.nGBPerJob)
            except:
                logger.error("nGBPerJob must be an integer or MAX")
            # check negative
            if job.backend.requirements.nGBPerJob <= 0:
                logger.error("nGBPerJob must be positive")

            # don't set MAX since it is the defalt on the server side
            if not job.backend.requirements.nGBPerJob in [-1,'MAX']:
                taskParamMap['nGBPerJob'] = job.backend.requirements.nGBPerJob

        # if app.atlas_exetype in ["ATHENA", "TRF"]:
        #     inputMap = {}
        #     if job.inputdata and job.inputdata._name == 'DQ2Dataset':
        #         tmpDict = {'type':'template',
        #                    'param_type':'input',
        #                    'value':'-i "${IN/T}"',
        #                    'dataset': ','.join(job.inputdata.dataset),
        #                    'expand':True,
        #                    'exclude':'\.log\.tgz(\.\d+)*$',
        #                    }
        #         #if options.inputType != '':
        #         #    tmpDict['include'] = options.inputType
        #         taskParamMap['jobParameters'].append(tmpDict)
        #         taskParamMap['dsForIN'] = ','.join(job.inputdata.dataset)
        #         inputMap['IN'] = ','.join(job.inputdata.dataset)
        #     else:
        #         # no input
        #         taskParamMap['noInput'] = True
        #         if job.backend.requirements.split > 0:
        #             taskParamMap['nEvents'] = job.backend.requirements.split
        #         else:
        #             taskParamMap['nEvents'] = 1
        #         taskParamMap['nEventsPerJob'] = 1
        #         taskParamMap['jobParameters'] += [
        #             {'type':'constant',
        #              'value': '-i "[]"',
        #              },
        #         ]
        # else:

        # input data
        if job.inputdata and job.inputdata._name == 'DQ2Dataset':
            tmpDict = {'type':'template',
                       'param_type':'input',
                       'value':'--inputAODFile=${IN_AOD}',
                       'offset': 0,
                       'dataset': ','.join(job.inputdata.dataset),
                       }
            taskParamMap['jobParameters'].append(tmpDict)
            taskParamMap['dsForIN'] = ','.join(job.inputdata.dataset)
        else:
            # no input
            taskParamMap['noInput'] = True
            if job.backend.requirements.split > 0:
                taskParamMap['nEvents'] = job.backend.requirements.split
            else:
                taskParamMap['nEvents'] = 1
            taskParamMap['nEventsPerJob'] = 1

        # param for DBR
        self.dbrelease_dataset = app.dbrelease_dataset
        self.dbrelease = app.dbrelease

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
        # no expansion
        #if options.notExpandDBR:
        #dictItem = {'type':'constant',
        #            'value':'--noExpandDBR',
        #            }
        #taskParamMap['jobParameters'] += [dictItem]

        # secondary FIXME disabled
        self.secondaryDSs = {}
        if self.secondaryDSs != {}:
            inMap = {}
            streamNames = []
            for tmpDsName,tmpMap in self.secondaryDSs.iteritems():
                # make template item
                streamName = tmpMap['streamName']
                dictItem = MiscUtils.makeJediJobParam('${'+streamName+'}',tmpDsName,'input',hidden=True,
                                                      expand=True,include=tmpMap['pattern'],offset=tmpMap['nSkip'],
                                                      nFilesPerJob=tmpMap['nFiles'])
                taskParamMap['jobParameters'] += dictItem
                inMap[streamName] = 'tmp_'+streamName
                streamNames.append(streamName)
            # make constant item
            strInMap = str(inMap)
            # set placeholders
            for streamName in streamNames:
                strInMap = strInMap.replace("'tmp_"+streamName+"'",'${'+streamName+'/T}')
            dictItem = {'type':'constant',
                        'value':'--inMap "%s"' % strInMap,
                        }
            taskParamMap['jobParameters'] += [dictItem]

        # misc
        jobParameters = ''

        # write input to txt
        #if options.writeInputToTxt != '':
        #    jobParameters += "--writeInputToTxt %s " % options.writeInputToTxt
        # debug parameters
        #if options.queueData != '':
        #    jobParameters += "--overwriteQueuedata=%s " % options.queueData
        # JEM
        #if options.enableJEM:
        #    jobParameters += "--enable-jem "
        #    if options.configJEM != '':
        #        jobParameters += "--jem-config %s " % options.configJEM

        # set task param
        if jobParameters != '':
            taskParamMap['jobParameters'] += [
                {'type':'constant',
                 'value': jobParameters,
                 },
            ]

        # force stage-in
        if job.backend.accessmode == "LocalIO":
            taskParamMap['useLocalIO'] = 1

        # set jobO parameter
        # if app.atlas_exetype in ["ATHENA", "TRF"]:
        #     taskParamMap['jobParameters'] += [
        #         {'type':'constant',
        #          'value': '-j "',
        #          'padding':False,
        #          },
        #     ]
        #     taskParamMap['jobParameters'] += PsubUtils.convertParamStrToJediParam(tmpJobO,inputMap,job.outputdata.datasetname[:-1], True,False)
        #     taskParamMap['jobParameters'] += [
        #         {'type':'constant',
        #          'value': '"',
        #          },
        #     ]
        #
        # else:


        # build step
        if not job.backend.nobuild:
            jobParameters = '-i ${IN} -o ${OUT} --sourceURL ${SURL} '

            if job.backend.bexec != '':
                jobParameters += ' --bexec "%s" ' % urllib.quote(job.backend.bexec)

            if app.atlas_exetype == 'ARES' or (app.atlas_exetype in ['PYARA','ROOT','EXE'] and app.useAthenaPackages):
                # use Athena packages
                jobParameters += "--useAthenaPackages "
            # use RootCore
            if app.useRootCore or app.useRootCoreNoBuild:
                jobParameters += "--useRootCore "

            # run directory
            if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
                jobParameters += '-r {0} '.format(self.rundirectory)

            # no compile
            #if options.noCompile:
            #    jobParameters += "--noCompile "
            # use mana
            if app.useMana:
                jobParameters += "--useMana "
                if app.atlas_release != "":
                    jobParameters += "--manaVer %s " % app.atlas_release

            # root
            if app.atlas_exetype in ['PYARA','ROOT','EXE'] and job.backend.requirements.rootver != '':
                rootver = re.sub('/','.', job.backend.requirements.rootver)
                jobParameters += "--rootVer %s " % rootver

            # cmt config
            if app.atlas_exetype in ['PYARA','ARES','ROOT','EXE']:
                if not app.atlas_cmtconfig in ['','NULL',None]:
                    jobParameters += " --cmtConfig %s " % app.atlas_cmtconfig


            #cmtConfig         = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release, cmtConfig=app.atlas_cmtconfig)
            #if cmtConfig:
            #    jobParameters += "--cmtConfig %s " % cmtConfig
            # debug parameters
            #if options.queueData != '':
            #    jobParameters += "--overwriteQueuedata=%s " % options.queueData
            # set task param
            taskParamMap['buildSpec'] = {
                'prodSourceLabel':'panda',
                'archiveName':os.path.basename(self.inputsandbox),
                'jobParameters':jobParameters,
            }


        # enable merging
        if job.backend.requirements.enableMerge:
            jobParameters = '-r {0} '.format(self.rundirectory)
            if 'exec' in job.backend.requirements.configMerge and job.backend.requirements.configMerge['exec'] != '':
                jobParameters += '-j "{0}" '.format(job.backend.requirements.configMerge['exec'])
            if not job.backend.nobuild:
                jobParameters += '-l ${LIB} '
            else:
                jobParameters += '-a {0} '.format(os.path.basename(self.inputsandbox))
                jobParameters += "--sourceURL ${SURL} "
            jobParameters += '${TRN_OUTPUT:OUTPUT} ${TRN_LOG:LOG}'
            taskParamMap['mergeSpec'] = {}
            taskParamMap['mergeSpec']['useLocalIO'] = 1
            taskParamMap['mergeSpec']['jobParameters'] = jobParameters
            taskParamMap['mergeOutput'] = True

        # Selected by Jedi
        #if not app.atlas_exetype in ['PYARA','ROOT','EXE']:
        #    taskParamMap['transPath'] = 'http://atlpan.web.cern.ch/atlpan/runAthena-00-00-12'

        logger.debug(taskParamMap)

        # upload sources
        # if self.inputsandbox and not job.backend.libds:
        #     uploadSources(os.path.dirname(self.inputsandbox),os.path.basename(self.inputsandbox))
        #
        #     if not self.inputsandbox == tmp_user_area_name:
        #         logger.info('Removing source tarball %s ...' % self.inputsandbox )
        #         os.remove(self.inputsandbox)

        # Set task parameters
        #taskParamMap['transformation'] = app.transformation
        taskParamMap['transPath'] = app.transformation

        # additional job parameters
        for p in app.job_parameters.split('--'):
            p = p.strip()
            if not p:
                continue

            taskParamMap['jobParameters'] += [
                    {'type':'constant',
                     'value': '--%s' % p,
                     },
                    ]

        # Add the run number
        if job.inputdata:
            m = re.search('(.*)\.(.*)\.(.*)\.(.*)\.(.*)\.(.*)',
                          job.inputdata.dataset[0])
            if not m:
                logger.error("Error retrieving run number from dataset name")
                #raise ApplicationConfigurationError(None, "Error retrieving run number from dataset name")
                runnumber = 105200
            else:
                runnumber = int(m.group(2))

            taskParamMap['jobParameters'] += [
                    {'type':'constant',
                     'value': '--runNumber %d' % runnumber,
                     },
                    ]

        return taskParamMap



        # from pandatools import Client
        # from pandatools import AthenaUtils
        # from taskbuffer.JobSpec import JobSpec
        # from taskbuffer.FileSpec import FileSpec
        # from GangaAtlas.Lib.ATLASDataset.DQ2Dataset import dq2_set_dataset_lifetime
        # from GangaPanda.Lib.Panda.Panda import refreshPandaSpecs
        #
        # job = app._getParent()
        # logger.debug('ProdTransJediRTHandler master_prepare() for %s',
        #             job.getFQID('.'))
        #
        # # make sure we have the correct siteType
        # #refreshPandaSpecs()
        #
        # job = app._getParent()
        # masterjob = job._getRoot()
        #
        # job.backend.actualCE = job.backend.site
        # job.backend.requirements.cloud = Client.PandaSites[job.backend.site]['cloud']
        #
        # # make task
        # taskParamMap = {}
        # # Enforce that outputdataset name ends with / for container
        # if not job.outputdata.datasetname.endswith('/'):
        #     job.outputdata.datasetname = job.outputdata.datasetname + '/'
        #
        # taskParamMap['taskName'] = job.outputdata.datasetname
        #
        # taskParamMap['currentPriority'] = app.priority
        # taskParamMap['coreCount'] = app.core_count
        # taskParamMap['uniqueTaskName'] = True
        # taskParamMap['vo'] = 'atlas'
        # taskParamMap['architecture'] = AthenaUtils.getCmtConfig(athenaVer=app.atlas_release, cmtConfig=app.atlas_cmtconfig)
        # taskParamMap['transUses'] = 'Atlas-%s' % app.atlas_release
        # taskParamMap['transHome'] = app.home_package
        #
        #
        # taskParamMap['jobParameters'] = [
        #     {'type':'constant',
        #      'value': '-j "" --sourceURL ${SURL}',
        #      },
        #     ]
        #
        # configSys = getConfig('System')
        # gangaver = configSys['GANGA_VERSION'].lower()
        # if not gangaver:
        #     gangaver = "ganga"
        #
        # taskParamMap['processingType'] = configPanda['processingType']
        # if app.prod_source_label:
        #     taskParamMap['prodSourceLabel'] = app.prod_source_label
        # else:
        #     taskParamMap['prodSourceLabel'] = configPanda['prodSourceLabelRun']
        #
        # taskParamMap['cloud'] = job.backend.requirements.cloud
        # taskParamMap['site'] = job.backend.site
        # if job.backend.requirements.noEmail:
        #     taskParamMap['noEmail'] = True
        # if job.backend.requirements.skipScout:
        #     taskParamMap['skipScout'] = True
        # #if not app.atlas_exetype in ["ATHENA", "TRF"]:
        # #    taskParamMap['nMaxFilesPerJob'] = job.backend.requirements.maxNFilesPerJob
        # if job.backend.requirements.disableAutoRetry:
        #     taskParamMap['disableAutoRetry'] = 1
        #
        # # log
        # logDatasetName = re.sub('/$','.log/',job.outputdata.datasetname)
        # taskParamMap['log'] = {'dataset': logDatasetName,
        #                        'container': logDatasetName,
        #                        'type':'template',
        #                        'param_type':'log',
        #                        'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName[:-1])
        #                        }
        #
        # # TODO: This should be fixed properly!
        # #taskParamMap['jobParameters'] = app.job_parameters
        #
        # self.dbrelease = app.dbrelease
        # if self.dbrelease != '' and self.dbrelease != 'LATEST' and self.dbrelease.find(':') == -1:
        #     raise ApplicationConfigurationError(None,"ERROR : invalid argument for DB Release. Must be 'LATEST' or 'DatasetName:FileName'")
        #
        # # validate dbrelease
        # if self.dbrelease != "LATEST":
        #     self.dbrFiles,self.dbrDsList = getDBDatasets(self.job_options,'',self.dbrelease)
        #
        # # param for DBR
        # if self.dbrelease != '':
        #     dbrDS = self.dbrelease.split(':')[0]
        #     # change LATEST to DBR_LATEST
        #     if dbrDS == 'LATEST':
        #         dbrDS = 'DBR_LATEST'
        #     dictItem = {'type':'template',
        #                 'param_type':'input',
        #                 'value':'--dbrFile=${DBR}',
        #                 'dataset':dbrDS,
        #                 }
        #     taskParamMap['jobParameters'] += [dictItem]
        #
        # inputMap = {}
        # if job.inputdata and job.inputdata._name == 'DQ2Dataset':
        #     tmpDict = {'type':'template',
        #                'param_type':'input',
        #                'value':'-i "${IN/T}"',
        #                'dataset': ','.join(job.inputdata.dataset),
        #                'expand':True,
        #                'exclude':'\.log\.tgz(\.\d+)*$',
        #                }
        #     #if options.inputType != '':
        #     #    tmpDict['include'] = options.inputType
        #     taskParamMap['jobParameters'].append(tmpDict)
        #     taskParamMap['dsForIN'] = ','.join(job.inputdata.dataset)
        #     inputMap['IN'] = ','.join(job.inputdata.dataset)
        # else:
        #     # no input
        #     taskParamMap['noInput'] = True
        #     if job.backend.requirements.split > 0:
        #         taskParamMap['nEvents'] = job.backend.requirements.split
        #     else:
        #         taskParamMap['nEvents'] = 1
        #     taskParamMap['nEventsPerJob'] = 1
        #     taskParamMap['jobParameters'] += [
        #         {'type':'constant',
        #          'value': '-i "[]"',
        #          },
        #         ]
        #

        # return taskParamMap

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
