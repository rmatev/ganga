from Ganga.GPIDev.Adapters.ISplitter import ISplitter
from Ganga.GPIDev.Schema import *
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Base.Proxy import addProxy, stripProxy
import pickle
import os
import copy

class OptionsFileSplitter(ISplitter):
    '''Split a jobs based on a list of option file fragments
    
    This Splitter takes as argument a list of option file statements and will
    generate a job for each item in this list. The value of the indevidual list
    item will be appended to the master options file. A use case of this
    splitter would be to change a parameter in an algorithm (e.g. a cut) and to
    recreate a set of jobs with different cuts
    '''
    _name = "OptionsFileSplitter"
    docstr = "List of option-file strings, each list item creates a new subjob"
    _schema =Schema(Version(1,0),
                    {'optsArray': SimpleItem(defvalue=[],doc=docstr)})

    def _create_subjob(self, job, inputdata):
        j=addProxy(job).copy() ## Proxy copy increments the shareref counter
        j=stripProxy(j)
        j.splitter = None
        j.merger = None
        j.inputsandbox = [] ## master added automatically
        j.inputdata = inputdata
        
        return j


    def split(self, job):        
        subjobs=[]

        inputdata = job.inputdata
        if not job.inputdata:
            share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                      'shared',
                                      getConfig('Configuration')['user'],
                                      job.application.is_prepared.name,
                                      'inputdata',
                                      'options_data.pkl')
            
            if os.path.exists(share_path):
                f=open(share_path,'r+b')
                inputdata = pickle.load(f)
                f.close()
  
        for i in self.optsArray:
            j = self._create_subjob(job, inputdata)
            j._splitter_data = i
            subjobs.append(j)
        return subjobs
