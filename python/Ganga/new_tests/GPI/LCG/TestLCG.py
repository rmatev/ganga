from __future__ import absolute_import

import os

from ..GangaUnitTest import GangaUnitTest

class TestLCG(GangaUnitTest):

    def setUp(self):
        super(TestLCG, self).setUp()
        from Ganga.GPI import gridProxy
        gridProxy.voms = 'gridpp'
        gridProxy.create()

        from Ganga.Utility.Config import setConfigOption
        current_dir = os.path.dirname(os.path.realpath(__file__))
        conf_file = os.path.join(current_dir, 'glite_wmsui.conf')
        setConfigOption('LCG', 'Config', conf_file)

    def testJobSubmit(self):
        from Ganga.GPI import Job, LCG

        j = Job()
        j.backend = LCG()
        j.submit()
