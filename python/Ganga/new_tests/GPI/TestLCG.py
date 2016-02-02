from GangaUnitTest import GangaUnitTest

class TestLCG(GangaUnitTest):

    def setUp(self):
        super(TestLCG, self).setUp()
        from Ganga.GPI import gridProxy
        gridProxy.voms = 'gridpp'
        gridProxy.create()

    def testJobSubmit(self):
        from Ganga.GPI import Job, LCG

        j = Job()
        j.backend = LCG()
        j.submit()
