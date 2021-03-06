from __future__ import absolute_import

import random

import Ganga.Utility.Config
from Ganga.Core.GangaRepository import RegistryLockError
from Ganga.Core.GangaRepository.Registry import Registry
from Ganga.Core.GangaThread.GangaThread import GangaThread
from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.Runtime.Repository_runtime import getLocalRoot
from Ganga.Utility.logging import getLogger
from ..GangaUnitTest import GangaUnitTest

config = Ganga.Utility.Config.getConfig('Configuration')
logger = getLogger(modulename=True)


class HammerThread(GangaThread):
    def __init__(self, _id, reg):
        self.id = _id
        self.reg = reg
        self.rng = random.Random()
        self.owned_ids = []
        self.owned_objs = {}
        self.done = False
        super(HammerThread, self).__init__('HammerThread_%s' % _id)

    def updown(self):
        logger.info(str(self.id) + ' shutdown()')
        self.reg.shutdown()
        logger.info(str(self.id) + ' shutdown() done!')
        self.owned_ids = []
        logger.info(str(self.id) + ' startup()')
        self.reg.startup()
        logger.info(str(self.id) + ' startup() done!')

    def uindex(self):
        logger.info(str(self.id) + ' update_index(None)')
        self.reg.ids()
        logger.info(str(self.id) + ' update_index(None) done!')

    def add(self):
        from GangaTest.Lib.TestObjects import TestGangaObject  # This import is in here to avoid confusing nosetests
        logger.info('self.ref.keys before: %s' % self.reg.keys())
        objs = [TestGangaObject('HT%i' % self.id) for _ in range(self.rng.randint(1, 2))]
        logger.info(str(self.id) + ' add(%s)' % objs)
        ids = []
        count = 1
        logger.info('Adding #%s Objs' % len(objs))
        for obj in objs:
            logger.info('\n\n\nAdding #%s of #%s Object(s)' % (count, len(objs)))
            ids.append(self.reg._add(obj))
            logger.info('Added as : %s' % ids[-1])
            assert (ids[-1] == obj.id)
            self.owned_objs[obj.id] = obj
            logger.info('Count: %s\n\n' % count)
            count += 1

        # ids = [self.reg._add(obj) for obj in objs]
        logger.info(str(self.id) + ' add(%s) done, ids = %s!' % (objs, ids))
        assert len(ids) == len(objs)
        # TODO: Check if objects stay the same
        self.owned_ids.extend(ids)
        logger.info('self.reg.keys after: %s' % self.reg.keys())

    def delete(self):
        _ids = self.reg.ids()
        logger.info('delete self.reg.keys start: %s' % self.reg.keys())
        if len(_ids) == 0:
            return
        n = min(len(self.reg.keys()), self.rng.randint(1, 2))
        ids = self.rng.sample(self.reg.keys(), n)
        logger.info(str(self.id) + ' delete(%s)' % ids)
        for _id in ids:
            logger.debug('Removing: %s' % _id)
            try:
                logger.debug('reg_id: %s' % stripProxy(self.reg[_id]).id)
            except:
                pass
            obj_to_remove = self.reg[_id]
            self.reg._remove(obj_to_remove)
            logger.info('Finished Remove\n\n')
        # [self.reg._remove(self.reg[id]) for id in ids]
        for _id in ids:
            logger.info('keys: %s' % self.reg.keys())
            logger.info('testing: %s' % _id)
            assert _id not in self.reg.keys()
            try:
                self.owned_ids.remove(_id)
                del self.owned_ids[_id]
            except:
                pass
        logger.info(str(self.id) + ' delete(%s) done!' % ids)
        logger.info('delete self.reg.keys end: %s' % self.reg.keys())

    def load(self):
        ids = self.reg.ids()
        if len(ids) == 0:
            return
        _id = self.rng.sample(ids, 1)[0]
        logger.info(str(self.id) + ' load(%s)' % _id)
        try:
            logger.info('Getting ReadAccess: %s from %s' % (_id, self.reg.ids()))
            stripProxy(self.reg[_id])._getReadAccess()
            # logger.info('Looking at: %s' % self.owned_objs[_id])
            # logger.info('stripped: %s' % stripProxy(self.owned_objs[_id]))
            logger.info('name: %s' % self.reg[_id].name)
            logger.info('Wanting: %s' % _id)
            logger.info('Loaded: %s' % self.reg._loaded_ids)
            assert self.reg[_id].name.startswith('HT')
            if _id in self.owned_ids:
                assert self.reg[_id].name == 'HT%i' % self.id, '{0} == {1}'.format(self.reg[_id].name, 'HT%i' % self.id)
        except KeyError:  # If the object has been deleted in the meantime, it must be gone from the registry
            assert _id not in self.reg.ids()
            logger.info(str(self.id) + '  %s deleted after KeyError (as per specification)' % _id)
        logger.info(str(self.id) + ' load(%s) done!' % _id)

    def lock(self):
        ids = self.reg.ids()
        if len(ids) == 0:
            return
        _id = self.rng.sample(ids, 1)[0]
        logger.info(str(self.id) + ' lock(%s)' % _id)
        try:
            logger.info('Getting Read, Write access: %s' % _id)
            stripProxy(self.reg[_id])._getReadAccess()
            stripProxy(self.reg[_id])._getWriteAccess()
            logger.info('Got Access: %s' % _id)
            logger.info('Name: %s' % self.reg[_id].name)
            assert self.reg[_id].name.startswith('HT')
            self.reg[_id].name = 'HT%i' % self.id
            if _id not in self.owned_ids:
                self.owned_ids.append(_id)
        except KeyError:  # If the object has been deleted in the meantime, it must be gone from the registry
            logger.info(str(self.id) + '  %s deleted after KeyError (as per specification)' % _id)
            assert _id not in self.reg
        except RegistryLockError:  # ok, this is already locked
            logger.info(str(self.id) + '  %s was locked...' % _id)
        logger.info(str(self.id) + ' lock(%s) done!' % _id)

    def unlock(self):
        if len(self.owned_ids) == 0:
            return
        _id = self.rng.sample(self.owned_ids, 1)[0]
        logger.info(str(self.id) + ' unlock(%s)' % _id)
        obj_to_unlock = self.reg[_id]
        assert obj_to_unlock.name.startswith('HT')
        # self.reg[_id].name = 'HT-unlocked'
        # self.owned_ids.remove(_id)
        self.reg._release_lock(self.reg[_id])
        logger.info(str(self.id) + ' unlock(%s) done!' % _id)

    def run(self):
        for i in range(100):
            choices = []
            choices.extend([self.updown] * 1)
            choices.extend([self.uindex] * 1)
            choices.extend([self.add] * 10)
            choices.extend([self.delete] * 10)
            choices.extend([self.load] * 10)
            choices.extend([self.lock] * 10)
            choices.extend([self.unlock] * 5)
            # choices.extend([self.flush]*2)
            this_choice = self.rng.choice(choices)
            logger.debug('\n\n\n\n\n%s) This Choise: %s\n' % (i, this_choice))
            this_choice()
            assert len(self.owned_ids) == len(dict(zip(self.owned_ids, range(len(self.owned_ids)))).keys())
            for _id in self.owned_ids:
                if _id not in self.reg._objects:
                    logger.info('LOCKED ID DELETED: ' + str(_id))
                    assert False

            logger.info('\n\nChecking Object consistency')
            try:
                self.reg._checkObjects()
                logger.info('PASSED')
            except:
                logger.error('FAILED')
                raise

        self.done = True


class TestRegistry(GangaUnitTest):
    def setUp(self):
        super(TestRegistry, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_pass1(self):
        t = TestReg(1)
        while not t.isReadyForCheck():
            pass
        return t.checkTest()

    def test_pass2(self):
        t = TestReg(2)
        while not t.isReadyForCheck():
            pass
        return t.checkTest()


class TestReg(object):
    def __init__(self, _id):
        self.id = _id
        self.registry = Registry('TestRegistry_%s' % _id, 'TestRegistry_%s' % _id)
        self.registry.type = config['repositorytype']
        self.registry.location = getLocalRoot()
        logger.info(str(_id) + ' startup()')
        self.registry.startup()
        logger.info(str(_id) + ' startup() done!')
        logger.info('RUNNING HAMMERTHREAD #%s on direcory %s' % (_id, self.registry.location))
        self.thread = HammerThread(_id, self.registry)
        self.thread.start()

    def isReadyForCheck(self):
        return self.thread.done or not self.thread.isAlive()

    def checkTest(self):
        self.thread.join()
        assert self.thread.done
        logger.info(str(self.id) + ' shutdown()')
        self.registry.shutdown()
        logger.info(str(self.id) + ' shutdown() done!')
