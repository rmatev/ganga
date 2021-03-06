from Ganga.Utility.logging import getLogger

from Ganga.Core import GangaException
from Ganga.Core.GangaRepository import InaccessibleObjectError, RepositoryError

import time
import threading

from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.GPIDev.Base.Objects import GangaObject
from Ganga.GPIDev.Schema import Schema, Version
from Ganga.GPIDev.Base.Proxy import stripProxy, isType, getName

logger = getLogger()

_reg_id_str = '_registry_id'
_id_str = 'id'

class RegistryError(GangaException):

    def __init__(self, what=''):
        super(RegistryError, self).__init__(self, what)
        self.what = what

    def __str__(self):
        return "RegistryError: %s" % self.what


class RegistryAccessError(RegistryError):

    """ This error is raised if the request is valid in principle, 
        but the Registry cannot be accessed at the moment."""

    def __init__(self, what=''):
        super(RegistryAccessError, self).__init__(what)

    def __str__(self):
        return "RegistryAccessError: %s" % self.what


class RegistryLockError(RegistryError):

    """ This error is raised if the request is valid in principle,
        but the object is locked by another Ganga session"""

    def __init__(self, what=''):
        super(RegistryLockError, self).__init__(what)

    def __str__(self):
        return "RegistryLockError: %s" % self.what


class ObjectNotInRegistryError(RegistryError):

    """ This error is raised if an object has been associated to this registry,
        but is not actually in the registry. This most probably indicates an internal Ganga error."""

    def __init__(self, what=''):
        super(ObjectNotInRegistryError, self).__init__(what)

    def __str__(self):
        return "ObjectNotInRegistryError: %s" % self.what


class RegistryKeyError(RegistryError, KeyError):

    """ This error is raised if the given id is not found in the registry """

    def __init__(self, what=''):
        super(RegistryKeyError, self).__init__(what)

    def __str__(self):
        return "RegistryKeyError: %s" % self.what


class RegistryIndexError(RegistryError, IndexError):

    """ This error is raised if the given id is not found in the registry """

    def __init__(self, what=''):
        super(RegistryIndexError, self).__init__(what)

    def __str__(self):
        return "RegistryIndexError: %s" % self.what


def makeRepository(registry):
    """Factory that selects, imports and instantiates the correct GangaRepository"""
    if registry.type in ["LocalXML", "LocalPickle"]:
        from Ganga.Core.GangaRepository.GangaRepositoryXML import GangaRepositoryLocal
        return GangaRepositoryLocal(registry)
    elif registry.type in ["SQLite"]:
        from Ganga.Core.GangaRepository.GangaRepositorySQLite import GangaRepositorySQLite
        return GangaRepositorySQLite(registry)
    elif registry.type in ["Transient"]:
        from Ganga.Core.GangaRepository.GangaRepository import GangaRepository
        return GangaRepository(registry)
    elif registry.type in ["ImmutableTransient"]:
        from Ganga.Core.GangaRepository.GangaRepositoryImmutableTransient import GangaRepositoryImmutableTransient
        return GangaRepositoryImmutableTransient(registry, registry.location, registry.file_ext, registry.pickle_files)
    else:
        raise RegistryError("Repository %s: Unknown repository type %s" % (registry.name, registry.type))


class IncompleteObject(GangaObject):

    """ This class represents an object that could not be loaded on startup"""

    _schema = Schema(Version(0, 0), {})
    _name = "IncompleteObject"
    _category = "internal"
    _hidden = 1

    _exportmethods = ['reload', 'remove', '__repr__']

    def __init__(self, registry, this_id):
        super(IncompleteObject, self).__init__()
        self.registry = registry
        self.id = this_id

    def reload(self):
        self.registry._lock.acquire()
        try:

            if self.id in self.registry.dirty_objs.keys() and self.registry.checkShouldFlush():
                self.registry.repository.flush([self.registry._objects[self.id]])
                self.registry._load([self.id])
            if self.id not in self.registry_loaded_ids:
                self.registry._load([self.id])
                self.registry._loaded_ids.append(self.id)
            logger.debug("Successfully reloaded '%s' object #%i!" % (self.registry.name, self.id))
            for d in self.registry.changed_ids.itervalues():
                d.add(self.id)
        finally:
            self.registry._lock.release()

    def remove(self):
        self.registry._lock.acquire()
        try:
            if len(self.registry.repository.lock([self.id])) == 0:
                errstr = "Could not lock '%s' object #%i!" % (self.registry.name, self.id)
                try:
                    errstr += " Object is locked by session '%s' " % self.registry.repository.get_lock_session(self.id)
                except Exception as err:
                    logger.debug("Remove Lock error: %s" % str(err))
                raise RegistryLockError(errstr)
            self.registry.repository.delete([self.id])
            for d in self.registry.changed_ids.itervalues():
                d.add(self.id)
        finally:
            self.registry._lock.release()

    def __repr__(self):
        return "Incomplete object in '%s', ID %i. Try reload() or remove()." % (self.registry.name, self.id)


class Registry(object):

    """Ganga Registry
    Base class providing a dict-like locked and lazy-loading interface to a Ganga repository
    """

    def __init__(self, name, doc, dirty_flush_counter=10, update_index_time=30, dirty_max_timeout=60, dirty_min_timeout=30):
        """Registry constructor, giving public name and documentation"""
        self.name = name
        self.doc = doc
        self._hasStarted = False
        self.dirty_flush_counter = dirty_flush_counter
        self.dirty_objs = {}
        self.dirty_hits = 0
        self.update_index_time = update_index_time
        self._update_index_timer = 0
        self._needs_metadata = False
        self.metadata = None
        self._lock = threading.RLock()
        self.hard_lock = {}
        self.changed_ids = {}
        self._autoFlush = True

        self._loaded_ids = []

        self._parent = None

        self.repository = None
        self._objects = None
        self._incomplete_objects = None

        ## Record the last dirty and flush times to determine whether an idividual flush command should flush
        ## Logc to use these is implemented in checkShouldFlush()
        self._dirtyModTime = None
        self._flushLastTime = None
        self._dirty_max_timeout = dirty_max_timeout
        self._dirty_min_timeout = dirty_min_timeout


        self._inprogressDict = {}
        self._accessLockDict = {}


        self.shouldReleaseRun = True
#        self.releaseThread = threading.Thread(target=self.trackandRelease, args=())
#        self.releaseThread.daemon = True
#        self.releaseThread.start()

    def hasStarted(self):
        return self._hasStarted

    def lock_transaction(self, this_id, action):
        while this_id in self._inprogressDict.keys():
            logger.debug("Getting item being operated on: %s" % this_id)
            logger.debug("Currently in state: %s" % self._inprogressDict[this_id])
            #import traceback
            #traceback.print_stack()
            #import sys
            #sys.exit(-1)
            #time.sleep(0.05)
        self._inprogressDict[this_id] = action
        if this_id not in self.hard_lock.keys():
            self.hard_lock[this_id] = threading.Lock()
        self.hard_lock[this_id].acquire()

    def unlock_transaction(self, this_id):
        self._inprogressDict[this_id] = False
        del self._inprogressDict[this_id]
        self.hard_lock[this_id].release()

    # Methods intended to be called from ''outside code''
    def __getitem__(self, this_id):
        """ Returns the Ganga Object with the given id.
            Raise RegistryKeyError"""
        logger.debug("__getitem__")
        #self._lock.acquire()
        try:
            self.lock_transaction( this_id, "_getitem")

            real_id = None
            if type(this_id) is int:
                if this_id >= 0:
                    ## +ve integer, should be in dictionary
                    real_id = this_id
                    this_obj = self._objects[this_id]
                else:
                    ## -ve integer should be a relative object in dictionary
                    real_id = self._objects.keys()[this_id]
                    this_obj = self._objects[real_id]
            else:
                ## NOT an integer, maybe it's a slice or other?
                this_obj = self._objects[this_id]

            logger.debug("found_object")

            found_id = None
            if hasattr(this_obj, _id_str):
                found_id = getattr(this_obj, _id_str)
            if hasattr(this_obj, _reg_id_str):
                found_id = getattr(this_obj, _reg_id_str)
            if found_id is not None and real_id is not None:
                assert( found_id == real_id )

            logger.debug("Checked ID")

            return this_obj

        except KeyError as err:
            logger.debug("Repo KeyError: %s" % str(err))
            logger.debug("Keys: %s id: %s" % (str(self._objects.keys()), str(this_id)))
            if this_id in self._incomplete_objects:
                return IncompleteObject(self, this_id)
            raise RegistryKeyError("Could not find object #%s" % this_id)
        finally:
            self.unlock_transaction(this_id)
        #    self._lock.release()

    def __len__(self):
        """ Returns the current number of root objects """
        logger.debug("__len__")
        #self._lock.acquire()
        try:
            return len(self._objects)
        finally:
            pass
        #    self._lock.release()

    def __contains__(self, this_id):
        """ Returns True if the given ID is in the registry """
        logger.debug("__contains__")
        #self._lock.acquire()
        try:
            return this_id in self._objects
        finally:
            pass
        #    self._lock.release()

    def updateLocksNow(self):
        loger.debug("updateLocksNow")
        #self._lock.acquire()
        try:
            self.repository.updateLocksNow()
            return
        finally:
            pass
        #    self._lock.release()

    def trackandRelease(self):

        while self.shouldReleaseRun is True:

            ## Needed import for shutdown
            import time
            timeNow = time.time()

            modTime = self._dirtyModTime
            if modTime is None:
                modTime = timeNow
            dirtyTime = self._flushLastTime
            if dirtyTime is None:
                dirtyTime = timeNow

            delta_1 = abs(timeNow - modTime)
            delta_2 = abs(timeNow - dirtyTime)

            if delta_1 > self._dirty_max_timeout and delta_2 > self._dirty_max_timeout:

                 flush_thread = threading.Thread(target=self._flush, args=())
                 flush_thread.run()

                 for obj in self.dirty_objs.itervalues():

                    if self.shouldReleaseRun is False:
                        break

                    _args = ([obj])
                    release_thread = threading.Thread(target=self._release_lock, args=_args)
                    release_thread.run()

                 self.dirty_objs = {}
    
            time.sleep(0.5)

    def turnOffAutoFlushing(self):
        self._autoFlush = False

    def turnOnAutoFlushing(self):
        self._autoFlush = True
        if self.checkShouldFlush():
            self._backgroundFlush()

    def isAutoFlushEnabled(self):
        return self._autoFlush

    def checkDirtyFlushtimes(self, timeNow):
        self._dirtyModTime = timeNow
        if self._flushLastTime is None:
            self._flushLastTime = timeNow

    def checkShouldFlush(self):
        logger.debug("checkShouldFlush")
        self._lock.acquire()

        timeNow = time.time()

        self.checkDirtyFlushtimes(timeNow)

        timeDiff = (self._dirtyModTime - self._flushLastTime)

        if timeDiff > self._dirty_min_timeout:
            hasMinTimedOut = True
        else:
            hasMinTimedOut = False

        if timeDiff > self._dirty_max_timeout:
            hasMaxTimedOut = True
        else:
            hasMaxTimedOut = False

        if self.dirty_hits > self.dirty_flush_counter:
            countLimitReached = True
        else:
            countLimitReached = False

        # THIS IS THE MAIN DECISION ABOUT WHETHER TO FLUSH THE OBJECT TO DISK!!!
        # if the minimum amount of time has passed __AND__ we meet a sensible condition for wanting to flush to disk
        decision = hasMinTimedOut and (hasMaxTimedOut or countLimitReached)
       
        ## This gives us the ability to automatically turn off the automatic flushing externally if required
        decision = decision and self._autoFlush

        ## Can't autosave if a flush is in progress. Wait until next time.
        if len(self._inprogressDict.keys()) != 0:
            decision = False

        if decision is True:
            self._flushLastTime = timeNow
            self.dirty_hits = 0

        self._lock.release()

        return decision

    def _getObjects(self):
        logger.debug("_getObjects")
        #self._lock.acquire()
        returnable = self._objects
        #self._lock.release()
        return returnable

    def ids(self):
        """ Returns the list of ids of this registry """
        logger.debug("ids")
        #self._lock.acquire()
        try:
            if self.hasStarted() is True and\
                    (time.time() > self._update_index_timer + self.update_index_time):
                try:
                    changed_ids = self.repository.update_index()
                    for this_d in self.changed_ids.itervalues():
                        this_d.update(changed_ids)
                except Exception as err:
                    pass
                finally:
                    pass
                self._update_index_timer = time.time()

            return sorted(self._objects.keys())
        finally:
            pass
        #    self._lock.release()

    def items(self):
        """ Return the items (ID,obj) in this registry. 
        Recommended access for iteration, since accessing by ID can fail if the ID iterator is old"""
        logger.debug("items")
        self._lock.acquire()
        try:
            if self.hasStarted() is True and\
                    (time.time() > self._update_index_timer + self.update_index_time):
                try:
                    changed_ids = self.repository.update_index()
                    for this_d in self.changed_ids.itervalues():
                        this_d.update(changed_ids)
                except Exception as err:
                    pass
                finally:
                    pass

                self._update_index_timer = time.time()

            return sorted(self._objects.items())
        finally:
            self._lock.release()

    def iteritems(self):
        """ Return the items (ID,obj) in this registry."""
        logger.debug("iteritems")
        #self._lock.acquire()
        returnable = self.items()
        #self._lock.release()
        return returnable

    def _checkObjects(self):
        for key, _obj in self._objects.iteritems():
            summary = "found: "
            try:
                if hasattr(_obj, _id_str):
                    summary = summary + "%s = '%s'" % (_id_str, getattr(_obj, _id_str))
                    assert(getattr(_obj, _id_str) == key)
                if hasattr(_obj, _reg_id_str):
                    summary = summary + " %s = '%s'" % (_reg_id_str, getattr(_obj, _reg_id_str))
                    assert(getattr(_obj, _reg_id_str) == key)
            except:
                logger.error(summary)
                raise
        return

    def keys(self):
        """ Returns the list of ids of this registry """
        logger.debug("keys")
        #self._lock.acquire()
        returnable = self.ids()
        #self._lock.release()
        return returnable

    def values(self):
        """ Return the objects in this registry, in order of ID.
        Besides items() this is also recommended for iteration."""
        logger.debug("values")
        #self._lock.acquire()
        returnable = [it[1] for it in self.items()]
        #self._lock.release()
        return returnable

    def __iter__(self):
        logger.debug("__iter__")
        #self._lock.acquire()
        returnable = iter(self.values())
        #self._lock.release()
        return returnable

    def find(self, _obj):
        """Returns the id of the given object in this registry, or 
        Raise ObjectNotInRegistryError if the Object is not found"""

        obj = stripProxy(_obj)
        try:
            if hasattr(obj, _reg_id_str):
                obj_reg_id = getattr(obj, _reg_id_str)
                objects_obj = self._objects[obj_reg_id]
                assert obj == objects_obj
                if hasattr(obj, _id_str):
                    if hasattr(objects_obj, _id_str):
                        assert getattr(obj, _id_str) == getattr(objects_obj, _id_str)
                assert obj_reg_id == getattr(objects_obj, _reg_id_str)
                return obj_reg_id
            elif hasattr(obj, _id_str):
                obj_id = getattr(obj, _id_str)
                objects_obj = self._objects[obj_id]
                if hasattr(objects_obj, _reg_id_str):
                    assert obj_id == getattr(objects_obj, _reg_id_str)
                assert obj_id == getattr(objects_obj, _id_str)
                return obj_id
            else:
                raise ObjectNotInRegistryError("Repo find: Object '%s' does not seem to be in this registry: %s !" % (getName(obj), self.name))
        except AttributeError as err:
            logger.debug("%s" % str(err))
            raise ObjectNotInRegistryError("Object %s does not seem to be in any registry!" % getName(obj))
        except AssertionError as err:
            logger.warning("%s" % str(err))
            raise ObjectNotInRegistryError("Object '%s' is a duplicated version of the one in this registry: %s !" % (getName(obj), self.name))
        except KeyError as err:
            logger.debug("%s", str(err))
            raise ObjectNotInRegistryError("Object '%s' does not seem to be in this registry: %s !" % (getName(obj), self.name))
        finally:
            pass

    def clean(self, force=False):
        """Deletes all elements of the registry, if no other sessions are present.
        if force == True it removes them regardless of other sessions.
        Returns True on success, False on failure."""
        logger.debug("clean")
        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot clean a disconnected repository!")
        self._lock.acquire()
        try:
            if not force:
                other_sessions = self.repository.get_other_sessions()
                if len(other_sessions) > 0:
                    logger.error("The following other sessions are active and have blocked the clearing of the repository: \n * %s" % ("\n * ".join(other_sessions)))
                    return False
            self.repository.reap_locks()
            self.repository.delete(self._objects.keys())
            self.dirty_objs = {}
            self.dirty_hits = 0
            self.changed_ids = {}
            self.repository.clean()
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise err
        except Exception as err:
            logger.debug("Clean Unknown Err: %s" % str(err))
            raise err
        finally:
            self._lock.release()

    def __safe_add(self, obj, force_index=None):
        logger.debug("__safe_add")
        if force_index is None:
            ids = self.repository.add([obj])
        else:
            if len(self.repository.lock([force_index])) == 0:
                raise RegistryLockError("Could not lock '%s' id #%i for a new object!" % (self.name, force_index))
            ids = self.repository.add([obj], [force_index])

        obj._setRegistry(self)
        obj._registry_locked = True

        this_id = self.find(obj)
        self.lock_transaction(this_id, "_add")

        self.repository.flush(ids)
        for this_v in self.changed_ids.itervalues():
            this_v.update(ids)

        for _id in ids:
            if hasattr(self._objects[_id], _reg_id_str):
                assert(getattr(self._objects[_id], _reg_id_str) == _id)
            if hasattr(self._objects[_id], _id_str):
                assert(getattr(self._objects[_id], _id_str) == _id)

        logger.debug("_add-ed as: %s" % str(ids))
        self.unlock_transaction(this_id)
        return ids[0]

    # Methods that can be called by derived classes or Ganga-internal classes like Job
    # if the dirty objects list is modified, the methods must be locked by self._lock
    # all accesses to the repository must also be locked!

    def _add(self, _obj, force_index=None):
        """ Add an object to the registry and assigns an ID to it. 
        use force_index to set the index (for example for metadata). This overwrites existing objects!
        Raises RepositoryError"""
        logger.debug("_add")
        obj = stripProxy(_obj)

        if self.hasStarted() is not True:
            raise RepositoryError("Cannot add objects to a disconnected repository!")

        self._lock.acquire()
        this_id = None
        returnable_id = None

        try:
            returnable_id = self.__safe_add(obj, force_index)
            self._loaded_ids.append(returnable_id)
        except (RepositoryError) as err:
            raise err
        except Exception as err:
            logger.debug("Unknown Add Error: %s" % str(err))
            raise err
        finally:
            self._lock.release()

        self._updateIndexCache(obj)

        return returnable_id

    def _remove(self, _obj, auto_removed=0):
        """ Private method removing the obj from the registry. This method always called.
        This method may be overriden in the subclass to trigger additional actions on the removal.
        'auto_removed' is set to true if this method is called in the context of obj.remove() method to avoid recursion.
        Only then the removal takes place. In the opposite case the obj.remove() is called first which eventually calls
        this method again with "auto_removed" set to true. This is done so that obj.remove() is ALWAYS called once independent
        on the removing context.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError"""
        logger.debug("_remove")
        self._lock.acquire()
        obj = stripProxy(_obj)
        try:
            self.__reg_remove(obj, auto_removed)
        except ObjectNotInRegistryError as err:
            try:
                ## Actually  make sure we've removed the object from the repo 
                if hasattr(obj, _reg_id_str):
                    del self._objects[getattr(obj, _reg_id_str)]
            except Exception as err:
                pass
            pass
        except Exception as err:
            raise err
        finally:
            self._lock.release()

    def __reg_remove(self, obj, auto_removed=0):

        logger.debug("_reg_remove")
        u_id = self.find(obj)

        obj_id = id(obj)

        self.lock_transaction(obj_id, "_remove")


        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot remove objects from a disconnected repository!")
        if not auto_removed and hasattr(obj, "remove"):
            obj.remove()
        else:
            this_id = self.find(obj)
            try:
                self._write_access(obj)
            except RegistryKeyError as err:
                logger.debug("Registry KeyError: %s" % str(err))
                logger.warning("double delete: Object #%i is not present in registry '%s'!" % (this_id, self.name))
                return
            logger.debug('deleting the object %d from the registry %s', this_id, self.name)
            try:
                if getattr(obj, _reg_id_str) in self.dirty_objs.keys():
                    del self.dirty_objs[getattr(obj, _reg_id_str)]
                self.repository.delete([this_id])
                del obj
                for this_v in self.changed_ids.itervalues():
                    this_v.add(this_id)
            except (RepositoryError, RegistryAccessError, RegistryLockError) as err:
                raise err
            except Exception as err:
                logger.debug("unknown Remove Error: %s" % str(err))
                raise err
            finally:
       
                self.unlock_transaction(obj_id)

    def _backgroundFlush(self, _objs=None):

        if _objs is not None:
            objs = [stripProxy(obj) for obj in _objs]
        else:
            objs = [obj for obj in self.dirty_objs.itervalues()]

        if False:
            thread = threading.Thread(target=self._flush, args=())
            thread.daemon = True
            thread.run()
        else:
            self._flush(objs)

    def _dirty(self, _obj):
        """ Mark an object as dirty.
        Trigger automatic flush after specified number of dirty hits
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError"""
        logger.debug("_dirty")
        obj = stripProxy(_obj)
        self._updateIndexCache(obj)

        if self.find(obj) in self._inprogressDict.keys():
            self.dirty_objs[getattr(obj, _reg_id_str)] = obj
            self.dirty_hits += 1
            return

        self._write_access(obj)
        self._lock.acquire()
        try:
            self.dirty_objs[getattr(obj, _reg_id_str)] = obj
            self.dirty_hits += 1
            if self.checkShouldFlush():
                self._backgroundFlush([obj])
            # HACK for GangaList: there _dirty is called _before_ the object is
            # modified
            for this_d in self.changed_ids.itervalues():
                this_d.add(self.find(obj))
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise err
        except Exception as err:
            logger.debug("Unknown Flush Exception: %s" % str(err))
            raise err
        finally:
            self._lock.release()

    def _flush(self, _objs=None):
        """Flush a set of objects to the persistency layer immediately
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError"""
        logger.debug("_flush")
        self._lock.acquire()

        if _objs is not None and isType(_objs, (list, tuple, GangaList)):
            objs = [stripProxy(_obj) for _obj in _objs]
        elif _objs is not None:
            objs = [stripProxy(_objs)]
        else:
            objs = []

        obj_ids = []
        for obj in objs:
            this_id = id(self.find(obj))
            obj_ids.append(this_id)
            self.lock_transaction(this_id, '_flush')

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot flush to a disconnected repository!")
        for obj in objs:
            self._write_access(obj)

        try:
            for obj in objs:
                self.dirty_objs[getattr(obj, _reg_id_str)] = obj
            ids = []
            for reg_id, obj in self.dirty_objs.iteritems():
                try:
                    ids.append(reg_id)
                except ObjectNotInRegistryError as err:
                    logger.error("flush: Object: %s not in Repository: %s" % (str(obj), str(err)))
                    raise err
            logger.debug("repository.flush(%s)" % ids)
            self.repository.flush(ids)
            self.repository.unlock(ids)
            self.dirty_objs = {}
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise err
        except Exception as err:
            logger.error("_flush Error: %s" % str(err))
            raise err
        finally:

            for obj_id in obj_ids:
                self.unlock_transaction(obj_id)

            self._lock.release()

    def _read_access(self, _obj, sub_obj=None):
        """Obtain read access on a given object.
        sub-obj is the object the read access is actually desired (ignored at the moment)
        Raise RegistryAccessError
        Raise RegistryKeyError"""
        logger.debug("_read_access")
        obj_id = id(stripProxy(_obj))
        if obj_id in self._inprogressDict.keys() or obj_id in self._accessLockDict.keys():
            return
        else:
            self._accessLockDict[id(_obj)] = _obj

        self._lock.acquire()
        try:
            self.__safe_read_access(_obj, sub_obj)
        except Exception as err:
            raise err
        finally:
            if id(_obj) in self._accessLockDict.keys():
                del self._accessLockDict[id(_obj)]
            self._lock.release()

    def _updateIndexCache(self, _obj):
        logger.debug("_updateIndexCache")
        obj = stripProxy(_obj)
        if self.find(obj) in self._inprogressDict.keys():
            return

        self._lock.acquire()
        try:
            self.repository.updateIndexCache(obj)
        finally:
            self._lock.release()

    def _load(self, obj_ids):
        logger.debug("_load")
        these_ids = []
        for obj_id in obj_ids:
            this_id = id(self[obj_id])
            these_ids.append(this_id)
            self.lock_transaction(this_id, "_load")

        self._lock.acquire()
        try:
            for obj_id in obj_ids:
                self.repository.load([obj_id])
        except Exception as err:
            logger.error("Error Loading Jobs!")
            raise err
        finally:
            for obj_id in these_ids:
                self.unlock_transaction(obj_id)
            self._lock.release()

    def __safe_read_access(self,  _obj, sub_obj):
        logger.debug("_safe_read_access")
        obj = stripProxy(_obj)
        if self.find(obj) in self._inprogressDict.keys():
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("The object #%i in registry '%s' is not fully loaded and the registry is disconnected! Type 'reactivate()' if you want to reconnect." % (self.find(obj), self.name))

        if hasattr(obj, "_registry_refresh"):
            delattr(obj, "_registry_refresh")
        assert not hasattr(obj, "_registry_refresh")

        try:
            this_id = self.find(obj)
            try:
                if this_id in self.dirty_objs.keys() and self.checkShouldFlush():
                    self._flush([self._objects[this_id]])
                if this_id not in self._loaded_ids:
                    self._load([this_id])
                    self._loaded_ids.append(this_id)
            except KeyError as err:
                logger.error("_read_access KeyError %s" % str(err))
                raise RegistryKeyError("Read: The object #%i in registry '%s' was deleted!" % (this_id, self.name))
            except InaccessibleObjectError as err:
                raise RegistryKeyError("Read: The object #%i in registry '%s' could not be accessed - %s!" % (this_id, self.name, str(err)))
            #finally:
            #    pass
            for this_d in self.changed_ids.itervalues():
                this_d.add(this_id)
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise err
        except Exception as err:
            logger.debug("Unknown read access Error: %s" % str(err))
            raise err
        #finally:
        #    pass

    def _write_access(self, _obj):
        """Obtain write access on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise RegistryLockError
        Raise ObjectNotInRegistryError (via self.find())"""
        logger.debug("_write_access")
        obj = stripProxy(_obj)
        obj_id = id(_obj)
        if obj_id in self._inprogressDict.keys() or obj_id in self._accessLockDict.keys():
            return
        else:
            self._accessLockDict[obj_id] = obj

        self._lock.acquire()

        try:
            self.__write_access(obj)
        except Exception as err:
            raise err
        finally:

            if obj_id in self._accessLockDict.keys():
                del self._accessLockDict[obj_id]
            self._lock.release()

    def __write_access(self, _obj):
        logger.debug("__write_acess")
        obj = stripProxy(_obj)

        if id(obj) in self._inprogressDict.keys():
            this_id = self.find(obj)
            for this_d in self.changed_ids.itervalues():
                this_d.add(this_id)
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot get write access to a disconnected repository!")
        if not hasattr(obj, '_registry_locked') or not obj._registry_locked:
            try:
                this_id = self.find(obj)
                try:
                    if len(self.repository.lock([this_id])) == 0:
                        errstr = "Could not lock '%s' object #%i!" % (self.name, this_id)
                        try:
                            errstr += " Object is locked by session '%s' " % self.repository.get_lock_session(this_id)
                        except RegistryLockError as err:
                            raise err
                        except Exception as err:
                            logger.debug( "Unknown Locking Exception: %s" % str(err) )
                            raise err
                        raise RegistryLockError(errstr)
                except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
                    raise err
                except Exception as err:
                    logger.debug("Unknown write access Error: %s" % str(err))
                    raise err
                finally:  # try to load even if lock fails
                    try:
                        if this_id in self.dirty_objs.keys() and self.checkShouldFlush():
                            self._flush([self._objects[this_id]])
                        if this_id not in self._loaded_ids:
                            self._load([this_id])
                            self._loaded_ids.append(this_id)
                            if hasattr(obj, "_registry_refresh"):
                                delattr(obj, "_registry_refresh")
                    except KeyError, err:
                        logger.debug("_write_access KeyError %s" % str(err))
                        raise RegistryKeyError("Write: The object #%i in registry '%s' was deleted!" % (this_id, self.name))
                    except InaccessibleObjectError as err:
                        raise RegistryKeyError("Write: The object #%i in registry '%s' could not be accessed - %s!" % (this_id, self.name, str(err)))
                    #finally:
                    #    pass
                    for this_d in self.changed_ids.itervalues():
                        this_d.add(this_id)
                obj._registry_locked = True
            except Exception as err:
                raise err
            #finally:
            #    pass

        return True

    def _release_lock(self, obj):
        """Release the lock on a given object.
        Raise RepositoryError
        Raise RegistryAccessError
        Raise ObjectNotInRegistryError"""

        try:
            self.__release_lock(obj)
        except ObjectNotInRegistryError as err:
            pass
        except Exception as err:
            logger.debug("Unknown exception %s" % str(err))
            raise err

    def __release_lock(self, _obj):
        logger.debug("_release_lock")
        obj = stripProxy(_obj)

        if self.find(obj) in self._inprogressDict.keys():
            return

        if self.hasStarted() is not True:
            raise RegistryAccessError("Cannot manipulate locks of a disconnected repository!")
        logger.debug("Reg: %s _release_lock(%s)" % (self.name, str(self.find(obj))))
        self._lock.acquire()
        try:
            if hasattr(obj, '_registry_locked') and obj._registry_locked:
                oid = self.find(obj)
                if getattr(obj, _reg_id_str) in self.dirty_objs.keys():
                    self.repository.flush([oid])
                    if getattr(obj, _reg_id_str) in self.dirty_objs:
                        del self.dirty_objs[getattr(obj, _reg_id_str)]
                obj._registry_locked = False
                self.repository.unlock([oid])
        except (RepositoryError, RegistryAccessError, RegistryLockError, ObjectNotInRegistryError) as err:
            raise err
        except Exception as err:
            logger.debug("un-known registry release lock err!")
            logger.debug("Err: %s" % str(err))
            raise err
        finally:
            self._lock.release()

    def pollChangedJobs(self, name):
        """Returns a list of job ids that changed since the last call of this function.
        On first invocation returns a list of all ids.
        "name" should be a unique identifier of the user of this information."""
        logger.debug("pollChangedJobs")
        self._lock.acquire()
        try:
            if self.hasStarted() is True and\
                    (time.time() > self._update_index_timer + self.update_index_time):
                changed_ids = self.repository.update_index()
                for this_d in self.changed_ids.itervalues():
                    this_d.update(changed_ids)
                self._update_index_timer = time.time()
            res = self.changed_ids.get(name, set(self.ids()))
            self.changed_ids[name] = set()
            return res
        finally:
            self._lock.release()

    def getIndexCache(self, obj):
        """Returns a dictionary to be put into obj._index_cache through setNodeIndexCache
        This can and should be overwritten by derived Registries to provide more index values."""
        return {}

    def startup(self):
        """Connect the repository to the registry. Called from Repository_runtime.py"""
        logger.debug("startup")
        self._lock.acquire()
        try:
            self._hasStarted = True
            t0 = time.time()
            self.repository = makeRepository(self)
            self._objects = self.repository.objects
            self._incomplete_objects = self.repository.incomplete_objects

            if self._needs_metadata:
                t2 = time.time()
                if self.metadata is None:
                    self.metadata = Registry(self.name + ".metadata", "Metadata repository for %s" % self.name, dirty_flush_counter=self.dirty_flush_counter, update_index_time=self.update_index_time)
                    self.metadata.type = self.type
                    self.metadata.location = self.location
                    setattr(self.metadata, '_parent', self) ## rcurrie Registry has NO '_parent' Object so don't understand this is this used for JobTree?
                logger.debug("metadata startup")
                self.metadata.startup()
                t3 = time.time()
                logger.debug("Startup of %s.metadata took %s sec" % (str(self.name), str(t3-t2)))

            logger.debug("repo startup")
            #self.hasStarted() = True
            self.repository.startup()
            # All Ids could have changed
            self.changed_ids = {}
            t1 = time.time()
            logger.debug("Registry '%s' [%s] startup time: %s sec" % (self.name, self.type, t1 - t0))
        except Exception as err:
            logger.debug("Logging Repo startup Error: %s" % str(err))
            self._hasStarted = False
            raise err
        finally:
            self._lock.release()

    def shutdown(self):
        """Flush and disconnect the repository. Called from Repository_runtime.py """
        from Ganga.Utility.logging import getLogger
#        self.shouldReleaseRun = False
#        self.releaseThread.stop()
        logger = getLogger()
        logger.debug("Shutting Down Registry")
        logger.debug("shutdown")
        self._lock.acquire()
        try:
            self._hasStarted = True
            try:
                if not self.metadata is None:
                    try:
                        self._flush()
                    except Exception, err:
                        logger.debug("shutdown _flush Exception: %s" % str(err))
                    self.metadata.shutdown()
            except Exception as err:
                logger.debug("Exception on shutting down metadata repository '%s' registry: %s", self.name, str(err))
            #finally:
            #    pass
            try:
                self._flush()
            except Exception as err:
                logger.error("Exception on flushing '%s' registry: %s", self.name, str(err))
                #raise err
            #finally:
            #    pass
            for obj in self._objects.values():
                # locks are not guaranteed to survive repository shutdown
                obj._registry_locked = False
            self.repository.shutdown()

            self._loaded_ids = []

        finally:
            self._hasStarted = False
            self._lock.release()

    def info(self, full=False):
        """Returns an informative string onFlush and disconnect the repository. Called from Repository_runtime.py """
        logger.debug("info")
        #self._lock.acquire()
        try:
            s = "registry '%s': %i objects" % (self.name, len(self._objects))
            if full:
                other_sessions = self.repository.get_other_sessions()
                if len(other_sessions) > 0:
                    s += ", %i other concurrent sessions:\n * %s" % (len(other_sessions), "\n * ".join(other_sessions))
            return s
        finally:
            pass
            #self._lock.release()

    def print_other_sessions(self):
        other_sessions = self.repository.get_other_sessions()
        if len(other_sessions) > 0:
            logger.warning("%i other concurrent sessions:\n * %s" % (len(other_sessions), "\n * ".join(other_sessions)))

    def has_loaded(self, obj):
        """Returns True/False for if a given object has been fully loaded by the Registry.
        Returns False on the object not being in the Registry!
        This ONLY applies to master jobs as the Registry has no apriori knowledge of the subjob structure.
        Consult SubJobXMLList for a more fine grained loaded/not-loaded info/test."""
        try:
            index = self.find(obj)
        except ObjectNotInRegistryError:
            return False

        if index in self._loaded_ids:
            return True
        else:
            return False

