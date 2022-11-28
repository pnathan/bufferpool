########################################
# mempool.py
#
# an implementation of a mempool in Python 3.
#
# (C) AGPL3 Paul Nathan 2022
import json
import os
import random

class UniqueStack(object):
    # A space-inefficient means of having a unique priority queue. A
    # set maintains the unique facility and rapid tests for existence.
    # A vector maintains the ordering.
    #
    # Space: 2*n
    #
    # Time: n on push (linear vector delete).
    #
    # The alternative here is, likely, working out a Heap or variant
    # thereof. That would be find - O(n) + delete - O(lg n) + insert -
    # O(lg n), where we are at O(n). If we could implement insert with
    # only optionally deleting/inserting "if we find", then that
    # should be done. But, too, heaps require a "key", whereas we
    # maintain here the order by simple indexing and the location of
    # the data in the linear data structure.
    def __init__(self):
        self._d = set()
        self._o = []
    def push(self, e):
        if e in self._d:
            r = []
            for i in range(0, len(self._o)):
                if self._o[i] == e:
                    # splice them together without e
                    r = self._o[:i] + self._o[i+1:]
            r.append(e)
            self._o = r
        else:
            self._d.add(e)
            self._o.append(e)
    # just whack it.
    def delete(self, e):
        self._d.remove(e)
        r = self._o
        for i in range(0, len(self._o)):
            if self._o[i] == e:
                # splice them together without e
                r = self._o[:i] + self._o[i+1:]
        self._o = r

    def pop(self):
        r = self._o[len(self._o) - 1]
        self._d.remove(r)
        self._o.pop()
        return r

    def top(self):
        return self._o[len(self._o) - 1]

    def bottom(self):
        return self._o[0]

    def __getitem__(self, i):
        return self._o[i]
    def __len__(self):
        return len(self._d)
    def __repr__(self):
        return "< " + ", ".join(map(str, self._o)) + " > "


class FramePool(object):
    def assess_size(self):
        raise NotImplemented
    def size(self, id):
        raise NotImplemented
    def read_frame(self, id):
        raise NotImplemented
    def write_frame(self, id, data):
        raise NotImplemented

class DiskPool(FramePool):
    def __init__(self, dirname):
        self._dirname = dirname
        self._size = None

    def preallocate(self, size, f):
        for i in range(0, size):
            self.write_frame(i, PageFrame(f(i)))
        self._size = size

    def assess_size(self):
        flist = []
        # this essentially requires a flock on the directory.
        with os.scandir(path=self._dirname) as it:
            for entry in it:
                if entry.is_file():
                    flist.append(entry.name)
        counter=0
        for f in flist:
            if f.startswith("page_"):
                counter+=1
        self._size = counter
        return counter

    def size(self):
        return self._size

    def read_frame(self, pageid):
        with open(os.path.join(self._dirname, f"page_{pageid}"), 'r') as f:
            return PageFrame(json.loads(f.read()))

    def write_frame(self, pageid, data):
        assert isinstance(data, PageFrame)
        with open(os.path.join(self._dirname, f"page_{pageid}"), 'w') as f:
            f.write(json.dumps(data.data()))

class MockPool(FramePool):
    def __init__(self, limit):
        self._frames = {}
    def assess_size():
        return len(self._frames)
    def read_frame(self, pageid):
        return frame[pageid]
    def write_frame(self, pageid, data):
        assert isinstance(data, PageFrame)
        frame[pageid] = data

class PageFrame(object):
    # a Page is created, associated with some specific data frame.
    def __init__(self, data):
        self._frame = data
        # one pin per thread using the page.
        self._pins = 0
        # should the frame know it's dirty? or should the FramePool
        # track whether its dirty or not?
        self._dirty = False
    def __repr__(self):
        return f"p: {self._pins}, d: {self._dirty}: {self._frame}"
    def data(self):
        return self._frame
    def set_data(self, data):
        self._dirty = True
        self._frame = data
    def count_pins(self):
        return self._pins
    def inc_pin(self):
        self._pins = self._pins + 1
        return self._pins
    def dec_pin(self):
        self._pins = self._pins - 1
        return self._pins
    def is_dirty(self):
        return self._dirty
    def make_dirty(self):
        self._dirty = True

    def __enter__(self):
        self.inc_pin()
        return self.data()

    def __exit__(self, x, y, z):
        self.dec_pin()

# interface: an evictor takes a list of pages and a unique Stack and return the index of
# the one to evict.
def random_evictor(pages, pageid_idx_map, lru):
    potential = random.randint(0, len(pages) - 1)
    while pages[potential].count_pins() != 0:
        potential = random.randint(0, len(pages) - 1)

    return potential

def bottom_evictor(pages, pageid_idx_map, lru):
    pageid = lru.bottom()
    for e in pageid_idx_map:
        if pageid_idx_map[e] == pageid:
            return e
    raise EvictionError()


class EvictionError(Exception):
    pass

class BufferPool(object):
    __slots__ = [
        '_size',
        # fixed number of pages
        '_pages',
        # pageid -> index
        '_active_pages',
        # index -> pageid
        '_reverse_active_pages',
        # lru
        '_stack',
        # backing store
        '_pool',
        # victim selector
        '_evictor',
        # unused, it seemed like a good idea at the time
        # page OIDs run from [0, _total_page_count) over integers.
        '_total_page_count',
    ]
    def __init__(self, size, pool, evictor):
        # This size is the size of the mempool.
        self._size = size
        self._pages = [None for x in range(0, size)]
        # map of pageid to index in self._pages
        self._active_pages = {}
        # map of index to pageid.
        self._reverse_active_pages = {}
        self._pool = pool
        self._evictor = evictor
        self._stack = UniqueStack()

    def release_page(self, id):
        """
        Page is released for later eviction
        """
        if id > self._pool.size() - 1:
            raise IndexError("mempool index out of range")

        if id not in self._active_pages:
            # this is not a valid page for writing: something has
            # evicted it from under our feet.
            raise EvictionError()

        self._active_pages[id].pin_dec()


    def acquire_page(self, id):
        """
        Page is acquired from its data source, if need be
        """
        p = self.get_page(id)
        p.inc_pin()
        return p

    def __getitem__(self, id):
        return self.get_page(id)

    def get_page(self, id):
        if id > self._pool.size() - 1:
            raise IndexError("mempool index out of range")

        # if we don't have the data already
        if id not in self._active_pages:

            # precondition: we don't have the page loaded

            if len(self._active_pages) == self._size:

                # precondition: we are full

                # victim index is the index in the array for the
                # (limited) list of pages. Evictors must check pin status.
                victim_index = self._evictor(self._pages, self._reverse_active_pages, self._stack)
                # victim pageid is the page victim_index points to
                victim_pageid = self._reverse_active_pages[victim_index]
                if self._pages[victim_index].is_dirty():
                    self._pool.write_frame(victim_pageid, self._pages[victim_index])

                self._pages[victim_index] = None
                del self._active_pages[victim_pageid]
                del self._reverse_active_pages[victim_index]
                # the Stack is indexed by the requested pageid.
                self._stack.delete(victim_pageid)

                # postcondition of this little block: we have one empty slot

            # precondition: we have at least one slot, which is
            # signified by a None element in the self._pages array

            target_index = None
            for i in range(0, self._size):
                if self._pages[i] == None:
                    target_index = i
                    break
            frame = self._pool.read_frame(id)
            self._pages[target_index] = frame
            self._active_pages[id] = frame
            self._reverse_active_pages[target_index] = id

            # postcondition: the frame is loaded into memory and wired into the map

        # push id onto the lru
        self._stack.push(id)
        return self._active_pages[id]
