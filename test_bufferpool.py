import unittest
import os
import tempfile
import json
import random

import bufferpool

class TestDiskPool(unittest.TestCase):

    def test_diskpool(self):
            d = tempfile.mkdtemp()
            with open(os.path.join(d, "page_0"), "w") as f:
                f.write(json.dumps("aleph bet gimel"))
            dp = bufferpool.DiskPool(1, d)
            # can we read things that already existed- without overwriting them?
            self.assertEqual(dp.read_frame(0).data(), "aleph bet gimel")
            example = { "index": True }
            dp.write_frame(1, bufferpool.PageFrame(example))
            self.assertEqual(dp.read_frame(1).data(), example)
            self.assertEqual(dp.assess_size(), 2)

class TestUniqueStack(unittest.TestCase):
    def test_unique_stack_normative(self):
        u = bufferpool.UniqueStack()
        u.push(10)
        self.assertEqual(len(u), 1)
        u.push(10)
        self.assertEqual(len(u), 1)
        x = u.pop()
        self.assertEqual(x, 10)
        self.assertEqual(len(u), 0)

        u.push(20)
        self.assertEqual(u[0], 20)

    def test_unique_stack_unique(self):
        u = bufferpool.UniqueStack()
        u.push(10)
        u.push(20)
        u.push(30)
        u.push(20)
        self.assertEqual(u.top(), 20)
        u.pop()
        self.assertEqual(u.top(), 30)
        u.pop()
        self.assertEqual(u.top(), 10)
        u.pop()
        self.assertEqual(len(u), 0)

    def test_unique_stack_del(self):
        u = bufferpool.UniqueStack()
        u.push(10)
        u.push(20)
        u.push(30)
        u.push(20)
        u.delete(30)
        self.assertEqual(len(u), 2)


class TestPage(unittest.TestCase):
    def test_happy(self):
        p = bufferpool.PageFrame({ 'data': True })
        self.assertEqual(p.count_pins(), 0)
        self.assertEqual(p.inc_pin(), 1)
        self.assertEqual(p.count_pins(), 1)
        self.assertEqual(p.inc_pin(), 2)
        self.assertEqual(p.count_pins(), 2)
        self.assertEqual(p.dec_pin(), 1)
        self.assertEqual(p.count_pins(), 1)
        self.assertEqual(p.dec_pin(), 0)
        self.assertEqual(p.count_pins(), 0)

class TestBufferPool(unittest.TestCase):
    def test_ensure_allocation(self):
        with tempfile.TemporaryDirectory() as d:
            dp = bufferpool.DiskPool(10, d)
            for i in range(0, 10):
                dp.write_frame(i, bufferpool.PageFrame({'init': i}))

            bp = bufferpool.BufferPool(3, dp, bufferpool.bottom_evictor)
            bp.ensure_allocation(2)
            for x in range(0, 10):
                with bp[x] as d:
                    self.assertEqual(d['init'], x)

            with self.assertRaises(IndexError):
                bp[10]
            bp.ensure_allocation(11)
            bp[10]


    def test_happy(self):
        with tempfile.TemporaryDirectory() as d:
            dp = bufferpool.DiskPool(10, d)

            for i in range(0, 10):
                dp.write_frame(i, bufferpool.PageFrame({'init': i}))

            bp = bufferpool.BufferPool(3, dp, bufferpool.bottom_evictor)

            with bp[0] as d:
                self.assertEqual(d['init'], 0)
            bp[1]
            bp[2]
            with bp[3] as d:
                pass
            bp[3]
            bp[4]
            bp[6]
            bp[4]
            bp[4]
            bp[9]
            with bp[9] as d:
                self.assertEqual(d['init'], 9)
            bp[4]

    def test_new_page(self):
        d = tempfile.mkdtemp()
        dp = bufferpool.DiskPool(5, d)
        for i in range(0, 5):
            dp.write_frame(i, bufferpool.PageFrame({'init': i}))

        bp = bufferpool.BufferPool(3, dp, bufferpool.bottom_evictor)

        # parameters: 3 slots; 5 backing frames on disk.
        bp[0]
        bp[1]
        bp[2]
        bp[3]
        bp[4]

        # all slots have been used, all frames read.
        bp.falloc()
        bp[5] ={"test": True}
        with bp[5] as d:
            self.assertEqual(d, {"test":True})


class TestSlabMapper(unittest.TestCase):
    def test_simple(self):
        #dp = bufferpool.MockPool(30)
        d = tempfile.mkdtemp()
        dp = bufferpool.DiskPool(5, d)
        bp = bufferpool.BufferPool(3, dp, bufferpool.bottom_evictor)
        sm = bufferpool.SlabMapper(bp, 10)
        dataset = list(map(lambda i: {'data': i}, range(100, 500)))
        sm.flush(dataset)
        got = sm.load()
        self.assertEqual(dataset, got)

if __name__ == '__main__':
    unittest.main()
