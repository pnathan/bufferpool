import unittest
import os
import tempfile
import json
import random

import bufferpool

class TestDiskPoll(unittest.TestCase):

    def test_diskpool(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "page_0"), "w") as f:
                f.write(json.dumps("aleph bet gimel"))
            dp = bufferpool.DiskPool(d)
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
    def test_happy(self):
        with tempfile.TemporaryDirectory() as d:
            dp = bufferpool.DiskPool(d)
            dp.preallocate(10, lambda i: {'init': i})
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


if __name__ == '__main__':
    unittest.main()
