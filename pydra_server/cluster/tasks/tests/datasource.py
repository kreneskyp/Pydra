from __future__ import with_statement

import unittest

from pydra_server.cluster.tasks.datasource import *
import tempfile, shutil

in_dict = { 
            "k1": ['one', 'two', 'four', 'two', 'four', 'seven'],
            "k2": ['seven', 'four', 'seven', 'seven'],
            "k3": ['seven', 'four', 'seven', 'seven'],
          }


class DatasourceDict_Test(unittest.TestCase):

    def setUp(self):
        self.source = DatasourceDict(in_dict)


    def test_datasourcedict(self):

        input = self.source

        for key in input:
            dkey = key[-1]
            self.assertEqual(in_dict[dkey], input.load(key))


    def test_sequenceslicer(self):

        source = self.source

        for send_as_input in (True, False):

            expected_seq = []

            for xs in in_dict.itervalues():
                for x in xs:
                    expected_seq.append(x)

            slicer = SequenceSlicer()
            slicer.input = source
            slicer.send_as_input = send_as_input

            for key in slicer:
                if not send_as_input:
                    self.assertEqual(len(key), 2)
                self.assertEqual(slicer.load(key), expected_seq.pop(0))


    def test_nested_sequenceslicer(self):

        source = self.source

        for send_as_input in (True, False):

            expected_seq = []

            for xs in in_dict.itervalues():
                for str in xs:
                    for char in str:
                        expected_seq.append(char)

            outer_slicer = SequenceSlicer()
            outer_slicer.input = source

            inner_slicer = SequenceSlicer()
            inner_slicer.input = outer_slicer
            inner_slicer.send_as_input = send_as_input

            for key in inner_slicer:
                if not send_as_input:
                    self.assertEqual(len(key), 3)
                self.assertEqual(inner_slicer.load(key), expected_seq.pop(0))


class DatasourceDir_Test(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

        # generate input files
        for filename, lines in in_dict.iteritems():
            path = os.path.join(self.tempdir, filename)
            with open(path, "w") as f:
                for line in lines:
                    f.write("%s\n" % line)

        # set source
        self.source = DatasourceDir(self.tempdir)


    def tearDown(self):
        shutil.rmtree(self.tempdir)


    def test_datasourcedir(self):
        input = self.source

        for key in input:
            dkey = key[-1]

            with input.load(key) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self.assert_(line in in_dict[dkey])


    def test_lineslicer(self):
        source = self.source

        for send_as_input in (True, False):

            expected_seq = []

            for xs in in_dict.itervalues():
                for x in xs:
                    expected_seq.append(x)

            slicer = LineFileSlicer()
            slicer.input = source
            slicer.send_as_input = send_as_input

            for key in slicer:
                if not send_as_input:
                    self.assertEqual(len(key), 2)
                val = slicer.load(key)
                expected = expected_seq.pop(0)
                self.assertEqual(val, expected,
                        "failed on key %s: %s == %s" % (str(key), str(val), str(expected)) )

