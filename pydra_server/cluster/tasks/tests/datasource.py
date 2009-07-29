import unittest

from pydra_server.cluster.tasks.datasource import *
import tempfile, shutil


class Datasource_Test(unittest.TestCase):

    def setUp(self):
        self.in_dict = { 
                    "k1": ['one', 'two', 'four', 'two', 'four', 'seven'],
                    "k2": ['seven', 'four', 'seven', 'seven'],
                    "k3": ['seven', 'four', 'seven', 'seven'],
                       }


    def test_datasourcedict(self):

        input = DatasourceDict(self.in_dict)

        for key in input:
            dkey = key[-1]
            self.assertEqual(self.in_dict[dkey], input.load(key))


    def test_sequenceslicer(self):

        source = DatasourceDict(self.in_dict)

        expected_seq = []

        for xs in self.in_dict.itervalues():
            for x in xs:
                expected_seq.append(x)

        slicer = SequenceSlicer()
        slicer.input = source

        for key in slicer:
            self.assertEqual(len(key), 2)
            self.assertEqual(slicer.load(key), expected_seq.pop(0))


    def test_nested_sequenceslicer(self):

        source = DatasourceDict(self.in_dict)

        expected_seq = []

        for xs in self.in_dict.itervalues():
            for str in xs:
                for char in str:
                    expected_seq.append(char)

        outer_slicer = SequenceSlicer()
        outer_slicer.input = source

        inner_slicer = SequenceSlicer()
        inner_slicer.input = outer_slicer

        for key in inner_slicer:
            self.assertEqual(len(key), 3)
            self.assertEqual(inner_slicer.load(key), expected_seq.pop(0))

