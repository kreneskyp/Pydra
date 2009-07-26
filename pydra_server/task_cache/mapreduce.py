from pydra_server.cluster.tasks.tasks import Task
from pydra_server.cluster.tasks.mapreduce import MapReduceTask, \
        IntermediateResultsFiles, DatasourceDict

import logging
logger = logging.getLogger('root')


class MapWords(Task):

    def work(self, input, output, **kwargs):
        """map for every input item output (word, 1) pair"""

        for word in input:
            # emmit (word, 1)
            output[word.strip()] = 1


class ReduceWords(Task):

    def work(self, input, output, **kwargs):
        """sum occurances of each word"""

        d = {}

        # WARNING the same key can appear more than once
        for word, v in input:
            d[word] = sum(v) + d.get(word, 0)

        for word, num in d.iteritems():
            # emmit output (word, num)
            output[word] = num


class CountWords(MapReduceTask):

    input = DatasourceDict(
                { 
                    "1": ['one', 'two', 'four', 'two', 'four', 'seven'],
                    "2": ['seven', 'four', 'seven', 'seven'],
                    "3": ['seven', 'four', 'seven', 'seven'],
                })

    output = {}

    map = MapWords
    reduce = ReduceWords

    intermediate = IntermediateResultsFiles
    intermediate_kwargs = {'dir': '/mnt/shared'}

    reducers = 2
    #sequential = True

    description = 'Simple Map-Reduce Task to count the words in a input'

    def __init__(self, msg='countwords'):
        MapReduceTask.__init__(self, msg)

