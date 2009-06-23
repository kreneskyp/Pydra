from pydra_server.cluster.tasks.mapreduce import MapReduceTask, MapTask, ReduceTask, \
        IntermediateResultsFiles

import logging
logger = logging.getLogger('root')


class MapWords(MapTask):

    def _work(self, input, output, **kwargs):
        """map for every input item output (word, 1) pair"""

        for word in input:
            # emmit (word, 1)
            output[word.strip()] = 1


class ReduceWords(ReduceTask):

    def _work(self, input, output, **kwargs):
        """sum occurances or each word"""

        d = {}

        # WARNING the same key can appear more than once
        for word, v in input:
            d[word] = sum(v) + d.get(word, 0)

        for word, num in d.iteritems():
            # emmit output (word, num)
            output[word] = num


class CountWords(MapReduceTask):

    input = ( 
                ['one', 'two', 'four', 'two', 'four', 'seven'],
                ['seven', 'four', 'seven', 'seven'],
                ['seven', 'four', 'seven', 'seven'],
            )

    output = {} # XXX will not work

    map = MapWords
    reduce = ReduceWords

    intermediate = IntermediateResultsFiles
    intermediate_kwargs = {'dir': '/mnt/shared'}

    reducers = 2
    #sequential = True

    description = 'Simple Map-Reduce Task to count the words in a input'

    def __init__(self, msg='countwords'):
        MapReduceTask.__init__(self, msg)

