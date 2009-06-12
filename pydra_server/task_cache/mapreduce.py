from pydra_server.cluster.tasks.mapreduce import MapReduceTask, IntermediateResultsFiles

import logging
logger = logging.getLogger('root')

class CountWords(MapReduceTask):

    input = ( 
                ['one', 'two', 'three', 'two', 'three', 'four'],
                ['four', 'three', 'four', 'four'],
                ['four', 'three', 'four', 'four'],
            )

    output = {} # XXX will not work

    intermediate = IntermediateResultsFiles
    intermediate_kwargs = {'dir': '/mnt/shared'}

    reducers = 2
    #sequential = True

    description = 'Simple Map-Reduce Task to count the words in a input'

    def __init__(self, msg='countwords'):
        MapReduceTask.__init__(self, msg)


    def map(self, input, output, **kwargs):
        """map for every input item output (word, 1) pair"""

        for word in input:
            # emmit (word, 1)
            output[word.strip()] = 1


    def reduce(self, input, output, **kwargs):
        """sum occurances or each word"""

        d = {}

        # WARNING the same key can appear more than once
        for word, v in input:
            d[word] = sum(v) + d.get(word, 0)

        for word, num in d.iteritems():
            # emmit output (word, num)
            output[word] = num

