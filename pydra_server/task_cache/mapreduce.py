from pydra_server.cluster.tasks.mapreduce import MapReduceTask, IntermediateResultsFiles

import logging
logger = logging.getLogger('root')

class CountWords(MapReduceTask):

    input = {
            'list_one': ['one', 'two', 'three', 'two', 'three', 'four'],
            'list_two': ['four', 'three', 'four', 'four'],
            }.itervalues()

    output = {} # XXX will not work

    intermediate = IntermediateResultsFiles
    intermediate_kwargs = {'dir': '/mnt/shared'}

    reducers = 2
    sequential = False

    description = 'Simple Map-Reduce Task to count the words in a input'

    def __init__(self, msg='countwords'):
        MapReduceTask.__init__(self, msg)

    def map(self, input, output, **kwargs):
        """map for every input item output (word, 1) pair"""
        logger.debug('      map started')

        for word in input:
            # emmit (word, 1)
            output[word.strip()] = 1

        logger.debug('      map finished')

    def reduce(self, input, output, **kwargs):
        """sum occurances or each word"""
        logger.debug('      reduce started')

        d = {}

        # WARNING the same key can appear more than once
        for word, v in input:
            d[word] = sum(v) + d.get(word, 0)
            logger.info('word: %s   next: %d times' % (word , sum(v)))

        for word, num in d.iteritems():
            # emmit output (word, num)
            output[word] = num

        logger.debug('      reduce finished')

