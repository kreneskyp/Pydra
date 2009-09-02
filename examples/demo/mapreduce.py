from pydra_server.cluster.tasks.tasks import Task

from pydra_server.cluster.tasks.mapreduce import MapReduceTask, \
        IntermediateResultsFiles, IntermediateResultsSQL

from pydra_server.cluster.tasks.datasource import DatasourceDict, \
        DatasourceDir, DatasourceSQL, SQLTableSlicer

import logging
logger = logging.getLogger('root')


class MapWords(Task):

    def work(self, input, output, **kwargs):
        """map for every input item output (word, 1) pair"""

        #id, data = input
        #input = data.split(" ")

        for word in input:
            # emmit (word, 1)
            output[word.strip()] = 1


class ReduceWords(Task):

    def work(self, input, output, **kwargs):
        """sum occurances of each word"""

        d = {}

        # WARNING the same key can appear more than once
        for word, v in input:
            try:
                v = sum(v)
            except:
                pass

            d[word] = int(v) + d.get(word, 0)

        for word, num in d.iteritems():
            # emmit output (word, num)
            output[word] = num


class CountWords(MapReduceTask):

    datasources = \
            {
            #'dict': DatasourceDict(
            #    { 
            #        "k1": ['one', 'two', 'four', 'two', 'four', 'seven'],
            #        "k2": ['seven', 'four', 'seven', 'seven'],
            #        "k3": ['seven', 'four', 'seven', 'seven'],
            #    }),
            #'sql': DatasourceSQL(user='pydra', passwd='pydra',
            #    host='192.168.56.1', db='mapreduce'),
            'dir': DatasourceDir(dir='/var/lib/pydra/mapreduce/in'),
            'dir_i9e': DatasourceDir(dir='/var/lib/pydra/mapreduce/i9e'),
            }

    #input = datasources['dict']

    input = datasources['dir']

    #input = SQLTableSlicer(table='count_words_in')
    #input.input = datasources['sql']

    output = {}

    map = MapWords
    reduce = ReduceWords

    intermediate = IntermediateResultsFiles(dir=datasources['dir_i9e'])
    #intermediate = IntermediateResultsSQL(table='count_words_i9e', db=datasources['sql'])

    reducers = 2
    #sequential = True

    description = 'Simple Map-Reduce Task to count the words in a input'

    def __init__(self, msg='countwords'):
        MapReduceTask.__init__(self, msg)

