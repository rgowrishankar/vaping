from __future__ import absolute_import
from datetime import datetime

import vaping
import vaping.config
try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None

@vaping.plugin.register('elastic')
class ElasticPlugin(vaping.plugins.TimeSeriesDB):

    """
    Elastic plugin that allows vaping to persist data
    in an elastic database
    """

    def __init__(self, config, ctx):
        self.config = config
        if not Elasticsearch:
            raise ImportError("Elastic python library not found")
        super(ElasticPlugin, self).__init__(config, ctx)
        self.log.debug("Elastic plugin init called, IP %s", self.config.get("IP", "0.0.0.0"))
        self.es = Elasticsearch([self.config.get("IP", "0.0.0.0")])
        self.id = 1

    def start(self):
        # right now it is just stub code
        self.log.debug("Elastic plugin start called")

    def create(self, filename):
        # since connection to elastic is opened in __init this is dummy call as well
        self.log.debug("Elastic plugin create called filename %s", filename)

    def update(self, filename, time, value):
        self.log.debug("elastic plugin update called filename %s time %s value %s",
                       filename, time, value)
        filename_split = filename.split("-")
        ip = filename_split[1]
        data_type = filename_split[2]
        if data_type == "loss":
            doc = {
                    'timestamp':datetime.now(),
                    'loss': value,
                    'ip': ip
                  }
            res = self.es.index(index="loss_index", id=self.id, body=doc)
            self.log.debug(res['result'])
        else:        
            doc = {
                    'timestamp':datetime.now(),
                    'latency': value,
                    'ip': ip
                  }
            res = self.es.index(index="latency_index", id=self.id, body=doc)
            self.log.debug(res['result'])
        self.id=self.id + 1

    def get(self, filename, from_time, to_time=None):
        #stubbed out for now
        self.log.debug("Elastic get called")
        return None, None
