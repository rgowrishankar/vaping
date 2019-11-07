from __future__ import absolute_import
from datetime import datetime

import vaping
import vaping.config
try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None

@vaping.plugin.register('elastic')
class ElasticPlugin(vaping.plugins.EmitBase):

    """
    Elastic plugin that allows vaping to persist data
    in an elastic database
    """

    def __init__(self, config, ctx):
        super(ElasticPlugin, self).__init__(config, ctx)
        self.field = self.config.get("field")
        self.elastic_index = self.config.get("elastic_index","default_vaping_index")
        self.log.debug("field %s index %s", self.field, self.elastic_index)

    def init(self):
        if not Elasticsearch:
            raise ImportError("Elastic python library not found")
        self.log.debug("Elastic plugin init called, IP %s", self.config.get("elastic_ip", "0.0.0.0"))
        if 'elastic_ip' not in self.config:
            msg = "IP address of the Elastic cluster should be specified"
            self.log.critical(msg)
            raise ValueError(msg)
            self.elastic_ip = None
        else:
            self.elastic_ip = self.config.get("elastic_ip")
        self.id = 1

    def on_start(self):
        # right now it is just stub code
        self.elastic = None
        self.log.debug("Elastic plugin start called")
        if self.elastic_ip is not None:
            try:
                # custom host with sniffing enabled
                self.elastic = Elasticsearch(
                          [self.elastic_ip],
                sniff_on_start=True,
                sniff_on_connection_fail=True,
                sniffer_timeout=10,
                max_retries=5
                )
            except Exception as error:
                msg = "ElasticSearch Client Error:" + str(error)
                self.log.critical(msg)
        else:
            self.elastic = None

    def on_stop(self):
        if self.elastic:
            self.elastic.transport.close()

    def emit(self, message):
        if isinstance(message.get("data"), list) and self.elastic:
            for row in message.get("data"):
                doc = {
                        'timestamp': message['ts'],
                        'data': row.get(self.field),
                        'source': row.get('host')
                      }
                res = self.elastic.index(index=self.elastic_index, id=self.id, body=doc)
                self.log.debug(res['result'])
                self.id = self.id + 1
