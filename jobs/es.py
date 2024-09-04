import os

class Es(object):
    def __init__(self, es_hosts, mode="append", write_operation="index"):
        self.es_hosts = os.environ.get('ES_HOSTS', es_hosts)
        self.es_mode = mode
        self.es_write_operation = write_operation
        self.es_index_auto_create = "true"

    def write_df(self, df, es_resource):
        try:
            df.write.format("org.elasticsearch.spark.sql") \
                .mode(self.es_mode) \
                .option("es.nodes", self.es_hosts) \
                .option("es.resource", es_resource) \
                .option("es.nodes.wan.only", "true") \
                .option("es.index.auto.create", self.es_index_auto_create) \
                .option("es.mapping.date.rich", "false") \
                .option("es.spark.dataframe.write.null", "true") \
                .option("es.write.operation", self.es_write_operation) \
                .save()
            print(f"Successfully wrote data to Elasticsearch index: {es_resource}")
        except Exception as e:
            print(f"Error writing to Elasticsearch: {str(e)}")
            raise