import json
import os
import tarfile

import boto3
import certifi
import jsonlines
from elasticsearch import Elasticsearch

from ksuid import ksuid


class ElasticScroll:

    es = None
    s3 = None

    # Define config
    timeout = 1000

    def __init__(self):

        with open('config.json') as f:
            data = json.load(f)

        self.host = data["host"]
        self.port = data["port"]
        self.index = data["index"]
        self.query = data["query"]
        self.sort = data["sort"]
        self.size = data["size"]
        self.compress = data["compress"]
        if "s3.bucket" in data:
            self.s3_bucket = data["s3.bucket"]

        self.es = Elasticsearch([{'host': self.host, 'port': self.port}], timeout=self.timeout,
                                scheme="https", verify_certs=True, send_get_body_as='POST', ca_certs=certifi.where())
        if not self.es.indices.exists(index=self.index):
            print("nonexistent index")
            exit()
        print("es init")

        if "s3.bucket" in data:
            self.s3 = boto3.resource('s3')
            print("s3 init")

    # Elastic scroll request looks like this:
    # GET /_search?scroll=1m
    # {
    #   "size":1000,
    #   "query": {
    #     "match_all": {}
    #   },
    #   "sort": [
    #     "_doc"
    #   ]
    # }
    def process(self, context, input):
        # Initialize the scroll
        # See elasticsearch.helpers.scan(client, query=None, scroll=u'5m', raise_on_error=True, preserve_order=False, size=1000, request_timeout=None, clear_scroll=True, scroll_kwargs=None, **kwargs)
        page = self.es.search(
            index=self.index,
            doc_type='web',
            scroll='2m',
            size=self.size,
            body={
                "query": self.query,
                "sort": self.sort
            }
        )

        # Get the scroll ID
        scroll_id = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])

        # Before scroll, process current batch of hits
        self.print_hits(page['hits']['hits'])
        self.export_hits(page['hits']['hits'])

        while scroll_size > 0:
            page = self.es.scroll(scroll_id=scroll_id, scroll='2m')

            # Update the scroll id and page size
            scroll_id = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

            if scroll_size == 0:
                break

            self.print_hits(page['hits']['hits'])
            self.export_hits(page['hits']['hits'])

            print("Scrolling..." + str(scroll_size))

        return

    # Export hits to disk
    def export_hits(self, hits):
        output_file_name = ksuid().toBase62()
        with jsonlines.open(output_file_name, mode='w') as writer:
            for item in hits:
                # no indent or pretty print - jsonlines / ndjson format
                writer.write(item)

        if self.s3:

            if self.compress:
                non_compress_file = output_file_name
                output_file_name = output_file_name+".tar.gz"
                outTar = tarfile.open(output_file_name, mode='w:gz')
                try:
                    outTar.add(non_compress_file)
                finally:
                    outTar.close()
                os.remove(non_compress_file)

            self.s3.meta.client.upload_file(
                output_file_name, self.s3_bucket, output_file_name)

            os.remove(output_file_name)

    # Print hits to console
    def print_hits(self, hits):
        for item in hits:
            # no indent (use indent=2 for pretty print)
            print(json.dumps(item))

    def copy_hits(self, hits, target_index):
        result = self.es.bulk(hits, "newindex")
        print(json.dumps(result))  # no indent (use indent=2 for pretty print)


def main():
    es = ElasticScroll()
    es.process('localhost', 9200)


if __name__ == "__main__":
    main()
