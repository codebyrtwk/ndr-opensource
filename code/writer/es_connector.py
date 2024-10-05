from elasticsearch import Elasticsearch, ElasticsearchException

class ESConnector:
    def __init__(self, hosts=None, username=None, password=None, use_ssl=False, verify_certs=True):
        """
        Initialize the Elasticsearch connector.

        :param hosts: List of Elasticsearch hosts (e.g., ["http://localhost:9200"])
        :param username: Username for basic authentication (optional)
        :param password: Password for basic authentication (optional)
        :param use_ssl: Boolean to use SSL (optional)
        :param verify_certs: Boolean to verify SSL certificates (optional)
        """
        self.hosts = hosts or ["http://localhost:9200"]
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.verify_certs = verify_certs

        # Initialize the Elasticsearch client
        self.client = self._connect()

    def _connect(self):
        """
        Connect to the Elasticsearch cluster.

        :return: Elasticsearch client object
        """
        try:
            if self.username and self.password:
                es_client = Elasticsearch(
                    self.hosts,
                    http_auth=(self.username, self.password),
                    use_ssl=self.use_ssl,
                    verify_certs=self.verify_certs
                )
            else:
                es_client = Elasticsearch(self.hosts)

            # Test the connection
            if not es_client.ping():
                raise ElasticsearchException("Unable to connect to Elasticsearch cluster")

            print("Successfully connected to Elasticsearch")
            return es_client

        except ElasticsearchException as e:
            print(f"Error connecting to Elasticsearch: {e}")
            raise

    def create_index(self, index_name, body=None):
        """
        Create an index in Elasticsearch.

        :param index_name: Name of the index
        :param body: Optional mapping/settings for the index
        :return: Response from Elasticsearch
        """
        try:
            if not self.client.indices.exists(index=index_name):
                response = self.client.indices.create(index=index_name, body=body)
                print(f"Index '{index_name}' created successfully.")
                return response
            else:
                print(f"Index '{index_name}' already exists.")
        except ElasticsearchException as e:
            print(f"Error creating index '{index_name}': {e}")
            raise

    def index_document(self, index_name, doc_id, document):
        """
        Index a document into a specific index.

        :param index_name: Name of the index
        :param doc_id: ID of the document
        :param document: The document data as a dictionary
        :return: Response from Elasticsearch
        """
        try:
            response = self.client.index(index=index_name, id=doc_id, document=document)
            print(f"Document indexed successfully with ID: {doc_id}")
            return response
        except ElasticsearchException as e:
            print(f"Error indexing document ID '{doc_id}': {e}")
            raise

    def search(self, index_name, query):
        """
        Search documents in a specific index.

        :param index_name: Name of the index
        :param query: The search query as a dictionary
        :return: Search results
        """
        try:
            response = self.client.search(index=index_name, body=query)
            print("Search executed successfully.")
            return response
        except ElasticsearchException as e:
            print(f"Error searching index '{index_name}': {e}")
            raise

    def delete_index(self, index_name):
        """
        Delete an index from Elasticsearch.

        :param index_name: Name of the index to delete
        :return: Response from Elasticsearch
        """
        try:
            if self.client.indices.exists(index=index_name):
                response = self.client.indices.delete(index=index_name)
                print(f"Index '{index_name}' deleted successfully.")
                return response
            else:
                print(f"Index '{index_name}' does not exist.")
        except ElasticsearchException as e:
            print(f"Error deleting index '{index_name}': {e}")
            raise