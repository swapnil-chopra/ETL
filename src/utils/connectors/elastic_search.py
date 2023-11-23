from elasticsearch import Elasticsearch as connector


class ElasticSearch():
    def __init__(self, endpoint, auth_type: str, **kwargs) -> None:
        self.endpoint = endpoint
        self.auth_type = auth_type
        self.connect()
        pass

    def connect(self) -> None:
        self.client = connector(self.endpoint, basic_auth=(
            'elastic', 'ybVs1KtWngfwdtW7r=LW'), verify_certs=False)
        print(self.client)

    def insert(self, data: dict, index: str, doc_id: str):
        if not self.client.indices.exists(index=index):
            self.client.indices.create(index=index)
        self.client.index(
            index=index,
            id=doc_id,
            document=data
        )
        # self.client.indices.create(index=index)
        pass

    def search_by_id(self, index, doc_id):
        print(self.client.get(index=index, id=doc_id))

    def search_by_query(self, index, query):
        print(self.client.search(index=index, query={'match_all': {}}))
        pass

    def update_doc(self, index, doc_id):
        pass
# ElasticSearch('https://192.168.56.101:9200', '').insert(
#     {'test_data': 'Hei, Swapnil\'s second elasticsearch doc'}, 'demo', '2')


ElasticSearch('https://192.168.56.101:9200', '').search_by_id('demo', '2')
ElasticSearch('https://192.168.56.101:9200', '').search_by_query('demo', '2')
