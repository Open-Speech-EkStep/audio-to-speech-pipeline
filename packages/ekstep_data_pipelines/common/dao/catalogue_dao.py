class CatalogueDao:

    def __init__(self, postgres_client):
        self.data_processor = postgres_client

    def get_utterances(self, audio_id):
        params = {'audio_id', audio_id}
        utterances = self.data_processor.execute_query("sql", params)
        return utterances
