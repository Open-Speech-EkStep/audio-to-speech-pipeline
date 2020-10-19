class Map:
    def __init__(self, embeddings, file_paths):
        self.list_em = [list(i) for i in embeddings]
        self.file_paths = file_paths

    def find_index(self, cluster):
        cluster_indices = [self.list_em.index(list(embed)) for embed in cluster]
        return cluster_indices

    def find_file(self, row):
        files = [self.file_paths[ind] for ind in row]
        return files
