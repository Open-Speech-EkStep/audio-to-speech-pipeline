from audio_speaker_clustering.create_file_mappings import Map
from audio_speaker_clustering.clustering import Clustering
import numpy as np


def create_speaker_clusters(embed_filename_map_path, source_name):
    # step:1 -> ENCODING AND SAVING : done by create_embeddings.py

    # step:2 -> CLUSTERING AND MAPPING FILES TO CLUSTERS
    embed_speaker_map = np.load(embed_filename_map_path)
    embeddings = embed_speaker_map['embeds']
    file_paths = embed_speaker_map['file_paths']

    clustering_obj = Clustering()
    mean_embeds, noise_embeds, all_cluster_embeds = clustering_obj.run_partial_set_clusterings(embeddings=embeddings,
                                                                                               min_cluster_size=2,
                                                                                               partial_set_size=11122,
                                                                                               min_samples=1)
    num_clusters = len(mean_embeds)

    print('Num clusters = {}'.format(num_clusters))

    cluster_embeds, unallocated_noise_embeds = clustering_obj.fit_noise_points(mean_embeds, noise_embeds,
                                                                               all_cluster_embeds)

    # step:3 -> SAVE FILE_NAMES TO CLUSTER MAPPINGS
    print('Creating mappings for files:')
    map_obj = Map(embeddings, file_paths)
    indices = [map_obj.find_index(cluster) for cluster in all_cluster_embeds]
    files_in_clusters = [map_obj.find_file(row) for row in indices]
    file_map_dict = {source_name + '_sp_' + str(ind): j for ind, j in enumerate(files_in_clusters)}
    noise_file_map_dict = {}
    if unallocated_noise_embeds:
        print('Creating mapping for noise points')
        noise_indices = [map_obj.find_index(cluster) for cluster in [unallocated_noise_embeds]]
        noise_files = [map_obj.find_file(row) for row in noise_indices]
        noise_file_map_dict = {source_name + '_noise': j for ind, j in enumerate(noise_files)}

    return file_map_dict, noise_file_map_dict


if __name__ == "__main__":
    file_map_dict, noise_file_map_dict = create_speaker_clusters(
        embed_filename_map_path='/home/anirudh/sdb/speaker_clustering/ldcil_embed_map.npz',
        source_name='ldcil')
