from audio_analysis.speaker_analysis.create_file_mappings import Map
from audio_analysis.speaker_analysis.clustering import Clustering
from audio_analysis.speaker_analysis.merging import Merge
from audio_analysis.speaker_analysis.splitting import get_big_cluster_embeds

import numpy as np

# from clustering import Clustering
# from create_file_mappings import Map
# from merging import Merge
# from splitting import get_big_cluster_embeds


def create_speaker_clusters(embed_filename_map_path, source_name, min_cluster_size=4, partial_set_size=11112,
                            min_samples=1, fit_noise_on_similarity=0.80):
    # step:1 -> ENCODING AND SAVING : done by create_embeddings.py

    # step:2 -> CLUSTERING AND MAPPING FILES TO CLUSTERS
    embed_speaker_map = np.load(embed_filename_map_path)
    embeddings = embed_speaker_map['embeds']
    file_paths = embed_speaker_map['file_paths']

    clustering_obj = Clustering()
    mean_embeds, noise_embeds, all_cluster_embeds = clustering_obj.run_partial_set_clusterings(embeddings,
                                                                                               min_cluster_size,
                                                                                               partial_set_size,
                                                                                               min_samples)
    num_clusters = len(mean_embeds)

    print('Num clusters = {}'.format(num_clusters))
    if num_clusters >= 1:
        # step:2.2 -> APPLYING MERGING OVER SIMILAR CLUSTERS FROM PARTIAL SETS CLUSTERS

        merger = Merge()
        all_cluster_embeds_merged_initial, mean_embeds_merged_initial = merger.run_repetitive_merging(
            all_cluster_embeds,
            mean_embeds,
            start_similarity_allowed=0.96,
            end_similarity_allowed=0.94,
            merge_closest_only=True)

        num_clusters = len(mean_embeds_merged_initial)
        print('Num clusters after initial merging= {}'.format(num_clusters))

        # step:2.3 -> SPLITTING "BIG" CLUSTERS AND MERGING AGAIN
        flat_embeddings_big_clusters, big_clusters_indices = get_big_cluster_embeds(all_cluster_embeds_merged_initial)

        if big_clusters_indices:
            mean_embeds_big, noise_embeds_big, all_cluster_embeds_big = clustering_obj.run_partial_set_clusterings(
                embeddings=flat_embeddings_big_clusters,
                min_cluster_size=min_cluster_size,
                partial_set_size=partial_set_size,
                min_samples=min_samples,
                cluster_selection_method='leaf')

            merger_big_cl = Merge()
            big_cluster_embeds_merged, big_mean_embeds_merged = merger_big_cl.run_repetitive_merging(
                all_cluster_embeds_big,
                mean_embeds_big,
                start_similarity_allowed=0.96,
                end_similarity_allowed=0.94,
                merge_closest_only=True)

            print('Num clusters after merging big clusters = {}'.format(len(big_mean_embeds_merged)))

            # preparing new list of clusters (after adding split+merged clusters together) and updated final noise
            all_cluster_embeds_to_merge, mean_embeddings_to_merge, noise_embeds_final = merger_big_cl.get_final_clusters_and_noise(
                big_clusters_indices, all_cluster_embeds_merged_initial, mean_embeds_merged_initial,
                noise_embeds, big_cluster_embeds_merged, big_mean_embeds_merged, noise_embeds_big)
            print('Num clusters before final merging  = {}'.format(len(mean_embeddings_to_merge)))
            print('Num final noise points = {}'.format(len(noise_embeds_final)))

            # step:2.4 -> repetitive merging on the final clusters from step 2.3
            merger_final = Merge()
            all_cluster_embeds_merged, mean_embeds_merged = merger_final.run_repetitive_merging(
                all_cluster_embeds_to_merge,
                mean_embeddings_to_merge,
                start_similarity_allowed=0.96,
                end_similarity_allowed=0.94,
                merge_closest_only=True)
            print('Num clusters after final merging = {}'.format(len(mean_embeds_merged)))

            mean_embeds_merged_initial = mean_embeds_merged
            noise_embeds = noise_embeds_final
            all_cluster_embeds_merged_initial = all_cluster_embeds_merged

        # step:3 -> FIT NOISE
        all_cluster_embeds_after_noise_fit, mean_embeds_new, unallocated_noise_embeds = merger.fit_noise_points(
            mean_embeds_merged_initial,
            noise_embeds,
            all_cluster_embeds_merged_initial,
            max_sim_allowed=fit_noise_on_similarity)

        # step:4 -> SAVE FILE_NAMES TO CLUSTER MAPPINGS
        print('Creating mappings for files')
        map_obj = Map(embeddings, file_paths)
        indices = [map_obj.find_index(cluster) for cluster in all_cluster_embeds_after_noise_fit]
        files_in_clusters = [map_obj.find_file(row) for row in indices]
        file_map_dict = {source_name + '_sp_' + str(ind): j for ind, j in enumerate(files_in_clusters)}

        noise_file_map_dict = dict({})
        if len(unallocated_noise_embeds):
            print('Creating mapping for {} unallocated noise points'.format(len(unallocated_noise_embeds)))
            noise_indices = [map_obj.find_index(cluster) for cluster in [unallocated_noise_embeds]]
            noise_files = [map_obj.find_file(row) for row in noise_indices]
            noise_file_map_dict = {source_name + '_noise': j for ind, j in enumerate(noise_files)}

        return file_map_dict, noise_file_map_dict
    else:
        print('No clusters could be found!')


if __name__ == "__main__":
    file_map_dict, noise_file_map_dict = create_speaker_clusters(
        embed_filename_map_path='/Users/neerajchhimwal/Desktop/spill.npz',
        source_name='spill')
