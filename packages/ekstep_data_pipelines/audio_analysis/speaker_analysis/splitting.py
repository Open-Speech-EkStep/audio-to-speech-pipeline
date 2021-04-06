import math

import numpy as np


def get_big_cluster_size_threshold(all_cluster_embeds):
    """
    Takes all_cluster_embeds
    Returns: threshold value defining big cluster size in the dataset
    (any cluster with size >= this threshold will be treated as a big cluster)
    """

    cl_sizes = []
    thresholds_wrt_multipliers = []
    for cluster in all_cluster_embeds:
        cl_sizes.append(len(cluster))
    mean_cl_size = np.mean(np.array(cl_sizes))
    for multiplier in range(5, 1, -1):
        if [size for size in cl_sizes if size >= mean_cl_size * multiplier]:
            thresholds_wrt_multipliers.append(mean_cl_size * multiplier)

    if len(thresholds_wrt_multipliers) > 2:
        return math.floor(thresholds_wrt_multipliers[1])
    elif len(thresholds_wrt_multipliers) == 1:
        return math.floor(thresholds_wrt_multipliers[0])
    else:
        return 0


def get_big_cluster_embeds(all_cluster_embeds):
    # defining big clusters
    threshold = get_big_cluster_size_threshold(all_cluster_embeds)
    flat_embeddings_big_clusters = []
    big_clusters_indices = []
    if threshold:
        # indices of big clusters
        big_clusters_indices = []
        print("Clusters larger than {} points have sizes:".format(threshold))
        for ind, cluster in enumerate(all_cluster_embeds):
            if len(cluster) >= threshold:
                print(len(cluster))
                big_clusters_indices.append(ind)

        # embeds of big clusters and their speakers
        big_cluster_embeds = [
            cl
            for ind, cl in enumerate(all_cluster_embeds)
            if ind in big_clusters_indices
        ]

        flat_embeddings_big_clusters = [
            item for sublist in big_cluster_embeds for item in sublist
        ]

        print("total points in big clusters: {}".format(
            len(flat_embeddings_big_clusters)))
        flat_embeddings_big_clusters = np.array(flat_embeddings_big_clusters)
    return flat_embeddings_big_clusters, big_clusters_indices
