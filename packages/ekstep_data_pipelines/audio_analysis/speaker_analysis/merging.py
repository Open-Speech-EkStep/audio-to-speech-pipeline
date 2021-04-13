from copy import deepcopy
from sklearn.metrics.pairwise import cosine_distances
import numpy as np

backup = dict({})


class Merge:
    def __init__(self):
        global backup
        backup = dict({})

    def cosine_dis_wrt_index(self, ind_1, ind_2, mean_embeddings):
        """
        Takes indices of clusters and mean_embeddings list (of all clusters)
        :return: cosine distance between clusters present at indices (ind_1 and ind_2)
        """
        em_1 = mean_embeddings[ind_1].reshape(1, -1)
        em_2 = mean_embeddings[ind_2].reshape(1, -1)
        return cosine_distances(em_1, em_2)[0][0]

    def pairs_to_merge(
        self,
        all_cluster_embeds,  # W0613: Unused argument 'all_cluster_embeds' (unused-argument)
        mean_embeddings,
        similarity_allowed,
        merge_closest_only=False,
    ):
        distances = cosine_distances(mean_embeddings)
        mask = np.eye(distances.shape[0], distances.shape[1])
        distances = distances + mask

        possible_mergers = dict({})

        for index, row in enumerate(distances):
            closest_clusters_dist = [
                dist for ind, dist in enumerate(row) if dist <= 1 - similarity_allowed
            ]
            closest_clusters_index = [
                ind for ind, dist in enumerate(row) if dist <= 1 - similarity_allowed
            ]

            if closest_clusters_dist:
                possible_mergers[index] = [i for i in closest_clusters_index]

        final_mergers = dict({})
        list_of_indices_covered = []
        for key in list(possible_mergers.keys()):
            if key not in list_of_indices_covered:
                values = possible_mergers[key]
                for value in values:
                    new_vals = [
                        i
                        for i in possible_mergers[value]
                        if i not in values
                        if i != key
                    ]
                    values.extend(new_vals)
                list_of_indices_covered.extend(values + [key])

                if merge_closest_only:
                    cos_dis_wrt_main_cluster = [
                        self.cosine_dis_wrt_index(key, i, mean_embeddings)
                        for i in values
                    ]
                    distance_allowed = 1 - similarity_allowed
                    pos = [
                        i
                        for i, dis in enumerate(cos_dis_wrt_main_cluster)
                        if dis <= distance_allowed
                    ]
                    values = [val for ind, val in enumerate(values) if ind in pos]

                final_mergers[key] = values

        return final_mergers

    def mean_embedding_of_cluster(self, cluster_embeds):
        cluster_embeds = np.array(cluster_embeds)
        raw_embed = np.mean(cluster_embeds, axis=0)
        mean_embedding = raw_embed / np.linalg.norm(raw_embed, 2)
        return mean_embedding

    def get_clusters_after_merging(
        self, final_pairs_to_merge, all_cluster_embeds_to_merge
    ):
        final_mean_embeds = []
        all_cluster_embeds_to_merge_copy = deepcopy(all_cluster_embeds_to_merge)

        print(
            "Total clusters before merging: {}".format(len(all_cluster_embeds_to_merge))
        )
        for item in final_pairs_to_merge.items():
            main_cluster_index = item[0]
            clusters_to_add_indices = item[1]
            for ind in clusters_to_add_indices:
                all_cluster_embeds_to_merge_copy[main_cluster_index].extend(
                    all_cluster_embeds_to_merge[ind]
                )

        clusters_lost_indices = [
            item for sublist in list(final_pairs_to_merge.values()) for item in sublist
        ]
        final_all_clusters_embeds = [
            val
            for i, val in enumerate(all_cluster_embeds_to_merge_copy)
            if i not in clusters_lost_indices
        ]

        for cluster in final_all_clusters_embeds:
            final_mean_embeds.append(self.mean_embedding_of_cluster(cluster))

        return final_all_clusters_embeds, final_mean_embeds

    def run_repetitive_merging(
        self,
        all_cluster_embeds,
        mean_embeddings,
        start_similarity_allowed,
        end_similarity_allowed,
        merge_closest_only,
    ):
        global backup
        backup["all_cluster_embeds"] = all_cluster_embeds
        backup["mean_embeddings"] = mean_embeddings

        possible_mergers = self.pairs_to_merge(
            all_cluster_embeds,
            mean_embeddings,
            start_similarity_allowed,
            merge_closest_only,
        )

        if possible_mergers:
            all_embeds_merged, mean_embeds_merged = self.get_clusters_after_merging(
                possible_mergers, all_cluster_embeds
            )
            backup["all_cluster_embeds"] = all_embeds_merged
            backup["mean_embeddings"] = mean_embeds_merged

            self.run_repetitive_merging(
                all_embeds_merged,
                mean_embeds_merged,
                start_similarity_allowed,
                end_similarity_allowed,
                merge_closest_only,
            )

        elif start_similarity_allowed > end_similarity_allowed:
            start_similarity_allowed -= 0.01
            possible_mergers = self.pairs_to_merge(
                all_cluster_embeds,
                mean_embeddings,
                start_similarity_allowed,
                merge_closest_only,
            )
            if possible_mergers:
                all_embeds_merged, mean_embeds_merged = self.get_clusters_after_merging(
                    possible_mergers, all_cluster_embeds
                )

                backup["all_cluster_embeds"] = all_embeds_merged
                backup["mean_embeddings"] = mean_embeds_merged

                self.run_repetitive_merging(
                    all_embeds_merged,
                    mean_embeds_merged,
                    start_similarity_allowed,
                    end_similarity_allowed,
                    merge_closest_only,
                )

        return backup["all_cluster_embeds"], backup["mean_embeddings"]

    def get_final_clusters_and_noise(
        self,
        big_clusters_indices,
        all_cluster_embeds,
        mean_embeds,
        noise_embeds,
        big_cluster_embeds,
        big_mean_embeds,
        noise_embeds_big,
    ):
        # sanity check and final data prep

        all_cluster_embeds_minus_big = [
            em_list
            for i, em_list in enumerate(all_cluster_embeds)
            if i not in big_clusters_indices
        ]
        mean_embeddings_minus_big = [
            mean_em
            for i, mean_em in enumerate(mean_embeds)
            if i not in big_clusters_indices
        ]

        all_cluster_embeds_to_merge = []
        all_cluster_embeds_to_merge.extend(all_cluster_embeds_minus_big)
        all_cluster_embeds_to_merge.extend(big_cluster_embeds)

        mean_embeddings_to_merge = []
        mean_embeddings_to_merge.extend(mean_embeddings_minus_big)
        mean_embeddings_to_merge.extend(big_mean_embeds)

        # noise
        noise_embeds_final = []
        noise_embeds_final.extend(noise_embeds)
        noise_embeds_final.extend(noise_embeds_big)

        return all_cluster_embeds_to_merge, mean_embeddings_to_merge, noise_embeds_final

    def fit_noise_points(
        self, mean_embeds, noise_embeds, all_cluster_embeds, max_sim_allowed=0.80
    ):
        """
        1. Calculate cos dis for each noise point wrt all mean embeds
        2. select cluster whose cosine dis with noise is the least (or less than a set threshold)
        3. append the newly classified noise point embed into the corresponding cluster embeds

        Returns:
         - new clusters with noise points allocated
         - number of noise points that were allocated to clusters
        """
        mean_embeds_new = []
        max_distance_allowed = 1 - max_sim_allowed
        all_cluster_embeds_copy = deepcopy(all_cluster_embeds)

        was_noise_flag = [[0] * len(cluster) for cluster in all_cluster_embeds_copy]

        allocated_noise_point_index = []
        print(
            "Trying to fit {} noise points with cos_similarity={}".format(
                len(noise_embeds), max_sim_allowed
            )
        )
        # distances is a matrix of shape (num_noise_points, num_mean_embeds)
        # with cosine dist of each noise embed with all mean embeds present in
        # rows
        distances = cosine_distances(noise_embeds, mean_embeds)
        closest_cluster_index = np.argmin(distances, axis=1)
        closest_cluster_dist = np.min(distances, axis=1)

        # C1801: Do not use `len(SEQUENCE)` without comparison to
        # determine if a sequence is empty (len-as-condition)
        if len(closest_cluster_dist):
            for index, dist in enumerate(closest_cluster_dist):
                if dist <= max_distance_allowed:
                    all_cluster_embeds_copy[closest_cluster_index[index]].append(
                        noise_embeds[index]
                    )
                    was_noise_flag[closest_cluster_index[index]].append(1)
                    allocated_noise_point_index.append(index)

            # embeddings for noise points that couldn't be allocated
            if allocated_noise_point_index:
                for cluster in all_cluster_embeds_copy:
                    new_em = self.mean_embedding_of_cluster(cluster)
                    mean_embeds_new.append(new_em)
                unallocated_noise_embeds = [
                    em
                    for ind, em in enumerate(noise_embeds)
                    if ind not in allocated_noise_point_index
                ]
            else:
                unallocated_noise_embeds = noise_embeds

        else:
            print("No noise points could be fit!")
            mean_embeds_new = mean_embeds
            unallocated_noise_embeds = noise_embeds
        return (
            all_cluster_embeds_copy,
            mean_embeds_new,
            unallocated_noise_embeds,
            was_noise_flag,
        )
