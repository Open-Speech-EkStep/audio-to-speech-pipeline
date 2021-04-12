from tqdm import tqdm
import glob
# from joblib import Parallel, delayed
import numpy as np
import math
import os


def audio_paths(directory, pattern):
    print('Using dir {}'.format(directory + pattern))
    return glob.glob(directory + pattern, recursive=True)


def save_embeddings(embed_file_path, embeddings, file_paths):
    np.savez_compressed(embed_file_path, embeds=embeddings, file_paths=file_paths)
    print('Embeddings mapped to filepaths have been saved at {}'.format(embed_file_path))
    return


def concatenate_embed_files(embed_file_dest,local_npz_folder_path):
    pattern = '.npz'
    pattern_prefix = f'{local_npz_folder_path}*'
    print(pattern_prefix)
    npz_files_to_concat = glob.glob(pattern_prefix, recursive=True)
    if npz_files_to_concat:
        print(npz_files_to_concat)
        list_of_loaded_files = []
        for file in npz_files_to_concat:
            if pattern in file:
                list_of_loaded_files.append(np.load(file))

        final_embeds = np.concatenate([file['embeds'] for file in list_of_loaded_files])
        final_file_paths = np.concatenate([file['file_paths'] for file in list_of_loaded_files])
        print(f'Final length of concatenated embeds', len(final_embeds))
        save_embeddings(embed_file_dest, embeddings=final_embeds,
                        file_paths=final_file_paths)


