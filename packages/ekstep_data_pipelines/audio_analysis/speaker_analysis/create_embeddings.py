from resemblyzer import preprocess_wav, VoiceEncoder
from tqdm import tqdm
import glob
from joblib import Parallel, delayed
import numpy as np


def audio_paths(directory, pattern):
    print('Using dir {}'.format(directory + pattern))
    return glob.glob(directory + pattern)


def encoder(source_dir, source_dir_pattern, embed_file_name):
    file_paths = audio_paths(source_dir, source_dir_pattern)
    print('Number of files: {}'.format(len(file_paths)))

    processed_wavs = [preprocess_wav(i) for i in tqdm(file_paths)]
    vocoder = VoiceEncoder()
    encodings = [vocoder.embed_utterance(i) for i in tqdm(processed_wavs)]
    print('Creating embeddings')
    encodings = np.array(encodings)
    np.savez_compressed(embed_file_name, embeds=encodings, file_paths=file_paths)
    print('Encodings mapped to filepaths have been saved at {}'.format(embed_file_name))


if __name__ == "__main__":
    encoder(source_dir='/home/anirudh/sdb/audio_speaker_clustering/processed_ldcil_data/',
            source_dir_pattern='*/*.wav',
            embed_file_name='/home/anirudh/sdb/speaker_clustering/ldcil_embed_map.npz')
