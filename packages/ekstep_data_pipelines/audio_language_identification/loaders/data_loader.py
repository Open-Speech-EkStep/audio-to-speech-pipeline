import numpy as np

from ekstep_data_pipelines.audio_language_identification.utils import utils


class SpeechDataGenerator:
    def __init__(self, manifest, mode):
        self.mode = mode
        self.audio_links = [line.rstrip("\n").split(
            ",")[0] for line in open(manifest)]
        self.labels = [int(line.rstrip("\n").split(",")[1])
                       for line in open(manifest)]

    def __len__(self):
        return len(self.audio_links)

    def __getitem__(self, idx):
        audio_link = self.audio_links[idx]
        class_id = self.labels[idx]
        spec = utils.load_data(audio_link, mode=self.mode)[np.newaxis, ...]
        feats = np.asarray(spec)
        label_arr = np.asarray(class_id)

        return feats, label_arr
