import os

import numpy as np
import torch
import yaml

from audio_language_identification.utils import utils

# check cuda available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
torch.manual_seed(0)


def load_model(model_path):
    if os.path.isfile(model_path):
        model = torch.load(model_path, map_location=device)
        model.eval()
        print("Model loaded from ", model_path)
    else:
        print("Saved model not found")
        exit(1)
    return model


def forward(audio, model, mode='train'):
    try:
        model.eval()
        spec = utils.load_data(audio, mode=mode)[np.newaxis, ...]
        feats = np.asarray(spec)
        feats = torch.from_numpy(feats)
        feats = feats.unsqueeze(0)
        feats = feats.to(device)
        label = model(feats.float())
        return label
    except:
        print("File error ", audio)


def language_confidence_score_map(confidence_scores, language_map_path):
    output_dictionary = {}
    language_map = load_yaml_file(language_map_path)['languages']
    for key in language_map:
        output_dictionary[language_map[key]] = confidence_scores[key]
    return output_dictionary

def load_yaml_file(path):
    with open(path, 'r') as file:
        read_dict = yaml.safe_load(file)
    return read_dict


def evaluation(audio_path, model_path='audio_language_identification/model/model.pt'):
    model = load_model(model_path)
    model_output = forward(audio_path, model=model)
    sm = torch.nn.Softmax()
    probabilities = sm(model_output)
    confidence_scores = ["{:.5f}".format(i.item()) for i in list(probabilities[0])]
    return list(map(lambda cs: float(cs), confidence_scores))

def infer_language(audio_path, language_map_path='audio_language_identification/language_map.yml'):
    return language_confidence_score_map(evaluation(audio_path), language_map_path)

