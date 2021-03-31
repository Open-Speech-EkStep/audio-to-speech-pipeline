import argparse
import os
import time

import joblib
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from resemblyzer import VoiceEncoder, preprocess_wav
from tqdm import tqdm


def get_parser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--model-path",
        default=None,
        type=str,
        help="path to model")

    parser.add_argument(
        "--csv-path",
        default=None,
        type=str,
        help="csv file path containig file paths of audios",
    )

    parser.add_argument(
        "--file-mode",
        default=False,
        type=bool,
        help="True to see results for single audio file",
    )

    parser.add_argument(
        "--npz-file-path",
        default=None,
        type=str,
        help="True to see results for single audio file",
    )

    parser.add_argument(
        "--file-path", default=None, type=str, help="file path of audio"
    )

    parser.add_argument(
        "--save-dir",
        default="./",
        type=str,
        help="location to save prediction file")

    return parser


def load_model(model_path):
    return joblib.load(model_path)


def get_embed(voice_enc, file):
    return np.asarray(
        voice_enc.embed_utterance(
            preprocess_wav(file))).reshape(
        1, -1)


def get_prediction(voice_enc, model, file):
    if os.path.exists(file):
        X = get_embed(voice_enc, file)
        return model.predict(X)[0]
    else:
        raise Exception(f"File path does not exist {file}")


def get_prediction_for_embed(model, embed):
    return model.predict(embed)[0]


def get_prediction_csv_mode(voice_enc, model, csv_path, save_dir):
    df = pd.read_csv(csv_path, header=None, names=["file_paths"])
    df["predicted_gender"] = Parallel(n_jobs=-1)(
        delayed(get_prediction)(voice_enc, model, file_path)
        for file_path in tqdm(df["file_paths"].values)
    )
    df.to_csv(
        os.path.join(
            save_dir,
            "predictions.csv"),
        header=False,
        index=False)
    print(f"Inference Completed")


def get_prediction_from_npz_file(model, npz_file_path):
    """

    :param model: (str) model file with extension .sav, stored in ../model/clf_svc.sav
    :param npz_file_path: (str) path to binary .npz file containing embeddings and file_paths
    :return: (dict) : key-> file_path, value->predicted gender label ('m' or 'f')
    """
    npz_file = np.load(npz_file_path)
    embeds = npz_file["embeds"]
    file_paths = npz_file["file_paths"]
    predicted_gender = Parallel(n_jobs=-1)(
        delayed(get_prediction_for_embed)(model, embed.reshape(1, -1))
        for embed in tqdm(embeds)
    )
    predicted_gender = list(
        map(lambda x: "m" if x == 0 else "f", predicted_gender))

    if len(predicted_gender) == len(file_paths):
        file_vs_gender_dict = dict(zip(file_paths, predicted_gender))
    else:
        raise Exception("len(predicted_genders) != len(embeddings)")
    return file_vs_gender_dict


def main(args):
    if os.path.exists(args.model_path):
        model = load_model(args.model_path)
        voice_enc = VoiceEncoder()
        if args.file_mode:
            print(
                f"Predicted gender : {get_prediction(voice_enc, model, args.file_path)}"
            )

        if args.npz_file_path:
            npz_file_path = args.npz_file_path
            save_dir = args.save_dir
            get_prediction_from_npz_file(model, npz_file_path, save_dir)

        else:
            csv_path = args.csv_path
            save_dir = args.save_dir
            get_prediction_csv_mode(voice_enc, model, csv_path, save_dir)


if __name__ == "__main__":
    s = time.time()
    parser = get_parser()
    args = parser.parse_args()
    main(args)
    print(f"Time taken {time.time() - s} seconds")
