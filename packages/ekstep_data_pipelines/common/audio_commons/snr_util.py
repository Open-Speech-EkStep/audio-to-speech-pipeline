import json
import os
import shutil
import subprocess

import pandas as pd
from ekstep_data_pipelines.audio_language_identification.audio_language_inference import (
    infer_language, )
from ekstep_data_pipelines.audio_processing.audio_duration import calculate_duration
from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("Snr")


class SNR:
    """
    Util object for performing SNR analysis over different
    """

    MAX_DURATION = 15

    @staticmethod
    def get_instance(initialization_dict):
        feat_language_identification = initialization_dict.get(
            "audio_processor_config", {}
        ).get("feat_language_identification", False)
        LOGGER.info(
            "Running with feat_language_identification=%s",
            str(feat_language_identification)
        )
        curr_instance = SNR(feat_language_identification)
        return curr_instance

    def __init__(self, feat_language_identification=False):
        self.feat_language_identification = feat_language_identification
        self.current_working_dir = os.getcwd()

    def get_command(self, current_working_dir, file_path):
        return f'"{current_working_dir}/ekstep_data_pipelines/binaries/WadaSNR/Exe/WADASNR" -i ' \
               f'"{file_path}" -t "{current_working_dir}' \
               f'/ekstep_data_pipelines/binaries/WadaSNR/Exe/Alpha0.400000.txt" -ifmt mswav'

    def get_output_directories(self, output_dir, ensure_path=True):
        clean_path, rejected_path = f"{output_dir}/clean", f"{output_dir}/rejected"

        if ensure_path:
            LOGGER.info(
                "ensure_path flag is %s, ensuring that the directories exist",
                ensure_path
            )
            if not os.path.exists(clean_path):
                LOGGER.info("%s does not exist, creating it", clean_path)
                os.makedirs(clean_path)

            if not os.path.exists(rejected_path):
                LOGGER.info("%s does not exist, creating it", rejected_path)
                os.makedirs(rejected_path)

        return clean_path, rejected_path

    def move_file_locally(self, source, destination):
        shutil.move(source, destination)

    def compute_file_snr(self, file_path):
        """
        Convert given file to required format with FFMPEG and process with WADA.
        """
        LOGGER.info("Measuring SNR for file at ", file_path)
        command = self.get_command(self.current_working_dir, file_path)

        LOGGER.info("Command to be run %s", command)

        try:
            process_output = subprocess.check_output(command, shell=True)
            LOGGER.info("process_output:%s", str(process_output))
        except subprocess.CalledProcessError as error:
            LOGGER.error("Called process error:%s", str(error))
            return float(-1)

        return float(process_output.split()[-3].decode("utf-8"))

    def process_files_list(self, input_file_list):

        LOGGER.info(
            "Processing all the file in the directory %s", input_file_list)

        file_snrs = {}

        for file_path in input_file_list:

            snr_value = self.compute_file_snr(file_path)

            if str(snr_value) == "nan":
                snr_value = 0.0

            file_snrs[file_path] = snr_value

            LOGGER.info("%s has an snr value of %s", file_path, snr_value)

        return file_snrs

    def fit_and_move(
            self,
            input_file_list,
            metadata_file_name,
            threshold,
            output_dir_path,
            audio_id,
            hash_code,
    ):
        LOGGER.info("Processing SNR for for the files %s", input_file_list)
        processed_file_snr_dict = self.process_files_list(input_file_list)

        LOGGER.info("Getting the clean and reject folders")
        clean_dir_path, rejected_dir_path = self.get_output_directories(
            output_dir_path)
        LOGGER.info(
            "Got the clean and reject folders, clean/%s and rejected/%s",
            clean_dir_path, rejected_dir_path
        )

        metadata = pd.read_csv(metadata_file_name)

        clean_audio_duration = []
        list_file_utterances_with_duration = []

        for file_path, snr_value in processed_file_snr_dict.items():

            audio_file_name = file_path.split("/")[-1]
            LOGGER.info(audio_file_name)

            metadata["audio_id"] = audio_id
            metadata["media_hash_code"] = hash_code

            if self.feat_language_identification:
                language_confidence_score = infer_language(file_path)
            else:
                language_confidence_score = None
            LOGGER.info(
                "language_confidence_score:%s",
                str(language_confidence_score))
            clip_duration = calculate_duration(file_path)
            if snr_value < threshold:
                self.move_file_locally(
                    file_path, f"{rejected_dir_path}/{audio_file_name}"
                )
                list_file_utterances_with_duration.append(
                    {
                        "name": audio_file_name,
                        "duration": str(clip_duration),
                        "snr_value": snr_value,
                        "status": "Rejected",
                        "reason": "High-SNR",
                        "snr_threshold": threshold,
                        "language_confidence_score": language_confidence_score,
                    }
                )
                metadata["cleaned_duration"] = round(
                    (sum(clean_audio_duration) / 60), 2
                )
                metadata["utterances_files_list"] = json.dumps(
                    list_file_utterances_with_duration
                )

                metadata.to_csv(metadata_file_name, index=False)
                continue

            if clip_duration > SNR.MAX_DURATION:
                self.move_file_locally(
                    file_path, f"{rejected_dir_path}/{audio_file_name}"
                )
                list_file_utterances_with_duration.append(
                    {
                        "name": audio_file_name,
                        "duration": str(clip_duration),
                        "snr_value": snr_value,
                        "status": "Rejected",
                        "reason": "High Audio Duration",
                        "max_duration": SNR.MAX_DURATION,
                        "language_confidence_score": language_confidence_score,
                    }
                )
                metadata["cleaned_duration"] = round(
                    (sum(clean_audio_duration) / 60), 2
                )
                metadata["utterances_files_list"] = json.dumps(
                    list_file_utterances_with_duration
                )

                metadata.to_csv(metadata_file_name, index=False)
                continue

            clean_audio_duration.append(clip_duration)
            self.move_file_locally(
                file_path, f"{clean_dir_path}/{audio_file_name}")
            list_file_utterances_with_duration.append(
                {
                    "name": audio_file_name,
                    "duration": str(clip_duration),
                    "snr_value": snr_value,
                    "status": "Clean",
                    "language_confidence_score": language_confidence_score,
                }
            )

            metadata["cleaned_duration"] = round(
                (sum(clean_audio_duration) / 60), 2)
            metadata["utterances_files_list"] = json.dumps(
                list_file_utterances_with_duration
            )
            metadata.to_csv(metadata_file_name, index=False)
