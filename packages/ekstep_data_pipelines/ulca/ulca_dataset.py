import copy
import fnmatch
import json
import multiprocessing
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from ekstep_data_pipelines.common import BaseProcessor, CatalogueDao
from ekstep_data_pipelines.common.utils import get_logger
from tqdm import tqdm

ESTIMATED_CPU_SHARE = 0.1
DEFAULT_COUNT = 10000

LOGGER = get_logger("ULCADataset")





class ULCADataset(BaseProcessor):
    """
    Class to identify speaker for each utterance in a source
    """

    DEFAULT_DOWNLOAD_PATH = "."
    ULCA_CONFIG = "ulca_config"
    SOURCE = "source"
    ULCA_PARAMS = "params"
    LANGUAGE = "language"
    SOURCE_PATH = "source_path"
    PUBLISH_PATH = "publish_path"
    EXPORT_COUNT = "export_count"
    GENDER_MAP = {
        "m": "male",
        "f": "female"
    }
    LABELLED = 'labelled'
    IS_TRANSCRIBED = 'is_transcribed'
    INCLUDE_REJECTED = 'include_rejected'
    IS_EXTERNAL = 'is_external'

    @staticmethod
    def get_instance(data_processor, **kwargs):
        return ULCADataset(data_processor, **kwargs)

    def __init__(self, data_processor, **kwargs):
        self.data_processor = data_processor
        self.ulca_config = None
        self.catalogue_dao = CatalogueDao(self.data_processor)
        super().__init__(**kwargs)

    def handle_termination_gracefully(self, signum, frame):
        LOGGER.info(
            f"SIGINT/SIGTERM invoked with the following information {signum}/{frame}"
        )
        sys.exit(1)

    def process(self, **kwargs):
        """
        Function for mapping utterance to speakers
        """
        LOGGER.info("Total available cpu count:" + str(multiprocessing.cpu_count()))

        source, ulca_config, language, source_path, publish_path, \
        params, export_count, is_labelled, is_transcribed, include_rejected, is_external = self.get_config(**kwargs)
        is_external = True if is_external.lower() == "true" else False

        utterances = self.get_clean_utterances(source, language, self.catalogue_dao, is_transcribed, include_rejected,
                                               is_external, source_path, export_count)

        current_time_formatted = self.get_timestamp(datetime.now())

        local_audio_download_path = f"{ULCADataset.DEFAULT_DOWNLOAD_PATH}/{source}_{current_time_formatted}/"
        self.ensure_path(local_audio_download_path)

        LOGGER.info(f"Ensured {local_audio_download_path} exists")

        self.download_utterances(local_audio_download_path, source_path, utterances, is_labelled, is_external)

        text_dict = self.read_transcriptions(local_audio_download_path)

        data = self.create_data_json(text_dict, source, utterances, is_labelled)

        if len(data) > 0:
            self.write_json(local_audio_download_path, "data.json", data)
            self.write_json(local_audio_download_path, "params.json", params)

            self.remove_txt_file(local_audio_download_path)
            self.remove_rejected_files(local_audio_download_path, data)

            self.make_zipfile(f"{source}.zip", local_audio_download_path)
            artifact_name = f"{source}_{current_time_formatted}.zip"
            self.publish_artifact(f"{source}.zip", f"{publish_path}/{artifact_name}")
            if not is_external:
                self.update_artifact_name(data, artifact_name)
        else:
            LOGGER.info('No data to create artifact')
            raise RuntimeError('No data exists to create artifact')

    def download_utterances(self, local_audio_download_path, source_path, utterances, is_labelled, is_external):

        LOGGER.info(f"Downloading source to:{local_audio_download_path}")

        max_workers = multiprocessing.cpu_count() / ESTIMATED_CPU_SHARE

        curr_executor = ThreadPoolExecutor(max_workers)

        for utterance in tqdm(utterances):
            file_name = utterance[0]
            audio_id = utterance[7]
            status = utterance[8].lower()
            text_file_name = f"{file_name.split('.')[0]}.txt"
            if is_external:
                source_path_utterance = f"{source_path}/{file_name}"
                source_path_utterance_text = f"{source_path}/{text_file_name}"
            else:
                source_path_utterance = f"{source_path}/{audio_id}/{status}/{file_name}"
                source_path_utterance_text = f"{source_path}/{audio_id}/{status}/{text_file_name}"

            local_file_path_sans_extention = f"{local_audio_download_path}/{file_name.split('.')[0]}"
            curr_executor.submit(self.fs_interface.download_to_location, source_path_utterance,
                                 f"{local_file_path_sans_extention}.wav")
            if is_labelled == 'True':
                curr_executor.submit(self.fs_interface.download_to_location, source_path_utterance_text,
                                     f"{local_file_path_sans_extention}.txt")
        curr_executor.shutdown(wait=True)
        LOGGER.info('Download complete...')

    def exclude_attributes(self,data):
        del data['audioId']
        del data['speaker']
        return data

    def write_json(self, local_audio_download_path, filename, data):
        data_cleaned = copy.deepcopy(data)
        if 'data' in filename:
            data_cleaned = list(map(self.exclude_attributes, data_cleaned))
        data_json = json.dumps(data_cleaned, indent=4)
        with open(f"{local_audio_download_path}/{filename}", "w") as f:
            f.write(data_json)


    def ensure_path(self, path):
        os.makedirs(path, exist_ok=True)

    def get_config(self, **kwargs):
        source = kwargs.get(ULCADataset.SOURCE)
        ulca_config = json.loads(kwargs.get(ULCADataset.ULCA_CONFIG))
        LOGGER.info(f"ulca_config:{str(ulca_config)}")
        language = ulca_config.get(ULCADataset.LANGUAGE)
        source_path = ulca_config.get(ULCADataset.SOURCE_PATH)
        publish_path = ulca_config.get(ULCADataset.PUBLISH_PATH)
        export_count = ulca_config.get(ULCADataset.EXPORT_COUNT)
        params = ulca_config.get(ULCADataset.ULCA_PARAMS)
        is_labelled = ulca_config.get(ULCADataset.LABELLED, "True")
        is_transcribed = ulca_config.get(ULCADataset.IS_TRANSCRIBED, "True")
        include_rejected = ulca_config.get(ULCADataset.INCLUDE_REJECTED, "False")
        is_external = ulca_config.get(ULCADataset.IS_EXTERNAL, "False")

        if source is None:
            raise Exception("source is mandatory")

        if ulca_config is None:
            raise Exception("ulca_config is mandatory")

        if language is None:
            raise Exception("language is mandatory")

        if source_path is None:
            raise Exception("source_path is mandatory")

        if publish_path is None:
            raise Exception("publish_path is mandatory")

        if params is None:
            raise Exception("params is mandatory")

        return source, ulca_config, language, source_path, publish_path, params, export_count, \
               is_labelled, is_transcribed, include_rejected, is_external

    def get_params(self):
        return self.ulca_config.get(ULCADataset.ULCA_PARAMS)

    def get_clean_utterances(self, source, language, catalogue_dao, is_transcribed, include_rejected, is_external,
                             source_path,
                             count=DEFAULT_COUNT):
        is_transcribed = True if is_transcribed.lower() == "true" else False
        include_rejected = True if include_rejected.lower() == "true" else False
        LOGGER.info(f"Creating json for source:{source}, language={language}")
        if is_external:
            all_files = self.fs_interface.list_blobs_in_a_path(source_path)
            utterances = [[file.name.split('/')[-1], 5, 15, 1, 'unknown', 'unknown', 'non-specified', 123, 'Clean']
                          for
                          file in
                          all_files if
                          'wav' in file.name]
        else:
            utterances = catalogue_dao.get_utterance_details_by_source(source, language, count, is_transcribed,
                                                                       include_rejected)
            LOGGER.info(f"total utterances: {str(len(utterances))}")
        if len(utterances) <= 0:
            raise LookupError(f"No data found in catalogue for language={language}, source={source}")
        return utterances

    def create_data_json(self, text_dict, source, utterances, is_labelled="True"):
        data = [
            self.to_data_element(utterance, source, text_dict, is_labelled)
            for utterance in utterances
        ]
        data = list(filter(lambda d: d != {}, data))

        LOGGER.info(f"Created data json object with len:{len(data)}")
        return data

    def to_data_element(self, utterance, source, text_dict, is_labelled):
        file_name = utterance[0]
        duration = utterance[1]
        snr = utterance[2]
        speaker = utterance[3]
        main_source_url = utterance[4]
        source_url = utterance[5]
        gender = utterance[6]
        audio_id = utterance[7]
        snr = {"methodType": "WadaSnr", "methodDetails": {"snr": snr}}
        file_name_key = file_name.split(".")[0]

        data = {
                "audioFilename": file_name,
                "collectionSource": [source, main_source_url, source_url],
                "snr": snr,
                "duration": duration,
                "speaker": speaker,
                "gender": ULCADataset.GENDER_MAP.get(gender, "non-specified"),
                "audioId": audio_id
            }

        if is_labelled == "False":
            return data

        if is_labelled == "True" and file_name_key in text_dict:
            text = text_dict.get(file_name_key, "")
            data['text'] = text
            return data
        else:
            return {}

    def read_transcriptions(self, local_source_path):
        listOfFiles = os.listdir(local_source_path)
        LOGGER.info(f'all files downloaded:{len(listOfFiles)}')

        pattern = "*.txt"
        text_dict = {}

        for entry in listOfFiles:
            if fnmatch.fnmatch(entry, pattern):
                print(entry)
                with open(f"{local_source_path}/{entry}", "r") as reader:
                    transcription = reader.read()
                    file_name = entry.split(".")[0]
                    text_dict[file_name] = transcription

        LOGGER.info(f"text filed read :{len(text_dict.keys())}")
        return text_dict

    def make_zipfile(self, output_filename, source_dir):
        subprocess.call(["zip", "-r", output_filename, source_dir])

    def publish_artifact(self, tar_file, publish_path):
        LOGGER.info('publishing artifact')
        self.fs_interface.upload_to_location(tar_file, publish_path)

    def remove_txt_file(self, local_path):
        LOGGER.info('removing txt files')
        listOfFiles = os.listdir(local_path)
        pattern = "*.txt"
        for entry in listOfFiles:
            if fnmatch.fnmatch(entry, pattern):
                os.remove(f"{local_path}/{entry}")

    def remove_rejected_files(self, local_path, data):
        LOGGER.info('Remove files not in catalogue or not clean based on data.json')
        listOfFiles = os.listdir(local_path)
        valid_files = list(map(lambda d: d['audioFilename'], data))
        pattern = "*.wav"
        for entry in listOfFiles:
            if (fnmatch.fnmatch(entry, pattern)) and (entry not in valid_files):
                LOGGER.info(f"Removing {entry}...")
                os.remove(f"{local_path}/{entry}")

    def get_timestamp(self, date_time):
        return date_time.strftime("%d-%m-%Y_%H-%M")

    def update_artifact_name(self, data, artifact_name):
        audioIdToUtteranceName = {}
        for element in data:
            audio_id = element['audioId']
            utteranceFileNames = audioIdToUtteranceName.get(element['audioId'], [])
            utteranceFileNames.append(element['audioFilename'])
            audioIdToUtteranceName[audio_id] = utteranceFileNames

        for audio_id, utteranceFileNames in audioIdToUtteranceName.items():
            LOGGER.info(f"Updating artifact_name={artifact_name} for audio_id:{audio_id}")
            updated = self.catalogue_dao.update_utterance_artifact(utteranceFileNames, artifact_name, audio_id)
            LOGGER.info(f"updated....{updated}")
