import os
import unittest
from unittest.mock import Mock, call

from ekstep_data_pipelines.ulca.ulca_dataset import ULCADataset
from datetime import datetime


class ULCADatasetTests(unittest.TestCase):
    def setUp(self):
        self.data_processor = Mock()
        self.catalogue_dao = Mock()
        self.maxDiff = None

    def test_should_get_utterances(self):
        utterances = [
            (
                "sample1.wav",
                13.38,
                38.432806,
                "dummy_speaker_name",
                "dummy_main_source",
                "dummy_collection_source",
                "m"
            ),
            (
                "sample2.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
                "f"
            ),
            (
                "sample3.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
                "f"
            )
        ]
        self.catalogue_dao.get_utterance_details_by_source.return_value = utterances

        actual_utterances = ULCADataset(self.data_processor).get_clean_utterances("test_source", "Hindi", self.catalogue_dao, "true", "false", 2)
        print("actual_utterances", actual_utterances)
        select_args = self.catalogue_dao.get_utterance_details_by_source.call_args
        print(select_args)
        self.assertEqual(actual_utterances, utterances)
        self.assertEqual(('test_source', 'Hindi', 2, True, False), select_args[0])


    def test_should_create_data_json(
        self,
    ):
        utterances = [
            (
                "sample1.wav",
                13.38,
                38.432806,
                "dummy_speaker_name",
                "dummy_main_source",
                "dummy_collection_source",
                "m",
                1
            ),
            (
                "sample2.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
                "f",
                2
            ),
            (
                "sample3.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
                "f",
                3
            )
        ]

        expected_data = [
            {
                "audioFilename": "sample1.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source",
                    "dummy_collection_source",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 38.432806}},
                "duration": 13.38,
                "speaker": "dummy_speaker_name",
                "gender": "male",
                "audioId": 1
            },
            {
                "audioFilename": "sample2.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source_2",
                    "dummy_collection_source_2",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 40.432806}},
                "duration": 15.38,
                "speaker": "dummy_speaker_name_2",
                "gender": "female",
                "audioId": 2
            }
        ]
        text_dict = {"sample1": "sample text", "sample2": "sample text"}
        data = ULCADataset(self.data_processor).create_data_json(
            text_dict, "test_source", utterances
        )
        self.assertEqual(expected_data, data)

    def test_should_read_transcription(self):
        expected_text_dict = {"test1": "sample text 1", "test2": "sample text 2"}
        text_dict = ULCADataset(self.data_processor).read_transcriptions(
            "ekstep_pipelines_tests/resources/ulca"
        )
        self.assertEqual(expected_text_dict, text_dict)

    def test_should_create_zip(self):
        zip_file_name = "ulca.zip"
        source_dir = "ekstep_pipelines_tests/resources/ulca/"
        ULCADataset(self.data_processor).make_zipfile(zip_file_name, source_dir)

    def test_should_remove_txt_files(self):
        temp_dir = "ekstep_pipelines_tests/resources/ulca/temp"
        filenames = ["file1.txt", "file2.txt", "file2.wav"]
        os.makedirs(temp_dir, exist_ok=True)

        for filename in filenames:
            with open(f"{temp_dir}/{filename}", "w") as f:
                f.write("test content")

        ULCADataset(self.data_processor).remove_txt_file(temp_dir)

        listOfFiles = os.listdir(temp_dir)

        self.assertEqual(listOfFiles, ["file2.wav"])


    def test_should_get_timestamp_in_specified_format(self):
        date_time = datetime.strptime('12/02/2021', '%d/%m/%Y')
        print('date:', date_time)
        formatted = ULCADataset(self.data_processor).get_timestamp(date_time)
        self.assertEqual('12-02-2021_00-00', formatted)


    def test_should_update_artifact_name(self):
        data = [
            {
                "audioFilename": "sample1.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source",
                    "dummy_collection_source",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 38.432806}},
                "duration": 13.38,
                "speaker": "dummy_speaker_name",
                "gender": "male",
                "audioId": 1
            },
            {
                "audioFilename": "sample2.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source_2",
                    "dummy_collection_source_2",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 40.432806}},
                "duration": 15.38,
                "speaker": "dummy_speaker_name_2",
                "gender": "female",
                "audioId": 2
            },
            {
                "audioFilename": "sample3.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source_2",
                    "dummy_collection_source_2",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 40.432806}},
                "duration": 15.38,
                "speaker": "dummy_speaker_name_2",
                "gender": "female",
                "audioId": 2
            }
        ]

        self.catalogue_dao.update_utterance_artifact.return_value = True

        ULCADataset(self.data_processor).update_artifact_name(data, 'test_name')

        calls = [call.update_utterance_artifact(['sample1.wav'], 'test_name', 1), call.update_utterance_artifact(['sample2.wav', 'sample3.wav'], 'test_name', 2)]

        call_args = self.catalogue_dao.update_utterance_artifact.call_args
        print('call_args', call_args)
        # self.assertEqual(2, self.catalogue_dao.call_count)
        # self.catalogue_dao.assert_has_calls(calls, any_order=True)


    def test_should_create_data_json_for_non_labelled_corpus(
        self,
    ):
        utterances = [
            (
                "sample1.wav",
                13.38,
                38.432806,
                "dummy_speaker_name",
                "dummy_main_source",
                "dummy_collection_source",
                "m",
                1
            ),
            (
                "sample2.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
                "f",
                2
            )
        ]

        expected_data = [
            {
                "audioFilename": "sample1.wav",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source",
                    "dummy_collection_source",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 38.432806}},
                "duration": 13.38,
                "speaker": "dummy_speaker_name",
                "gender": "male",
                "audioId": 1
            },
            {
                "audioFilename": "sample2.wav",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source_2",
                    "dummy_collection_source_2",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 40.432806}},
                "duration": 15.38,
                "speaker": "dummy_speaker_name_2",
                "gender": "female",
                "audioId": 2
            }
        ]
        text_dict = {}
        data = ULCADataset(self.data_processor).create_data_json(
            text_dict, "test_source", utterances, "False"
        )
        self.assertEqual(expected_data, data)
