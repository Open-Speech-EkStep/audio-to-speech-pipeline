import os
import unittest
from unittest.mock import Mock

from ekstep_data_pipelines.ulca.ulca_dataset import ULCADataset


class ULCADatasetTests(unittest.TestCase):
    def setUp(self):
        self.data_processor = Mock()
        self.catalogue_dao = Mock()
        self.maxDiff = None

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
                "gender": "male"
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
                "gender": "female"
            }
        ]

        text_dict = {"sample1": "sample text", "sample2": "sample text"}
        data = ULCADataset(self.data_processor).create_data_json(
            text_dict, "test_source", "Hindi", self.catalogue_dao
        )
        select_args = self.catalogue_dao.get_utterance_details_by_source.call_args
        print(data)
        self.assertEqual("test_source", select_args[0][0])
        self.assertEqual("Hindi", select_args[0][1])
        self.assertEqual(expected_data, data)

    def test_should_read_transcription(self):
        expected_text_dict = {"test1": "sample text 1", "test2": "sample text 2"}
        text_dict = ULCADataset(self.data_processor).read_transcriptions(
            "ekstep_pipelines_tests/resources/ulca"
        )
        self.assertEqual(expected_text_dict, text_dict)

    def test_should_create_tar(self):
        tar_file_name = "ulca.tar.gz"
        source_dir = "ekstep_pipelines_tests/resources/ulca/"
        ULCADataset(self.data_processor).make_tarfile(tar_file_name, source_dir)

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

    @unittest.skip
    def test_remove_rejected_files(self):

        data = [
            {
                "audioFilename": "file1.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source",
                    "dummy_collection_source",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 38.432806}},
                "duration": 13.38,
                "speaker": "dummy_speaker_name",
                "gender": "male"
            },
            {
                "audioFilename": "file2.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source_2",
                    "dummy_collection_source_2",
                ],
                "snr": {"methodType": "WadaSnr", "methodDetails": {"snr": 40.432806}},
                "duration": 15.38,
                "speaker": "dummy_speaker_name_2",
                "gender": "female"
            }
        ]
        temp_dir = "ekstep_pipelines_tests/resources/ulca/temp2"
        filenames = ["file2.wav", "file1.wav", "file3.wav", "data.json" , "params.json"]
        os.makedirs(temp_dir, exist_ok=True)

        for filename in filenames:
            with open(f"{temp_dir}/{filename}", "w") as f:
                f.write("test content")

        ULCADataset(self.data_processor).remove_rejected_files(temp_dir, data)

        listOfFiles = os.listdir(temp_dir)

        self.assertTrue("file1.wav" in listOfFiles)
        self.assertTrue("file1.wav" in listOfFiles)
        self.assertTrue("data.json" in listOfFiles)
        self.assertTrue("params.json" in listOfFiles)
        self.assertEqual(len(listOfFiles), 4)