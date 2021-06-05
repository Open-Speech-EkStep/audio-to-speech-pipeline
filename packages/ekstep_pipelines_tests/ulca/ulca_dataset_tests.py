import unittest
from unittest import mock
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
            ),
            (
                "sample2.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
            ),
            (
                "sample3.wav",
                15.38,
                40.432806,
                "dummy_speaker_name_2",
                "dummy_main_source_2",
                "dummy_collection_source_2",
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
                    "dummy_collection_source"
                ],
                "snr": {
                    "methodType": "WadaSnr",
                    "methodDetails": {
                        "snr": 38.432806
                    }
                },
                "duration": 13.38
            },
            {
                "audioFilename": "sample2.wav",
                "text": "sample text",
                "collectionSource": [
                    "test_source",
                    "dummy_main_source_2",
                    "dummy_collection_source_2"
                ],
                "snr": {
                    "methodType": "WadaSnr",
                    "methodDetails": {
                        "snr": 40.432806
                    }
                },
                "duration": 15.38
            }
        ]

        text_dict = {"sample1": "sample text", "sample2": "sample text"}
        data = ULCADataset(self.data_processor).create_data_json(
            text_dict,
            "test_source",
            "Hindi",
            self.catalogue_dao
        )
        select_args = self.catalogue_dao.get_utterance_details_by_source.call_args
        print(data)
        self.assertEqual("test_source", select_args[0][0])
        self.assertEqual("Hindi", select_args[0][1])
        self.assertEqual(expected_data, data)

    def test_should_read_transcription(self):
        expected_text_dict = {"test1": "sample text 1", "test2": "sample text 2"}
        text_dict = ULCADataset(self.data_processor).read_transcriptions("ekstep_pipelines_tests/resources/ulca")
        self.assertEqual(expected_text_dict, text_dict)