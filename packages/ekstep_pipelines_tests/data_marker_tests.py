import json, os
import unittest
import pandas as pd
from unittest.mock import Mock

from ekstep_data_pipelines.data_marker.data_marker import DataMarker


class DataMarkerTests(unittest.TestCase):
    def setUp(self):
        self.postgres_client = Mock()
        self.gcp_instance = Mock()
        self.data_stager = DataMarker(self.postgres_client, self.gcp_instance)
        self.data_stager.fs_interface = Mock()

    def test__should_transform_utterances_to_file_paths(self):
        source_base_path = (
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/"
            "hindi/audio/source_1"
        )
        utterances = [
            (1, "file_10.wav", 4, "2020123", 13),
            (1, "file_11.wav", 1, "2020124", 14),
        ]
        files = self.data_stager.to_files(utterances, source_base_path)

        expected_files = [
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/"
            "source_1/2020123/clean/file_10.wav",
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/"
            "source_1/2020124/clean/file_11.wav",
        ]
        self.assertListEqual(files, expected_files)

    def test__should_append_audio_ids_to_file_paths(self):
        source_base_path = (
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/"
            "hindi/audio/source_1"
        )
        audio_ids = [
            '123', '324', '434'
        ]
        paths = self.data_stager.to_paths(audio_ids, source_base_path)

        expected_files = [
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/"
            "source_1/123",
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/"
            "source_1/324",
            "gs://ekstepspeechrecognition-dev/data/audiotospeech/raw/catalogued/hindi/audio/"
            "source_1/434",
        ]
        self.assertListEqual(paths, expected_files)

    def test__should_parse_config(self):
        data_marker_config = (
            '{ "swayamprabha_chapter_4": { "language": "hindi","file_mode" : "y", "data_set" : "test", "file_path" : "../test.csv","filter": { "then_by_snr": { '
            '"lte": 100, "gte": 15 }, "then_by_duration": 1, "with_randomness":'
            ' "true" } } }'
        )

        filteg_config = json.loads(data_marker_config)
        key = list(filteg_config.keys())[0]
        kwargs = {"filter_spec": filteg_config.get(key), "source": key}
        source, language ,data_set, filter_criteria, file_mode, file_path = self.data_stager.get_config(**kwargs)
        self.assertEqual(source, "swayamprabha_chapter_4")
        self.assertEqual(data_set, "test")
        self.assertEqual(language, "hindi")
        self.assertEqual(file_mode, "y")
        self.assertEqual(file_path, "../test.csv")
        self.assertEqual(
            filter_criteria,
            {
                "then_by_snr": {"lte": 100, "gte": 15},
                "then_by_duration": 1,
                "with_randomness": "true",
            },
        )

        by_snr = filter_criteria.get("then_by_snr", None)
        by_duration = filter_criteria.get("then_by_duration", None)
        with_randomness = filter_criteria.get("with_randomness", "false")

        self.assertEqual(by_snr, {"lte": 100, "gte": 15})
        self.assertEqual(by_duration, 1)

    def test__should_return_distinct_audio_ids_from_utterances(self):
        utterances = [
            (1, "file_10.wav", 4, "2020123", 13),
            (1, "file_11.wav", 1, "2020124", 14),
            (2, "file_12.wav", 1, "2020124", 15),
            (3, "file_13.wav", 1, "2020124", 14),
            (3, "file_15.wav", 1, "2020125", 14)
        ]
        audio_ids_list = self.data_stager.fetch_distinct_audio_ids(utterances)

        expected_files = [
            '2020123', '2020124', '2020125'
        ]
        self.assertListEqual(audio_ids_list, expected_files)

    def test__should_download_filtered_utterances_file(self):
        bucket = 'ekstepspeechrecognition-test'
        input_file_path = "data/audiotospeech/raw/download/duplicate/test_source1.csv"
        local_path = "./data_marker/file_path/"
        expected_download_path = f'{local_path}{os.path.basename(input_file_path)}'
        actual_path_downloaded = self.data_stager.download_filtered_utterances_file(bucket, input_file_path, local_path)
        call_args_list = self.data_stager.fs_interface.download_file_to_location.call_args_list
        self.assertEqual(
            self.data_stager.fs_interface.download_file_to_location.call_count,
            1,
        )
        self.assertEqual(
            call_args_list[0][0][0],
            bucket + '/' + input_file_path,
        )
        self.assertEqual(
            call_args_list[0][0][1],
            expected_download_path,
        )

        self.assertEqual(actual_path_downloaded, expected_download_path)

    def test__should_raise_exception_while_getting_utterances_from_empty_file(self):
        records = [

        ]
        FILE_COLUMN_LIST = ["speaker_id", "clipped_utterance_file_name", "clipped_utterance_duration", "audio_id",
                            "snr"]
        df = pd.DataFrame.from_records(records, columns=FILE_COLUMN_LIST)
        local_file_path = './dummy.csv'
        df.to_csv(local_file_path)
        with self.assertRaises(Exception):
            self.data_stager.get_utterances_from_file(local_file_path)
