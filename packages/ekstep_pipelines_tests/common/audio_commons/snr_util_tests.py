import subprocess
import unittest
from unittest import mock
from unittest.mock import Mock
from unittest.mock import call

from ekstep_data_pipelines.common.audio_commons.snr_util import SNR


class SNRTests(unittest.TestCase):

    expected_file_content = (
        "audio_id,cleaned_duration,utterances_files_list,media_hash_code\n"
        '17147714,0.5,"[{""name"": ""file1.wav"", ""duration"": ""10"",'
        ' ""snr_value"": 24.0, ""status"": ""Clean"", '
        '""language_confidence_score"": null}, {""name"": '
        '""file2.wav"", ""duration"": ""10"", ""snr_value"": '
        '25.0, ""status"": ""Clean"", ""language_confidence_score"": null}, '
        '{""name"": ""file3.wav"", ""duration"": ""10"", ""snr_value"": '
        '280.0, ""status"": ""Clean"", ""language_confidence_score"":'
        ' null}]",dummy_hash\n'
    )

    def setUp(self):
        self.maxDiff = None
        self.test_audio_file_path = "ekstep_pipelines_tests/resources/chunk.wav"
        self.audio_commons = {"snr_util": Mock(), "chunking_conversion": Mock()}
        self.snr = SNR()

    def setup_meta_data_file(self, file_name):
        headers = ["audio_id", "cleaned_duration", "utterances_files_list"]
        with open(file_name, "w") as file:
            file.write(",".join(headers) + "\n" + "1,abc,null")

    def test__should_return_command_when_get_command_called_with_file_path_and_dir(
        self,
    ):
        command = (
            '"test_dir/ekstep_data_pipelines/binaries/WadaSNR/Exe/WADASNR" -i '
            '"input_file_path" -t "test_dir/ekstep_data_pipelines/binaries/WadaSNR/'
            'Exe/Alpha0.400000.txt" -ifmt mswav'
        )
        actual_output = self.snr.get_command("test_dir", "input_file_path")
        self.assertEqual(actual_output, command)

    @mock.patch("subprocess.check_output")
    def test__should_return_SNR_when_compute_file_snr_called(
        self, mock_subprocess_check_output
    ):

        mock_subprocess_check_output.return_value = b"4.0 mock_output mock_output"

        snr = self.snr.compute_file_snr(self.test_audio_file_path)

        self.assertEqual(mock_subprocess_check_output.call_count, 1)
        self.assertEqual(snr, 4.0)

    @mock.patch("subprocess.check_output")
    def test__should_raise_error_when_compute_file_snr_called_and_subprocess_failed(
        self, mock_subprocess_check_output
    ):

        mock_subprocess_check_output.side_effect = subprocess.CalledProcessError(
            -1, "some_command"
        )
        snr = self.snr.compute_file_snr(self.test_audio_file_path)

        self.assertEqual(mock_subprocess_check_output.call_count, 1)
        self.assertEqual(snr, -1)

    @mock.patch("subprocess.check_output")
    def test__should_retun_file_snr_dict_when_process_files_list_called(
        self, mock_subprocess_check_output
    ):

        mock_subprocess_check_output.side_effect = [
            b"4.0 mock_output mock_output",
            b"5.0 mock_output mock_output",
            b"80.0 mock_output mock_output",
        ]
        snr_file_dict = self.snr.process_files_list(
            ["file1.wav", "file2.wav", "file3.wav"]
        )

        expected_value = {"file1.wav": 4.0, "file2.wav": 5.0, "file3.wav": 80.0}

        self.assertEqual(mock_subprocess_check_output.call_count, 3)

        self.assertEqual(snr_file_dict, expected_value)

    @mock.patch("subprocess.check_output")
    def test__should_retun_file_snr_dict_and_convert_nan_to_0_when_process_files_list_called(
        self, mock_subprocess_check_output
    ):

        mock_subprocess_check_output.side_effect = [
            b"nan mock_output mock_output",
            b"5.0 mock_output mock_output",
            b"80.0 mock_output mock_output",
        ]
        snr_file_dict = self.snr.process_files_list(
            ["file1.wav", "file2.wav", "file3.wav"]
        )

        expected_value = {"file1.wav": 0.0, "file2.wav": 5.0, "file3.wav": 80.0}

        self.assertEqual(mock_subprocess_check_output.call_count, 3)
        self.assertEqual(snr_file_dict, expected_value)

    @mock.patch("subprocess.check_output")
    @mock.patch("sox.file_info.duration")
    @mock.patch("shutil.move")
    def test__given_valid_file_input_list__when_fit_and_move_is_invoked__then_update_all_values_in_the_metadata_file(
        self, mock_shutil, mock_sox, mock_subprocess_check_output
    ):

        # input setup
        input_file_list = ["file1.wav", "file2.wav", "file3.wav"]
        meta_data_file_name = "test_file.csv"
        threshold = 15
        output_dir_path = "/tmp"
        audio_id = "17147714"
        hash_code = "dummy_hash"

        self.setup_meta_data_file(meta_data_file_name)

        mock_sox.return_value = 10

        mock_subprocess_check_output.side_effect = [
            b"24.0 mock_output mock_output",
            b"25.0 mock_output mock_output",
            b"280.0 mock_output mock_output",
        ]
        self.snr.fit_and_move(
            input_file_list,
            meta_data_file_name,
            threshold,
            output_dir_path,
            audio_id,
            hash_code,
        )

        mock_calls = [call("file1.wav"), call("file2.wav"), call("file3.wav")]

        # assert mock calls
        mock_shutil.has_calls(mock_calls, any_order=False)
        self.assertEqual(mock_shutil.call_count, 3)

        mock_sox.has_calls(mock_calls, any_order=False)
        self.assertEqual(mock_sox.call_count, 3)

        mock_subprocess_check_output.has_calls(mock_calls, any_order=False)
        self.assertEqual(mock_subprocess_check_output.call_count, 3)

        # check the metadata file for information
        with open(meta_data_file_name) as file:
            meta_data_contents = file.read()

        self.assertEqual(meta_data_contents, self.expected_file_content)

    @mock.patch("shutil.move")
    @mock.patch("subprocess.check_output")
    @mock.patch("sox.file_info.duration")
    @mock.patch("ekstep_data_pipelines.common.audio_commons.snr_util.infer_language")
    def test__given_valid_file_input_list__when_fit_and_move_is_invoked__then_update_all_values_in_the_metadata_file_with_LID(
        self, mock_infer_language, mock_sox, mock_subprocess_check_output, mock_shutil
    ):
        # input setup
        input_file_list = ["file1.wav", "file2.wav", "file3.wav"]
        meta_data_file_name = "test_file.csv"
        threshold = 15
        output_dir_path = "/tmp"
        audio_id = "17147714"
        hash_code = "dummy_hash"

        self.setup_meta_data_file(meta_data_file_name)

        mock_infer_language.return_value = {"hi-IN": "0.00004", "en": "0.99996"}
        mock_sox.return_value = 10

        mock_subprocess_check_output.side_effect = [
            b"24.0 mock_output mock_output",
            b"25.0 mock_output mock_output",
            b"280.0 mock_output mock_output",
        ]
        snr = SNR(True)

        snr.fit_and_move(
            input_file_list,
            meta_data_file_name,
            threshold,
            output_dir_path,
            audio_id,
            hash_code,
        )

        mock_calls = [call("file1.wav"), call("file2.wav"), call("file3.wav")]

        # assert mock calls
        mock_shutil.has_calls(mock_calls, any_order=False)
        self.assertEqual(mock_shutil.call_count, 3)

        mock_sox.has_calls(mock_calls, any_order=False)
        self.assertEqual(mock_sox.call_count, 3)

        mock_subprocess_check_output.has_calls(mock_calls, any_order=False)
        self.assertEqual(mock_subprocess_check_output.call_count, 3)

        # check the metadata file for information
        with open(meta_data_file_name) as file:
            meta_data_contents = file.read()
        expected_file_content = (
            "audio_id,cleaned_duration,utterances_files_list,media_hash_code"
            '\n17147714,0.5,"[{""name"": ""file1.wav"", ""duration"": '
            '""10"", ""snr_value"": 24.0, ""status"": ""Clean"", '
            '""language_confidence_score"": {""hi-IN"": ""0.00004"", '
            '""en"": ""0.99996""}}, {""name"": ""file2.wav"", ""duration"":'
            ' ""10"", ""snr_value"": 25.0, ""status"": ""Clean"", '
            '""language_confidence_score"": {""hi-IN"": ""0.00004"", '
            '""en"": ""0.99996""}}, {""name"": ""file3.wav"", ""duration"":'
            ' ""10"", ""snr_value"": 280.0, ""status"": ""Clean"", '
            '""language_confidence_score"": {""hi-IN"": ""0.00004"", '
            '""en"": ""0.99996""}}]",dummy_hash\n'
        )
        print("meta_data_contents:" + meta_data_contents)
        print("expected_file_content:" + self.expected_file_content)
        self.assertEqual(expected_file_content, meta_data_contents)
