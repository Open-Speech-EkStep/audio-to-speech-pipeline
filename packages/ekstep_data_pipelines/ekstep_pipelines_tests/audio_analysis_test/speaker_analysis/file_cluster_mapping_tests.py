import sys
import unittest

from audio_analysis.speaker_analysis.file_cluster_mapping import file_to_speaker_map, speaker_to_file_name_map

sys.path.insert(0, '..')


class FileClusterMappingTests(unittest.TestCase):

    def setUp(self):
        super(FileClusterMappingTests, self).setUp()
        self.maxDiff = None

    def test_should_create_filename_to_speaker_map(self):
        file_map_dict = {'mkb_sp_0':
                             ['ekstep_pipelines_tests/resources/CEC/202008120411310459/clean/13_Hashiye_Ka_Siddhant_aur_Sahitya.wav',
                              'ekstep_pipelines_tests/resources/CEC/202008120411310459/clean/14_Hashiye_Ka_Siddhant_aur_Sahitya.wav',
                              'ekstep_pipelines_tests/resources/CEC/202008120411310459/clean/12_Hashiye_Ka_Siddhant_aur_Sahitya.wav'],
                         'mkb_sp_1':
                             ['ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/149_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',
                              'ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/150_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',
                              'ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/153_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav'],
                          'mkb_sp_2':
                             ['ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/155_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',
                              'ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/151_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav']
                         }
        expected = {
            '12_Hashiye_Ka_Siddhant_aur_Sahitya.wav': 'mkb_sp_0',
            '13_Hashiye_Ka_Siddhant_aur_Sahitya.wav': 'mkb_sp_0',
            '14_Hashiye_Ka_Siddhant_aur_Sahitya.wav': 'mkb_sp_0',
            '149_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav': 'mkb_sp_1',
            '150_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav': 'mkb_sp_1',
            '153_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav': 'mkb_sp_1',
            '155_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav': 'mkb_sp_2',
            '151_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav': 'mkb_sp_2',
        }

        speaker_map = file_to_speaker_map(file_map_dict)
        print(len(speaker_map))
        self.assertEqual(len(expected), len(speaker_map))
        self.assertEqual(expected, speaker_map)

    def test_should_create_speaker_to_filename_map(self):
        file_map_dict = {'mkb_sp_0':
                             [('ekstep_pipelines_tests/resources/CEC/202008120411310459/clean/13_Hashiye_Ka_Siddhant_aur_Sahitya.wav', 0),
                              ('ekstep_pipelines_tests/resources/CEC/202008120411310459/clean/14_Hashiye_Ka_Siddhant_aur_Sahitya.wav', 1),
                              ('ekstep_pipelines_tests/resources/CEC/202008120411310459/clean/12_Hashiye_Ka_Siddhant_aur_Sahitya.wav', 0)],
                         'mkb_sp_1':
                             [('ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/149_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0),
                              ('ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/150_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0),
                              ('ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/153_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',1)],
                          'mkb_sp_2':
                             [('ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/155_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0),
                              ('ekstep_pipelines_tests/resources/CEC/202008120507305747/clean/151_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0)]
                         }
        expected = {'mkb_sp_0':
                             [('13_Hashiye_Ka_Siddhant_aur_Sahitya.wav', 0),
                              ('14_Hashiye_Ka_Siddhant_aur_Sahitya.wav',1),
                              ('12_Hashiye_Ka_Siddhant_aur_Sahitya.wav',0)],
                     'mkb_sp_1':
                         [('149_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0),
                          ('150_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0),
                          ('153_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',1)],
                     'mkb_sp_2':
                         [('155_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0),
                          ('151_Hindi_Bhasha_ke_Vikas_ki_Purvpithika.wav',0)]
                     }
        speaker_map = speaker_to_file_name_map(file_map_dict)
        print(len(speaker_map))
        self.assertEqual(len(expected), len(speaker_map))
        self.assertEqual(expected, speaker_map)