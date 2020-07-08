from src.main.create_chunks_vad import *

import sys


def create_vad_chunks_for_file(args, dir_to_save_chunks, path_to_save_log_file):
    '''
    Saves chunks of an audio file after applying VAD.
    Also saves the vad stddout file in the directory specified
    :param args: list | elem 1 = aggresiveness of vad algo, elem 2 = path_to_audio_file
    :param dir_to_save_chunks: string | directory to save the chunks after applying vad in
    :param path_to_save_log_file: string | path to save log file for vad
                                - one file per audio (before chunking)
                                - log file name = <main audio name>.txt
                                - eg: audio = 'pm_modi.wav' so logfile = 'pm_modi.txt'
    :return:
    '''
    file_name = args[1].split('/')[-1]
    file_name = file_name.replace('.wav', '')
    txt_file_name = file_name + '_vad_output.txt'
    path_to_save_log_file = path_to_save_log_file + txt_file_name

    main(args=args, dir_to_save_chunks=dir_to_save_chunks, vad_output_file_path=path_to_save_log_file)

