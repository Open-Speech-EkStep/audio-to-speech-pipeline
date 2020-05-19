from clip_audio import ClipAudio
from download import DownloadVideo
from generate_srt import GenerateSRT
import os
import time
import yaml
yaml.warnings({'YAMLLoadWarning': False})


class AudioPipeline():
    def __init__(self):
        pass

    def __load_yaml_file(self, path):
        read_dict = {}
        with open(path, 'r') as file:
            read_dict = yaml.load(file)
        return read_dict
    
    def fit(self, yaml_file_path):
        pipeline_start_time = time.time()
        read_dict = self.__load_yaml_file(yaml_file_path)

        args_application = read_dict['application']
        args_downloader = read_dict['downloader']
        args_srtgenerator  = read_dict['srtgenerator']
        args_clipaudio  = read_dict['clipaudio']

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args_application['credential_file_path']
        
       

        obj_video = DownloadVideo()
        output_file_paths = obj_video.fit(args_downloader['link'], 
                                        mode = args_downloader['mode'], 
                                        output_dir_path = args_downloader['output_dir_path'],
                                        filename_prefix = args_downloader['filename_prefix'],
                                        convert_to_wav = args_downloader['convert_to_wav'],
                                        output_wav_dir = args_downloader['output_wav_dir'])

        obj_srt = GenerateSRT(args_application['language'])
        obj_clip_audio = ClipAudio()


        if args_downloader['mode'] == 'video':
            srt_path = obj_srt.fit_single(bin_size = args_srtgenerator['bin_size'],
                                        input_file_path = output_file_paths[0],
                                        output_file_path= args_srtgenerator['output_file_path'],
                                        dump_response= args_srtgenerator['dump_response'],
                                        dump_response_directory = args_srtgenerator['dump_response_directory'])

            
            obj_clip_audio.fit_single(srt_file_path = srt_path, 
                                    audio_file_path = output_file_paths[0],
                                    output_file_dir = args_clipaudio['output_file_dir'])

        else:
            srt_path = obj_srt.fit_dir(bin_size = args_srtgenerator['bin_size'],
                                        input_file_dir = output_file_paths,
                                        output_file_dir = args_srtgenerator['output_file_path'],
                                        dump_response= args_srtgenerator['dump_response'],
                                        dump_response_directory = args_srtgenerator['dump_response_directory'])

            
            obj_clip_audio.fit_dir(srt_dir = srt_path, 
                                    audio_dir = output_file_paths,
                                    output_dir = args_clipaudio['output_file_dir'])

        pipeline_end_time = time.time()
        print("Pipeline took ", pipeline_end_time - pipeline_start_time , " seconds to run!")


            




if __name__ == "__main__":

    obj = AudioPipeline()
    obj.fit('./config.yaml')
    # link = 'https://www.youtube.com/watch?v=OcJ6KTF7nmc&list=PLmXe0lvPfBbenQJ4qtYXsmwzGlmIiU4nk&index=6'
    # mode = 'video'
    # output_video_dir = '/home/harveen.chadha/gcmount/data/audiotospeech/raw/landing/hindi/audio/testing'
    # filename_prefix = 'testing'
    # convert_to_wav = True
    # output_wav_dir = None


    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

    # obj_video = DownloadVideo()

    # output_file_paths = obj_video.fit(link, mode='video', output_dir_path = output_video_dir, filename_prefix = filename_prefix, convert_to_wav = convert_to_wav, output_wav_dir = output_wav_dir)

    # print("Video Downloaded")
    # print(output_file_paths)

    # bin_size = 10
    # input_file_path= ''
    # output_file_path = None
    # dump_response = False
    # dump_response_directory = None



    # obj_srt = GenerateSRT('hi')

    # srt_path = obj_srt.fit_single(bin_size=bin_size, input_file_path=output_file_paths[0], dump_response=True)

    # print("SRT File generated")

    # srt_file_path = srt_path
    # audio_file_path = output_file_paths[0]
    # output_file_dir = '/home/harveen.chadha/gcmount/data/audiotospeech/raw/landing/hindi/audio/testing/cutaudio'
    
    # obj_clip_audio = ClipAudio()

    # obj_clip_audio.fit_single(srt_file_path, audio_file_path, output_file_dir)