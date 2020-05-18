from clip_audio import ClipAudio
from download import DownloadVideo
from generate_srt import GenerateSRT


if __name__ == "__main__":
    link = ''
    mode = 'video'
    output_video_dir = ''
    filename_prefix = 'custom'
    convert_to_wav = True
    output_wav_dir = None

    obj_video = DownloadVideo()

    output_file_paths = obj_video.fit(link, mode='video', output_dir_path = output_video_dir, filename_prefix = filename_prefix, convert_to_wav = convert_to_wav, output_wav_dir = output_wav_dir)

    bin_size = 10
    input_file_path= ''
    output_file_path = None
    dump_response = False
    dump_response_directory = None



    obj_srt = GenerateSRT('hi')

    srt_path = obj_srt.fit_single(bin_size=bin_size, input_file_path=output_file_paths[0], dump_response=True)

    srt_file_path = srt_path
    audio_file_path = output_file_paths[0]
    output_file_dir = ''
    
    obj_clip_audio = ClipAudio()

    obj_clip_audio.fit_single(srt_file_path, audio_file_path, output_file_dir)




