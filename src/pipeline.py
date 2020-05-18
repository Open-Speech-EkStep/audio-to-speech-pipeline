from clip_audio import ClipAudio
from download import DownloadVideo
from generate_srt import GenerateSRT
import os



if __name__ == "__main__":
    link = 'https://www.youtube.com/watch?v=OcJ6KTF7nmc&list=PLmXe0lvPfBbenQJ4qtYXsmwzGlmIiU4nk&index=6'
    mode = 'video'
    output_video_dir = '/home/harveen.chadha/gcmount/data/audiotospeech/raw/landing/hindi/audio/testing'
    filename_prefix = 'testing'
    convert_to_wav = True
    output_wav_dir = None


    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

    obj_video = DownloadVideo()

    output_file_paths = obj_video.fit(link, mode='video', output_dir_path = output_video_dir, filename_prefix = filename_prefix, convert_to_wav = convert_to_wav, output_wav_dir = output_wav_dir)

    print("Video Downloaded")
    print(output_file_paths)

    bin_size = 10
    input_file_path= ''
    output_file_path = None
    dump_response = False
    dump_response_directory = None



    obj_srt = GenerateSRT('hi')

    srt_path = obj_srt.fit_single(bin_size=bin_size, input_file_path=output_file_paths[0], dump_response=True)

    print("SRT File generated")

    srt_file_path = srt_path
    audio_file_path = output_file_paths[0]
    output_file_dir = '/home/harveen.chadha/gcmount/data/audiotospeech/raw/landing/hindi/audio/testing/cutaudio'
    
    obj_clip_audio = ClipAudio()

    obj_clip_audio.fit_single(srt_file_path, audio_file_path, output_file_dir)




