from google.cloud import storage
from google.cloud import speech_v1
from google.cloud.speech_v1 import enums
from google.cloud.speech_v1 import types

import srt
import glob
from tqdm import tqdm
import datetime
import pickle

import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credentials.json"

class GenerateSRT(object):
    def __init__(self, language, sample_rate = 16000, audio_channel_count=1):
        if language == 'hi':
            self.language = 'hi-IN'
        self.sample_rate = sample_rate
        self.channels = audio_channel_count

    def fit_single(self, bin_size, input_file_path, output_file_path = None, dump_response = False, dump_response_directory = None):

        if input_file_path[-3:] != 'wav':
            print('Enter a valid wav file')
        
        video_name = input_file_path.split('/')[-1].split('.')[0]
        if output_file_path is None:
            output_file_path = "/".join( input_file_path.split('/')[:-1] ) + '/' + video_name + '.srt'
        

        response = self.call_speech_to_text(input_file_path)

        if dump_response:
            if dump_response_directory is None:
                dump_response_directory = './'
            
            with open( dump_response_directory + video_name + '.txt',"wb") as file:
                pickle.dump(response, file)

        subtitles = self.subtitle_generation(response)

        self.save_subtitle_file(subtitles, output_file_path)
        return output_file_path


    def fit_dir(self, bin_size, input_file_dir, output_file_dir = None, dump_response = False, dump_response_directory = None):
        #filenames = glob.glob(input_file_dir+'/*.wav')

        output_file_paths = []
        if len(input_file_dir) == 0:
            print('No files to process here')

        for file in tqdm(input_file_dir):
            if output_file_dir is None:
                local_path = self.fit_single(bin_size, file)
                output_file_paths.append(local_path)

            else:
                video_name = file.split('/')[-1].split('.')[0]
                output_file_name = output_file_dir + '/' + video_name + '.srt'
            
                local_path = self.fit_single(bin_size, file, output_file_name)
                output_file_paths.append(local_path)

        return output_file_paths




    def save_subtitle_file(self, subtitles, output_file_path):
        with open(output_file_path, "w") as f:
            f.write(subtitles)

    def call_speech_to_text(self, input_file_path):
        client = speech_v1.SpeechClient()

        config = {
            "language_code": self.language,
            "sample_rate_hertz": self.sample_rate,
            "encoding": enums.RecognitionConfig.AudioEncoding.LINEAR16,
            "audio_channel_count": self.channels,

            "enable_word_time_offsets": True,
            "enable_automatic_punctuation":True
        }

        print(config)
        print(input_file_path)
        bucket_name = 'gs://ekstepspeechrecognition-dev/'+ '/'.join(input_file_path.split('/')[4:])
        print(bucket_name)
        audio = {"uri": bucket_name}

        operation = client.long_running_recognize(config, audio)

        print(u"Waiting for operation to complete...")
        response = operation.result()
        return response

    def subtitle_generation(self, speech_to_text_response, bin_size=10):
        """We define a bin of time period to display the words in sync with audio. 
        Here, bin_size = 3 means each bin is of 3 secs. 
        All the words in the interval of 3 secs in result will be grouped togather."""

        transcriptions = []
        index = 0
    
        for result in speech_to_text_response.results:
            try:
                if result.alternatives[0].words[0].start_time.seconds:
                    # bin start -> for first word of result
                    start_sec = result.alternatives[0].words[0].start_time.seconds 
                    start_microsec = result.alternatives[0].words[0].start_time.nanos * 0.001
                else:
                    # bin start -> For First word of response
                    start_sec = 0
                    start_microsec = 0 
                    
                #bin_size = result.alternatives[0].words[0].word.index('।')
                end_sec = start_sec + bin_size # bin end sec
                
                # for last word of result
                last_word_end_sec = result.alternatives[0].words[-1].end_time.seconds
                last_word_end_microsec = result.alternatives[0].words[-1].end_time.nanos * 0.001
                
                # bin transcript
                transcript = result.alternatives[0].words[0].word
                
                index += 1 # subtitle index

                for i in range(len(result.alternatives[0].words) - 1):
                    try:
                        word = result.alternatives[0].words[i + 1].word
                        word_start_sec = result.alternatives[0].words[i + 1].start_time.seconds
                        word_start_microsec = result.alternatives[0].words[i + 1].start_time.nanos * 0.001 # 0.001 to convert nana -> micro
                        word_end_sec = result.alternatives[0].words[i + 1].end_time.seconds
                        word_end_microsec = result.alternatives[0].words[i + 1].end_time.nanos * 0.001

                        if word_end_sec < end_sec:
                            transcript = transcript + " " + word
                        else:
                            previous_word_end_sec = result.alternatives[0].words[i].end_time.seconds
                            previous_word_end_microsec = result.alternatives[0].words[i].end_time.nanos * 0.001
                            
                            # append bin transcript
                            transcriptions.append(srt.Subtitle(index, datetime.timedelta(0, start_sec, start_microsec), datetime.timedelta(0, previous_word_end_sec, previous_word_end_microsec), transcript))
                            
                            # reset bin parameters
                            start_sec = word_start_sec
                            start_microsec = word_start_microsec
                            end_sec = start_sec + bin_size
                            transcript = result.alternatives[0].words[i + 1].word
                            
                            index += 1
                    except IndexError:
                        pass
                # append transcript of last transcript in bin
                transcriptions.append(srt.Subtitle(index, datetime.timedelta(0, start_sec, start_microsec), datetime.timedelta(0, last_word_end_sec, last_word_end_microsec), transcript))
                index += 1
            except IndexError:
                pass
        
        # turn transcription list into subtitles
        subtitles = srt.compose(transcriptions)
        return subtitles