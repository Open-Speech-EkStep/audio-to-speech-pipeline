from google.cloud import storage
from google.cloud import speech_v1
from google.cloud.speech_v1 import enums
from google.cloud.speech_v1 import types

import srt
import glob
from tqdm import tqdm
import datetime
import pickle
import numpy as np
import os
import subprocess
from .gcs_operations import CloudStorageOperations


class GenerateSRT(object):
    def __init__(self, language, sample_rate = 16000, audio_channel_count=1):
        if language == 'hi':
            self.language = 'hi-IN'
        self.sample_rate = sample_rate
        self.channels = audio_channel_count

    def convert_to_wav(self, input_dir, output_dir=None, ext='mp4'):
        audio_paths = glob.glob( input_dir + '/*.' + ext )
        output_file_paths = []

        for file in audio_paths:
            print("File to be converted to wav format: {}".format(file))
            input_file_name = file
            output_file_name = file.split('/')[-1].split('.')[0] + '.wav'

            if output_dir is None:
                output_file_path = "/".join(file.split('/')[:-1]) + '/' + output_file_name
            else:
                output_file_path = output_dir + '/' + output_file_name

            print("Output path for converted wav file is: {}".format(os.path.join(output_file_path,output_file_name)))
            if(os.path.exists(output_file_path) and os.path.isfile(output_file_path)):
                print("Wav file already exists...")
            else:
                command = f"ffmpeg -i {input_file_name} -ar 16000 -ac 1 -bits_per_raw_sample 16 -vn {output_file_path}"
                subprocess.call(command, shell=True)

                print("File converted to wav format successfully...")
            output_file_paths.append(output_file_path)

        return output_file_paths

    def fit_single(self, bin_size, input_file_path,bucket_name,audio_extn,output_file_path = None,  dump_response = False , dump_response_directory=None ):

        print("Processing single audio file...")
        print("Initiating input raw file conversion to wav format...")
        # srt_output_file_paths = []
        current_working_directory = os.getcwd()
        if output_file_path is None:
            output_file_paths = self.convert_to_wav(os.path.join(current_working_directory,input_file_path), output_file_path, audio_extn)
        else:
            output_file_paths = self.convert_to_wav(os.path.join(current_working_directory, input_file_path),
                                                    os.path.join(current_working_directory, output_file_path),
                                                    audio_extn)

        # Upload converted wav file to GCS
        print("List of wav files to be processed...")
        print(output_file_paths)

        obj_gcs = CloudStorageOperations()
        print("Initiating wav file upload to google cloud storage for Speech to Text API usage")
        for filepath in output_file_paths:
            file_name = filepath.split('/')[-1].split('.')[0]
            obj_gcs.upload_to_gcs(bucket_name,filepath,os.path.join(input_file_path,file_name)+".wav",is_directory=False)

            print("Initiating conversion of wav file to text using speech to text API")
            response = self.call_speech_to_text(os.path.join("gs://",bucket_name,input_file_path,file_name + ".wav"))

            print("File converted to text successfully using Speech to Text API")
            # Generate name for srt file
            if output_file_path is None:
                # output_file_path = "/".join(input_file_path.split('/')[:-1]) + '/' + file_name + '.srt'
                output_file_path = os.path.join(input_file_path,file_name + '.srt')

            print("Flag value for dumping Speech to Text API response is: ", str(dump_response))
            if dump_response:
                if dump_response_directory is None:
                    dump_response_directory = "/".join(input_file_path.split('/')[:-1]) + '/api-response-dump'

                print("Directory for dumping Speech to Text API response is: ", str(dump_response_directory))
                # Create API dump resposne directory if not exists
                obj_gcs.make_directories(dump_response_directory)

                with open( dump_response_directory + '/' + file_name + '.txt',"wb") as file:
                    print('Dumping Speech to Text API response')
                    pickle.dump(response, file)

            # Generate subtitles
            print("Generating subtitle file...")
            subtitles = self.subtitle_generation(response, bin_size)
            print("Subtitle file generated successfully...")

            # Save subtitle file
            print("Saving subtitle file...")
            self.save_subtitle_file(subtitles, os.path.join(current_working_directory,output_file_path))
            print("Subtitle file saved successfully at: {}".format(os.path.join(current_working_directory,output_file_path)))
            # srt_output_file_paths.append(output_file_path)

            return output_file_path


    def fit_dir(self, bin_size, input_file_dir, output_file_dir = None, dump_response = False, dump_response_directory = None):
        #filenames = glob.glob(input_file_dir+'/*.wav')
        print('Inside dir function')
        print("Response is ", str(dump_response))
        print(" Directory is ", str(dump_response_directory))

        output_file_paths = []
        if len(input_file_dir) == 0:
            print('No files to process here')

        for file in tqdm(input_file_dir):
            if output_file_dir is None:
                local_path = self.fit_single(bin_size, file, dump_response=dump_response, dump_response_directory=dump_response_directory)
                output_file_paths.append(local_path)

            else:
                video_name = file.split('/')[-1].split('.')[0]
                output_file_name = output_file_dir + '/' + video_name + '.srt'
            
                local_path = self.fit_single(bin_size=bin_size, input_file_path = file, output_file_path = output_file_name, dum_response = dump_response, dum_response_directory =dump_response_directory)
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
            "enable_automatic_punctuation":False
        }

        print("Speech to Text API config to be used: {}".format(config))
        print("Source file path on GCS to be converted to text using API: {}".format(input_file_path))
        # bucket_name = 'gs://ekstepspeechrecognition-dev/'+ '/'.join(input_file_path.split('/')[4:])
        # print(bucket_name)

        audio = {"uri": input_file_path}
        operation = client.long_running_recognize(config, audio)

        print(u"Waiting for Speech to Text API operation to complete...")
        response = operation.result()
        print("Speech to Text operation completed successfully")
        return response


    def subtitle_generation_new(self, response, bin_size=10):
        
        def get_time(sec, nano_sec):
            return sec + nano_sec * 1e-9
        
        class Word(object):
            def __init__(self):
                self.start_sec = ''
                self.start_nano_sec = ''
                #self.start_time = datetime.timedelta(0, self.start_time_sec, int(self.start_time_nano_sec) * 0.001)

                self.end_sec = ''
                self.end_nano_sec = ''
                #self.end_time = datetime.timedelta(0, self.end_time_sec, int(self.end_time_nano_sec) * 0.001)

                self.text = ''

            def print_fn(self):
                self.start_time = datetime.timedelta(0, self.start_sec, int(self.start_nano_sec) * 0.001)
                self.end_time = datetime.timedelta(0, self.end_sec, int(self.end_nano_sec) * 0.001)
                print("\n ", self.start_time, " --> ", self.end_time, '\n', self.text)

        list_words = []

        ## create dictionary 
        for res in response.results:
            for alt in res.alternatives:
                for word in alt.words:
                    word_obj = Word()

                    word_obj.start_sec = word.start_time.seconds
                    word_obj.start_nano_sec = word.start_time.nanos

                    word_obj.end_sec = word.end_time.seconds
                    word_obj.end_nano_sec = word.end_time.nanos


                    word_obj.text = word.word
                    list_words.append(word_obj)

        ## create windows
        
        start_sec = list_words[0].start_sec + list_words[0].start_nano_sec * 1e-9
        end_sec = list_words[len(list_words)-1].end_sec + list_words[len(list_words)-1].end_nano_sec * 1e-9

        windows = {}

        index = 0
        for i in np.arange(start_sec, end_sec, bin_size):
            window_start = i
            window_end = i + bin_size

            local_word_list = []

            for j in list_words:
                local_time_start = get_time(j.start_sec, j.start_nano_sec)
                local_time_end = get_time(j.end_sec, j.end_nano_sec)

                if (local_time_start >= window_start and local_time_end <= window_end) or (local_time_start < window_end and local_time_end > window_end):
                    local_word_list.append(j)

            windows[index] = local_word_list
            index = index + 1

        ## understand what indexes to break on windows
        
        index_to_break = {}
        index = 0

        for key, window in windows.items():
            for ind, word in enumerate(window):
                diff = get_time(word.end_sec, word.end_nano_sec) - get_time(word.start_sec, word.start_nano_sec)
                #print(diff)
                if  diff > 2:
                    #print('here')
                    if key in index_to_break:
                        index_to_break[key].append( ind  )
                    else:
                        index_to_break[key] = [ind]
            index = index + 1
        #return index_to_break
        
        #print(index_to_break)
                    
        ## find values with breaks
        
        new_windows = {}

        for original_key, original_value in windows.items():

            if original_key in index_to_break:
                indexes_to_break = index_to_break[original_key]

                start = 0
                ind = 1
                reached_end = False
                for index in indexes_to_break:

                    ## case for start and end word

                    if index == start:
                        start = index + 1
                        continue

                    elif index == len(original_value) - 1:
                        reached_end = True
                        break;

                    subwindow_1 = original_value[start : index-1]
                    new_windows[original_key + ind *0.1] = subwindow_1
                    start = index + 1

                    ind = ind + 1
        #             else:
        #                 subwindow_1 = original_value[start + 1: index-1]
        #                 new_windows[original_key + ind *0.1] = subwindow_1
        #                 start = index + 1

                if not reached_end:  
                    subwindow_1 = original_value[start : len(original_value)]
                    if len(subwindow_1) > 0:
                        new_windows[original_key + (ind) *0.1] = subwindow_1
                else:
                    subwindow_1 = original_value[start : len(original_value)-1]
                    if len(subwindow_1) > 0:
                        new_windows[original_key + (ind) *0.1] = subwindow_1


            else:
                new_windows[original_key] = original_value
        
        
        ## export to srt
        
        index = 1
        transcriptions = []
        for key, value in new_windows.items():
            start_word_start_time = value[0].start_sec
            last_word_end_time = value[len(value)-1].end_sec

            #print(index)
            #print(f"{start_word_start_time} --> {last_word_end_time}")

            transcript = []
            for val in value:
                transcript.append(val.text)

            #print(" ".join(transcript))
            #print("\n")

            transcriptions.append(srt.Subtitle(index, datetime.timedelta(0, value[0].start_sec, value[0].start_nano_sec * 0.001), datetime.timedelta(0, value[len(value)-1].end_sec, value[len(value)-1].end_nano_sec * 0.001), " ".join(transcript)))
            index = index + 1

        subtitles = srt.compose(transcriptions)
        
        return subtitles
                        
    def subtitle_generation(self, response, bin_size=10):
        """We define a bin of time period to display the words in sync with audio. 
        Here, bin_size = 3 means each bin is of 3 secs. 
        All the words in the interval of 3 secs in result will be grouped togather."""
        transcriptions = []
        index = 0
    
        for result in response.results:
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
                        
                        flag = 0
                        if(word_end_sec - word_start_sec) > 2:
                            word_end_sec = end_sec
                            print(word)
                            flag = 1

                        if word_end_sec < end_sec:
                            transcript = transcript + " " + word
                        else:
                            previous_word_end_sec = result.alternatives[0].words[i].end_time.seconds
                            previous_word_end_microsec = result.alternatives[0].words[i].end_time.nanos * 0.001
                            
                            # append bin transcript
                            transcriptions.append(srt.Subtitle(index, datetime.timedelta(0, start_sec, start_microsec), datetime.timedelta(0, previous_word_end_sec, previous_word_end_microsec), transcript))
                            
                            # reset bin parameters
                            if flag == 1:
                                word_start_sec = result.alternatives[0].words[i + 2].start_time.seconds
                                word_start_microsec = result.alternatives[0].words[i + 2].start_time.nanos * 0.001 # 0.001 to convert nana -> micro
                                word_end_sec = result.alternatives[0].words[i + 2].end_time.seconds
                                word_end_microsec = result.alternatives[0].words[i + 2].end_time.nanos * 0.001
                                
                                start_sec = word_start_sec
                                start_microsec = word_start_microsec
                                end_sec = start_sec + bin_size
                                transcript = result.alternatives[0].words[i + 2].word

                                index += 2
                            
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
        #subtitles = transcriptions
        return subtitles
