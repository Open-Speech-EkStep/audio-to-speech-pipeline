import re


def extract_transcription(content):
    trimmed = list(map(lambda s: s.strip(),
                       re.split('हैप्पी पोला|हॉपिपोला|हॉपीपोला',
                                ' '.join(list(map(lambda chunk:
                                                  chunk.alternatives[0].transcript, content.results))))))
    return trimmed


def save_transcriptions(input_file_dir, transcriptions, file_name):
    rejected_wavs = []
    for index, transcription in enumerate(transcriptions):
        if transcription:
            transcription_file = '{0}/{1}-{2}.txt'.format(input_file_dir, file_name, index)
            save_file(transcription, transcription_file)
        else:
            rejected_wavs.append('{0}/{1}-{2}.wav'.format(input_file_dir, file_name, index))
    return rejected_wavs


def save_file(transcription, output_file_path):
    with open(output_file_path, "w") as f:
        f.write(transcription)
