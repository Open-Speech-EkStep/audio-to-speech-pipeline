
def save_transcription(transcription, output_file_path):
    with open(output_file_path, "w") as f:
        f.write(transcription)

def save_transcriptions(output_dir, transcriptions, file_name):
    for index, transcription in enumerate(transcriptions):
        transcription_file = '{0}/{1}-{2}.txt'.format(output_dir, file_name, index)
        save_transcription(transcription, transcription_file)
