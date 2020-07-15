import glob

import sox

COMMA = ','


def merge_chunks(input_dir, output_dir, voice_separator_audio, file_prefix, merged_file_name='merged.wav'):
    files = glob.glob(input_dir + '/*.wav')
    chunk_files = list(map(lambda i:
                           f'{input_dir}/{file_prefix}-{i}.wav',
                           list(range(0, len(files)))))
    separator = COMMA + voice_separator_audio + COMMA
    files_to_merge = separator.join(list(chunk_files)).split(COMMA)
    print('merging files:' + str(files_to_merge))
    cbn = sox.Combiner()
    cbn.build(files_to_merge, merged_file_name, 'concatenate')
    return merged_file_name
