import glob
import os
import subprocess


def convert_to_wav(input_dir, output_dir=None, ext='mp4'):
    print("****input_dir in convert to wav**** ", input_dir)
    audio_paths = glob.glob(input_dir + '/*.' + ext)
    print("****audio_paths in convert to wav**** ", audio_paths)
    file = audio_paths[0]
    print("File to be converted to wav format: {}".format(file))
    input_file_name = file
    output_file_name = file.split('/')[-1].split('.')[0] + '.wav'
    print("****output_dir in convert to wav**** ", output_dir)
    if output_dir is None:
        output_file_path = "/".join(file.split('/')[:-1]) + '/' + output_file_name
    else:
        output_file_path = output_dir + '/' + output_file_name

    print("Output path for converted wav file is: {}".format((output_file_name)))
    if (os.path.exists(output_file_path) and os.path.isfile(output_file_path)):
        print("Wav file already exists...")
    else:
        command = f"ffmpeg -i {input_file_name} -ar 16000 -ac 1 -bits_per_raw_sample 16 -vn {output_file_path}"
        subprocess.call(command, shell=True)

        print("File converted to wav format successfully...")
    return output_file_path
