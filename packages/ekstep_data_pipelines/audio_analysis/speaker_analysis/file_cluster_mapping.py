import json


def save_json(file_path, mappings):
    with open(file_path, "w+") as f:
        json.dump(mappings, f)


def file_to_speaker_map(speaker_to_file_map):
    file_to_speaker = {}
    for speaker in speaker_to_file_map:
        files = speaker_to_file_map.get(speaker)
        for file in files:
            file_name = file.split("/")[-1]
            file_to_speaker[file_name] = speaker
    return file_to_speaker


def speaker_to_file_name_map(speaker_to_file_map):
    speaker_to_utterances = {}
    for speaker in speaker_to_file_map:
        utterances = list(
            map(lambda f: (f[0].split("/")[-1], f[1]), speaker_to_file_map.get(speaker))
        )
        speaker_to_utterances[speaker] = utterances
    return speaker_to_utterances
