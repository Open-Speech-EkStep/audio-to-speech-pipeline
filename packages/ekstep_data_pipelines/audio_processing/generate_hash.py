import hashlib

from ekstep_data_pipelines.common.utils import get_logger

LOGGER = get_logger("hash_code")


def get_hash_code_of_audio_file(file_path):

    md5_hash = hashlib.md5()
    audio_file = open(file_path, "rb")
    content = audio_file.read()
    md5_hash.update(content)
    digest = md5_hash.hexdigest()
    LOGGER.info("Given file is %s and hash is %s", file_path, digest)
    return digest
