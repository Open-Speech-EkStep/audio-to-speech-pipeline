import multiprocessing

from common.utils import get_logger

from concurrent.futures import ThreadPoolExecutor
Logger = get_logger("MediaFilesMover")


class MediaFilesMover(object):
    def __init__(self, file_system):
        self.file_system = file_system

    def move_media_files(self, source_base_path, landing_base_path, audio_ids):
        workers = multiprocessing.cpu_count() / .2
        worker_pool = ThreadPoolExecutor(max_workers=workers)
        for audio_id in audio_ids:
            clean_path = f'{source_base_path}/{audio_id}/clean'
            rejected_path = f'{source_base_path}/{audio_id}/rejected'
            landing_path = f'{landing_base_path}/{audio_id}'
            worker_pool.submit(self.file_system.mv, clean_path, landing_path)
            worker_pool.submit(self.file_system.mv, rejected_path, landing_path)
        worker_pool.shutdown(wait=True)
