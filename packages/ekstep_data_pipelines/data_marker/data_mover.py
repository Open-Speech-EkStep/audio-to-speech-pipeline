import multiprocessing

from common.utils import get_logger

from concurrent.futures import ThreadPoolExecutor
Logger = get_logger("MediaFilesMover")


class MediaFilesMover(object):
    def __init__(self, file_system, concurrency):
        self.file_system = file_system
        self.concurrency = concurrency

    def move_media_files(self, files, landing_base_path):
        worker_pool = ThreadPoolExecutor(max_workers=self.concurrency)
        for file in files:
            file_relative_path = '/'.join(file.split('/')[-3:])
            landing_path = f'{landing_base_path}/{file_relative_path}'
            worker_pool.submit(self.file_system.mv_file, file, landing_path)
        worker_pool.shutdown(wait=True)
