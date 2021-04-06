from concurrent.futures import ThreadPoolExecutor

from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("MediaFilesMover")


class MediaFilesMover(object):
    def __init__(self, file_system, concurrency):
        self.file_system = file_system
        self.concurrency = concurrency

    def move_media_files(self, files, landing_path_with_source):
        Logger.info("using concurrency:%s", str(self.concurrency))
        worker_pool = ThreadPoolExecutor(max_workers=self.concurrency)
        for file in files:
            relative_audio_id_clean_path = "/".join(file.split("/")[-3:-1])
            landing_path = f"{landing_path_with_source}/{relative_audio_id_clean_path}"
            worker_pool.submit(self.file_system.mv_file, file, landing_path)
        worker_pool.shutdown(wait=True)
