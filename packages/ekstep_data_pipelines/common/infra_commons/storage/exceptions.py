class FileNotFoundException(Exception):
    def __init__(self, value, *args, **kwargs):
        self.value = value


class PathDoesNotExist(Exception):
    def __init__(self, value, *args, **kwargs):
        self.value = value
