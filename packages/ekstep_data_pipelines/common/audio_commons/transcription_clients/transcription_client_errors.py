class GoogleTranscriptionClientError(Exception):
    """Exception raised for errors in the input.
    Attributes:
    """

    def __init__(self, rootError):
        self.rootError = rootError


class AzureTranscriptionClientError(Exception):
    """Exception raised for errors in the input.
    Attributes:
    """

    def __init__(self, rootError):
        self.rootError = rootError
