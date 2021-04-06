class GoogleTranscriptionClientError(Exception):
    """Exception raised for errors in the input.
    Attributes:
    """

    def __init__(self, root_error):
        self.rootError = root_error


class AzureTranscriptionClientError(Exception):
    """Exception raised for errors in the input.
    Attributes:
    """

    def __init__(self, root_error):
        self.rootError = root_error
