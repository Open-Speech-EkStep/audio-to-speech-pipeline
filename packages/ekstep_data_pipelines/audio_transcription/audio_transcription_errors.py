class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class TranscriptionSanitizationError(Error):
    """Exception raised for errors in the input.
    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, error):
        self.error = error
