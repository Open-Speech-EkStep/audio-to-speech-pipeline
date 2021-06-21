import grpc


class SpeechRecognizerConStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, server, port):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.server = server
        self.port = port
        if self.server is None or self.port is None:
            raise ValueError('Ekstep speech recognize service host or port not provided in config file')
        self.channel = self.set_channel()

    def get_channel(self):
        return self.channel

    def set_channel(self):
        return grpc.insecure_channel(self.server + ':' + self.port)
