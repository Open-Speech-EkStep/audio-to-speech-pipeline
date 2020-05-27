import unittest
import sys

# sys.path.append("../../src")
sys.path.append("packages/datacollector_youtube/src")
from packages.datacollector_youtube.src.scripts.download import DownloadVideo

class DownloadVideoTest(unittest.TestCase):

    def test_generate_unique_filename(self):
        dwn = DownloadVideo()
        self.assertEqual(dwn.generate_unique_filename('a')[-1], "a")


if __name__ == '__main__':
    unittest.main()
