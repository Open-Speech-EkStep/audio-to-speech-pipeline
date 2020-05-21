import unittest
import sys

sys.path.append("../../src")

from scripts.clip_audio import ClipAudio


class TestClipAudio(unittest.TestCase):

    def test_preprocess_srt(self):
        ca = ClipAudio()
        obj = ca.preprocess_srt('test_resources/1.srt')
        self.assertEqual(type(obj), type([1,2,3]))
        self.assertEqual(obj[1].text, 'क्या मुझे मालूम है जिस तरह किसी कुत्ते को खींचा जाता है इसी तरह मेरे को भी जीने से खींचते वक्त पर डाल दिया\n')




if __name__ == '__main__':
    unittest.main()
