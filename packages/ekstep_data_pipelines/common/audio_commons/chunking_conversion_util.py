import collections
import contextlib
import glob
import os
import subprocess
import sys
import wave

import sox
import webrtcvad
from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("Chunking Util")


class ChunkingConversionUtil:
    re_chunking_aggressiveness = 3

    @staticmethod
    def get_instance():
        return ChunkingConversionUtil()

    def convert_to_wav(self, input_dir, output_dir=None, ext="mp4"):

        Logger.info("Convert all the files in %s to wav", input_dir)
        audio_paths = glob.glob(input_dir + "/*." + ext)

        Logger.info("Files to be completed: %s", audio_paths)

        if len(audio_paths) < 1:
            return None, False

        input_file_name = audio_paths[0]
        output_file_name = input_file_name.split("/")[-1].split(".")[0] + ".wav"

        if output_dir is None:
            output_file_path = (
                "/".join(input_file_name.split("/")[:-1]) + "/" + output_file_name
            )
        else:
            output_file_path = output_dir + "/" + output_file_name

        Logger.info("Output path for converted wav file is:%s", output_file_name)

        if os.path.exists(output_file_path) and os.path.isfile(output_file_path):
            Logger.info(
                "WAV file at %s already exists, not doing anything", output_file_name
            )
            return output_file_path, True

        Logger.info("No file exists on %s, running the command", output_file_name)

        command = (
            f"ffmpeg -i {input_file_name} -ar 16000 -ac 1 -bits_per_raw_sample 16 -vn "
            f"{output_file_path}"
        )
        subprocess.call(command, shell=True)

        Logger.info("No file exists on %s, running the command", output_file_name)
        return output_file_path, True

    def create_audio_clips(
        self,
        aggressiveness,
        max_duration,
        wav_file_path,
        dir_to_save_chunks,
        vad_output_file_path,
        base_chunk_name,
        is_rechunking=True,
    ):
        audio, sample_rate = self.read_wave(wav_file_path)
        vad = webrtcvad.Vad(int(aggressiveness))
        frames = self.frame_generator(30, audio, sample_rate)
        frames = list(frames)
        file = open(vad_output_file_path, "w+")

        segments = self.vad_collector(
            sample_rate, 30, 300, vad, frames, vad_output_file_path, file
        )
        for i, segment in enumerate(segments):
            path = f"{dir_to_save_chunks}/{i}_{base_chunk_name}"
            file.write("\nWriting %s" % (path,))
            file.write("\n")
            self.write_wave(path, segment, sample_rate)

        file.close()

        if is_rechunking:
            self.rechunking_acc_to_duration(
                max_duration, dir_to_save_chunks, vad_output_file_path
            )

    def rechunking_acc_to_duration(
        self, max_duration, dir_of_chunks, vad_output_file_path
    ):

        file_list = glob.glob(dir_of_chunks + "/*.wav")

        for file_path in file_list:

            duration = self.calculate_duration(file_path)

            if duration > max_duration:
                base_chunk_name = file_path.split("/").pop()
                Logger.info(
                    "rechunking of file %s and duration of file is: %s",
                    base_chunk_name,
                    duration,
                )

                self.create_audio_clips(
                    ChunkingConversionUtil.re_chunking_aggressiveness,
                    max_duration,
                    file_path,
                    dir_of_chunks,
                    vad_output_file_path,
                    base_chunk_name,
                    False,
                )
                os.remove(file_path)

    def calculate_duration(self, input_filepath):
        duration = sox.file_info.duration(input_filepath)
        Logger.info(
            "Duration for input_filepath:%s : %s", input_filepath, str(duration)
        )
        return duration

    def read_wave(self, path):
        """Reads a .wav file.
        Takes the path, and returns (PCM audio data, sample rate).
        """
        with contextlib.closing(wave.open(path, "rb")) as wave_file:
            num_channels = wave_file.getnchannels()
            assert num_channels == 1
            sample_width = wave_file.getsampwidth()
            assert sample_width == 2
            sample_rate = wave_file.getframerate()
            assert sample_rate in (8000, 16000, 32000, 48000)
            pcm_data = wave_file.readframes(wave_file.getnframes())
            return pcm_data, sample_rate

    def write_wave(self, path, audio, sample_rate):
        """Writes a .wav file.
        Takes path, PCM audio data, and sample rate.
        """
        with contextlib.closing(wave.open(path, "wb")) as wave_file:
            wave_file.setnchannels(1)
            wave_file.setsampwidth(2)
            wave_file.setframerate(sample_rate)
            wave_file.writeframes(audio)

    def frame_generator(self, frame_duration_ms, audio, sample_rate):
        """Generates audio frames from PCM audio data.
        Takes the desired frame duration in milliseconds, the PCM data, and
        the sample rate.
        Yields Frames of the requested duration.
        """
        n = int(sample_rate * (frame_duration_ms / 1000.0) * 2)
        offset = 0
        timestamp = 0.0
        duration = (float(n) / sample_rate) / 2.0
        while offset + n < len(audio):
            #         print("offset, offset+n: ", offset, offset+n)
            #         print("timestamp:", timestamp)
            #         print("duration:", duration)
            yield Frame(audio[offset : offset + n], timestamp, duration)
            timestamp += duration
            offset += n

    def vad_collector(
        self,
        sample_rate,
        frame_duration_ms,
        padding_duration_ms,
        vad,
        frames,
        vad_output_file_path,
        file,
    ):
        """Filters out non-voiced audio frames.
        Given a webrtcvad.Vad and a source of audio frames, yields only
        the voiced audio.
        Uses a padded, sliding window algorithm over the audio frames.
        When more than 90% of the frames in the window are voiced (as
        reported by the VAD), the collector triggers and begins yielding
        audio frames. Then the collector waits until 90% of the frames in
        the window are unvoiced to detrigger.
        The window is padded at the front and back to provide a small
        amount of silence or the beginnings/endings of speech around the
        voiced frames.
        Arguments:
        sample_rate - The audio sample rate, in Hz.
        frame_duration_ms - The frame duration in milliseconds.
        padding_duration_ms - The amount to pad the window, in milliseconds.
        vad - An instance of webrtcvad.Vad.
        frames - a source of audio frames (sequence or generator).
        Returns: A generator that yields PCM audio data.
        """
        num_padding_frames = int(padding_duration_ms / frame_duration_ms)
        # We use a deque for our sliding window/ring buffer.
        ring_buffer = collections.deque(maxlen=num_padding_frames)
        # We have two states: TRIGGERED and NOTTRIGGERED. We start in the
        # NOTTRIGGERED state.
        triggered = False

        voiced_frames = []
        for frame in frames:
            is_speech = vad.is_speech(frame.bytes, sample_rate)

            sys.stdout.write("1" if is_speech else "0")

            if not triggered:
                ring_buffer.append((frame, is_speech))
                num_voiced = len([f for f, speech in ring_buffer if speech])
                # If we're NOTTRIGGERED and more than 90% of the frames in
                # the ring buffer are voiced frames, then enter the
                # TRIGGERED state.
                if num_voiced > 0.9 * ring_buffer.maxlen:
                    triggered = True
                    sys.stdout.write("+(%s)" % (ring_buffer[0][0].timestamp,))
                    file.write("+(%s)" % (ring_buffer[0][0].timestamp,))

                    # We want to yield all the audio we see from now until
                    # we are NOTTRIGGERED, but we have to start with the
                    # audio that's already in the ring buffer.
                    for f, s in ring_buffer:
                        voiced_frames.append(f)
                    ring_buffer.clear()
            else:
                # We're in the TRIGGERED state, so collect the audio data
                # and add it to the ring buffer.
                voiced_frames.append(frame)
                ring_buffer.append((frame, is_speech))
                num_unvoiced = len([f for f, speech in ring_buffer if not speech])
                # If more than 90% of the frames in the ring buffer are
                # unvoiced, then enter NOTTRIGGERED and yield whatever
                # audio we've collected.
                if num_unvoiced > 0.9 * ring_buffer.maxlen:
                    sys.stdout.write("-(%s)" % (frame.timestamp + frame.duration))
                    file.write("-(%s)" % (frame.timestamp + frame.duration))

                    # file.write('\n')
                    triggered = False
                    yield b"".join([f.bytes for f in voiced_frames])
                    ring_buffer.clear()
                    voiced_frames = []
        # W0631: Using possibly undefined loop variable 'frame' (undefined-loop-variable)
        # out of scope frame loop variable
        if triggered:
            sys.stdout.write("-(%s)" % (frame.timestamp + frame.duration))
            file.write("-(%s)" % (frame.timestamp + frame.duration))
        sys.stdout.write("\n")
        # file.write('\n')
        # If we have any leftover voiced audio when we run out of input,
        # yield it.
        if voiced_frames:
            yield b"".join([f.bytes for f in voiced_frames])


class Frame(object):
    """Represents a "frame" of audio data."""

    def __init__(self, bytes, timestamp, duration):
        self.bytes = bytes
        self.timestamp = timestamp
        self.duration = duration
