import os
import shutil

# import sys
import time
from datetime import datetime  # , timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
import logging

import ffmpeg
import m3u8

# from botocore import UNSIGNED
# from botocore.config import Config
from pytz import timezone

from . import datetime_utils, s3_utils, scraper


def get_readable_clipname(hydrophone_id, cliptime_utc):
    # cliptime is of the form 2020-09-27T00/16/55.677242Z
    cliptime_utc = timezone("UTC").localize(cliptime_utc)
    date = cliptime_utc.astimezone(timezone("US/Pacific"))
    date_format = "%Y_%m_%d_%H_%M_%S_%Z"
    clipname = date.strftime(date_format)
    return hydrophone_id + "_" + clipname, date


# TODO: Handle date ranges that don't exist
class DateRangeHLSStream:
    """
    stream_base = 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab' # noqa
    polling_interval = 60 sec
    start_unix_time
    end_unix_time
    wav_dir
    overwrite_output: allows ffmpeg to overwrite output, default is False
    quiet_ffmpeg: Passed to ffmpeg.run quiet argument. Set true to print ffmpeg logs to stdout and stderr
    real_time: if False, get data as soon as possible, if true wait for
                polling interval before pulling
    """

    def __init__(
        self,
        stream_base,
        polling_interval,
        start_unix_time,
        end_unix_time,
        wav_dir,
        overwrite_output=False,
        quiet_ffmpeg=False,
        real_time=False,
    ):
        """ """

        self.logger = logging.getLogger("DateRangeHLSStream")

        # Get all necessary data and create index
        self.stream_base = stream_base
        self.polling_interval_in_seconds = polling_interval
        self.start_unix_time = start_unix_time
        self.end_unix_time = end_unix_time
        self.wav_dir = wav_dir
        self.overwrite_output = overwrite_output
        self.real_time = real_time
        self.is_end_of_stream = False
        self.quiet_ffmpeg = quiet_ffmpeg

        # Create wav dir if necessary
        Path(self.wav_dir).mkdir(parents=True, exist_ok=True)

        # query the stream base for all m3u8 files between the timestamps

        # split the stream base into bucket and folder
        # eg.
        # 'https://s3-us-west-2.amazonaws.com/streaming-orcasound-net/rpi_orcasound_lab' # noqa
        # would be split into s3_bucket = 'streaming-orcasound-net' and
        # folder_name = 'rpi_orcasound_lab'

        bucket_folder = self.stream_base.split(
            "https://s3-us-west-2.amazonaws.com/"
        )[1]
        tokens = bucket_folder.split("/")
        self.s3_bucket = tokens[0]
        self.folder_name = tokens[1]
        prefix = self.folder_name + "/hls/"

        # returns folder names corresponding to epochs, this grows as more
        # data is added, we should probably maintain a list of
        # hydrophone folders that exist
        all_hydrophone_folders = s3_utils.get_all_folders(
            self.s3_bucket, prefix=prefix
        )
        self.logger.info(
            "Found {} folders in all for hydrophone".format(
                len(all_hydrophone_folders)
            )
        )

        # Get folders
        self.valid_folders = s3_utils.get_folders_between_timestamp(
            all_hydrophone_folders, self.start_unix_time, self.end_unix_time
        )
        self.logger.info("Found {} folders in date range".format(len(self.valid_folders)))
        self.logger.debug(f"Folders found: {self.valid_folders}")
        self.valid_folder_iter = iter(self.valid_folders)

        # Init variables
        self.current_segment = None
        self.get_next_folder()
        self.current_clip_duration = 0
        self.current_time = int(self.current_folder)

    def get_next_folder(self) -> None:
        """
        Get the next folder in range. Sets the start timestamp and segments. Raises StopIteration if at last folder.
        """

        # Update timestamp
        self.current_folder = next(self.valid_folder_iter)
        self.current_time = int(self.current_folder)

        # Load m3u8 file
        stream_url = "{}/hls/{}/live.m3u8".format(
            (self.stream_base), (self.current_folder)
        )
        stream_obj = m3u8.load(stream_url)
        self.current_segments = iter(stream_obj.segments)

    def get_next_file(self):
        """
        Get the next file name. Increments current timestamp. If at end of folder, raise StopIteration
        """
        audio_segment = next(self.current_segments)

        # Increment timestamp
        self.current_time += audio_segment.duration
        self.current_clip_duration += audio_segment.duration

        # Return filename
        base_path = audio_segment.base_uri
        file_name = audio_segment.uri

        return base_path + file_name

    def get_next_clip(self, current_clip_name=None):
        """
        Creates the next wav file and returns filename. Will iterate through ts files
        until either the clip length is at polling duration or the end of the folder is hit.
        """
        # Fast forward to sstart time
        while self.current_time < self.start_unix_time:
            try:
                self.get_next_file()
            except StopIteration:
                self.get_next_folder()

        # Init
        self.current_clip_duration = 0
        clip_start_time = self.current_time
        self.logger.info(f"Creating clip starting at {datetime.fromtimestamp(self.current_time)}")
        self.logger.debug(f"Current folder: {self.current_folder}")

        # if real_time execution mode is specified
        if self.real_time:
            # sleep till enough time has elapsed

            now = datetime.utcnow()
            time_to_sleep = (current_clip_name - now).total_seconds()

            if time_to_sleep < 0:
                self.logger.warning("Issue with timing")

            if time_to_sleep > 0:
                time.sleep(time_to_sleep)

        # Create tmp path to hold .ts segments
        with TemporaryDirectory() as tmp_path:
            # Go until end of folder or end of clip duration
            file_names = []
            while round(self.current_clip_duration) <= self.polling_interval_in_seconds and self.current_time <= self.end_unix_time:
                try:
                    audio_url = self.get_next_file()
                    file_name = audio_url.split("/")[-1]
                except StopIteration:
                    try:
                        self.logger.info(f"At end of folder {self.folder_name}, truncating clip to {self.current_clip_duration}s")
                        self.get_next_folder()
                    except StopIteration:
                        self.logger.warning(f"Folder {self.folder_name} is last in range, stream is ended")
                        self.is_end_of_stream = True
                    finally:
                        break
                try:
                    scraper.download_from_url(audio_url, tmp_path)
                    file_names.append(file_name)
                except Exception:
                    self.logger.warning("Skipping", audio_url, ": error.")
                    break

            # concatentate all .ts files
            self.logger.info(f"Found {len(file_names)} files for duration of {self.current_clip_duration} secs")
            self.logger.debug(f"Files to concat = {file_names}")
            clipname, clip_start_time_formatted = datetime_utils.get_clip_name_from_unix_time(self.folder_name.replace("_", "-"), clip_start_time)
            hls_file = os.path.join(tmp_path, Path(clipname + ".ts"))
            with open(hls_file, "wb") as wfd:
                for f in file_names:
                    with open(os.path.join(tmp_path, f), "rb") as fd:
                        shutil.copyfileobj(fd, wfd)

            # read the concatenated .ts and write to wav
            audio_file = clipname + ".wav"
            wav_file_path = os.path.join(self.wav_dir, audio_file)
            stream = ffmpeg.input(hls_file)
            stream = ffmpeg.output(stream, wav_file_path)
            try:
                ffmpeg.run(
                    stream, overwrite_output=self.overwrite_output, quiet=self.quiet_ffmpeg
                )
            except Exception:
                self.logger.exception(f"Failed to generate file {clipname}")

        # If we're in demo mode, we need to fake timestamps to make it seem
        # like the date range is real-time
        if current_clip_name:
            clipname, _ = get_readable_clipname(
                self.folder_name.replace("_", "-"), current_clip_name
            )

            # rename wav file
            full_new_clip_path = os.path.join(self.wav_dir, clipname + ".wav")
            os.rename(wav_file_path, full_new_clip_path)
            wav_file_path = full_new_clip_path

            # change clip_start_time - this has to be in UTC so that the email
            # can be in PDT
            clip_start_time = current_clip_name.isoformat() + "Z"

        # Update
        if self.current_time >= self.end_unix_time:
            self.is_end_of_stream = True

        # Get new index
        return wav_file_path, clip_start_time_formatted, current_clip_name

    def is_stream_over(self):
        """
        Returns true if stream has be set to over or current time is past the desiignated end time.
        """
        return self.is_end_of_stream or (int(self.current_time) >= int(self.end_unix_time))
