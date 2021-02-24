import pandas as pd

from ekstep_data_pipelines.common.utils import get_logger

Logger = get_logger("DataFilter")


class DataFilter(object):
    def exclude_audio_ids(self, utterances, audio_ids):
        excluding_audio_ids = filter(lambda t: t[0] not in audio_ids, utterances)
        return excluding_audio_ids

    def exclude_speaker_ids(self, utterances, speaker_ids):
        excluding_speaker_ids = filter(lambda t: t[0] not in speaker_ids, utterances)
        return excluding_speaker_ids

    def by_utterance_duration(self, utterances, filters):
        by_utterance_duration = filter(
            lambda t: filters["lte"] >= t[2] >= filters["gte"], utterances
        )
        return by_utterance_duration

    def by_snr(self, utterances, filters):
        by_snr_utterances = filter(
            lambda t: filters["lte"] >= t[4] >= filters["gte"], utterances
        )
        return by_snr_utterances

    def by_duration(
        self,
        utterances,
        total_duration_in_hrs,
        with_randomness="false",
        with_fraction=1,
    ):
        df = self.to_df(utterances)
        if with_randomness == "true":
            Logger.info("applying randomness")
            df = df.sample(frac=with_fraction)
        df["cum_hours"] = df["clipped_utterance_duration"].cumsum()
        df = df[(df.cum_hours <= total_duration_in_hrs * 3600)].drop(
            columns="cum_hours"
        )
        return self.to_tuples(df)

    def to_df(self, utterances):
        return pd.DataFrame(
            utterances,
            columns=[
                "speaker_id",
                "clipped_utterance_file_name",
                "clipped_utterance_duration",
                "audio_id",
                "snr",
            ],
        )

    def to_tuples(self, df):
        return [tuple(x) for x in df.to_records(index=False)]

    def by_per_speaker_duration(self, utterances, filters):
        lte_speaker_duration = filters["lte_per_speaker_duration"] * 60
        gte_speaker_duration = filters["gte_per_speaker_duration"] * 60
        threshold = filters["with_threshold"] * 60

        lower_bound = gte_speaker_duration - threshold
        upper_bound = lte_speaker_duration + threshold

        df = self.to_df(utterances)
        # df = df[df['speaker_id' is not None]]
        df["cum_hours"] = df.groupby(["speaker_id"])[
            "clipped_utterance_duration"
        ].cumsum()
        df = df[
            df["speaker_id"].isin(
                list(
                    df[(df.cum_hours <= upper_bound) & (df.cum_hours >= lower_bound)][
                        "speaker_id"
                    ]
                )
            )
        ]
        df = df[(df.cum_hours <= upper_bound)].drop(columns="cum_hours")
        return self.to_tuples(df)

    def apply_filters(self, filters, utterances):
        Logger.info("Applying filters:" + str(filters))
        by_utterance_duration = filters.get("by_utterance_duration", None)
        by_snr = filters.get("by_snr", None)
        by_speaker = filters.get("by_speaker", None)
        by_duration = filters.get("by_duration", None)
        with_randomness = filters.get("with_randomness", "false")
        with_fraction = filters.get("with_fraction", 1)
        exclude_audio_ids = filters.get("exclude_audio_ids", [])
        exclude_speaker_ids = filters.get("exclude_speaker_ids", [])

        filtered_utterances = utterances
        if len(utterances) <= 0:
            return []

        if len(exclude_speaker_ids) > 0:
            Logger.info("Excluding audio_ids :" + str(exclude_speaker_ids))
            filtered_utterances = self.exclude_speaker_ids(
                utterances, exclude_speaker_ids
            )

        if len(exclude_audio_ids) > 0:
            Logger.info("Excluding audio_ids: " + str(exclude_audio_ids))
            filtered_utterances = self.exclude_audio_ids(utterances, exclude_audio_ids)

        if by_utterance_duration is not None:
            Logger.info(
                "Filtering by_utterance_duration: " + str(by_utterance_duration)
            )
            filtered_utterances = self.by_utterance_duration(
                filtered_utterances, by_utterance_duration
            )
        if by_snr is not None:
            Logger.info("Filtering by snr:" + str(by_snr))
            filtered_utterances = self.by_snr(filtered_utterances, by_snr)
            
        if by_speaker is not None:
            Logger.info("Filtering by speaker:" + str(by_speaker))
            filtered_utterances = self.by_per_speaker_duration(
                filtered_utterances, by_speaker
            )

        if by_duration is not None:
            Logger.info("Filtering by duration: " + str(by_duration))
            filtered_utterances = self.by_duration(
                filtered_utterances, by_duration, with_randomness, with_fraction
            )

        return list(filtered_utterances)
