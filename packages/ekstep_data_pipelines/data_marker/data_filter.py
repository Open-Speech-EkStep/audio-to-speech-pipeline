import pandas as pd

from common.utils import get_logger

Logger = get_logger("DataFilter")


class DataFilter(object):
    def by_snr(self, utterances, filters):
        by_snr_utterances = filter(lambda t: filters['lte'] >= t[4] >= filters['gte'], utterances)
        return by_snr_utterances

    def by_duration(self, utterances, total_duration, with_randomness=False, with_fraction=1):
        df = self.to_df(utterances)
        if with_randomness:
            df = df.sample(frac=with_fraction)
        df['cum_hours'] = df['clipped_utterance_duration'].cumsum()
        df = df[(df.cum_hours <= total_duration)] \
            .drop(columns='cum_hours')
        return self.to_tuples(df)

    def to_df(self, utterances):
        return pd.DataFrame(utterances,
                            columns=['speaker_id', 'clipped_utterance_file_name', 'clipped_utterance_duration',
                                     'audio_id', 'snr'])

    def to_tuples(self, df):
        return [tuple(x) for x in df.to_records(index=False)]

    def by_per_speaker_duration(self, utterances, filters):
        lte_speaker_duration = filters['lte_per_speaker_duration']
        gte_speaker_duration = filters['gte_per_speaker_duration']
        threshold = filters['with_threshold']
        lower_bound = gte_speaker_duration - threshold
        upper_bound = lte_speaker_duration + threshold

        df = self.to_df(utterances)
        df['cum_hours'] = df.groupby(['speaker_id'])['clipped_utterance_duration'].cumsum()
        df = df[df['speaker_id'].isin(
            list(df[(df.cum_hours <= upper_bound) & (df.cum_hours >= lower_bound)]['speaker_id']))]
        df = df[(df.cum_hours <= upper_bound)] \
            .drop(columns='cum_hours')
        return self.to_tuples(df)

    def apply_filters(self, filters, utterances):
        Logger.info('Applying filters:' + str(filters))
        by_snr = filters.get('then_by_snr', None)
        by_speaker = filters.get('then_by_speaker', None)
        by_duration = filters.get('then_by_duration', None)
        with_randomness = filters.get('with_randomness', False)
        with_fraction = filters.get('with_fraction', 1)

        filtered_utterances = utterances
        if by_snr is not None:
            Logger.info("Filtering by snr:" + str(by_snr))
            filtered_utterances = self.by_snr(utterances, by_snr)

        if by_speaker is not None:
            Logger.info("Filtering by speaker:" + str(by_speaker))
            filtered_utterances = self.by_per_speaker_duration(filtered_utterances, by_speaker)

        if by_duration is not None:
            Logger.info("Filtering by duration:" + str(by_duration))
            filtered_utterances = self.by_duration(filtered_utterances, by_duration, with_randomness, with_fraction)

        return list(filtered_utterances)
