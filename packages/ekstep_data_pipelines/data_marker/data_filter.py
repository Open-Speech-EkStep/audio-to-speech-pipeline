import pandas as pd


class DataFilter(object):
    def by_snr(self, utterances, filters):
        return filter(lambda t: filters['lte'] >= t[4] >= filters['gte'], utterances)

    def by_duration(self, utterances, filters):
        duration = 0
        filtered_utterances = []
        for utterance in utterances:
            duration = duration + utterance[2]
            filtered_utterances.append(utterance)
            if duration >= filters['duration']:
                break
        return filtered_utterances

    def by_per_speaker_duration(self, utterances, filters):
        lte_speaker_duration = filters['lte_per_speaker_duration']
        gte_speaker_duration = filters['gte_per_speaker_duration']
        threshold = filters['threshold']

        lower_bound = gte_speaker_duration - threshold
        upper_bound = lte_speaker_duration + threshold

        df = pd.DataFrame(utterances,
                          columns=['speaker_id', 'clipped_utterance_file_name', 'clipped_utterance_duration',
                                   'audio_id', 'snr'])
        df['cum_hours'] = df.groupby(['speaker_id'])['clipped_utterance_duration'].cumsum()
        df = df[df['speaker_id'].isin(list(df[(df.cum_hours <= upper_bound) & (df.cum_hours >= lower_bound)]['speaker_id']))]
        df = df[(df.cum_hours <= upper_bound)]\
            .drop(columns='cum_hours')
        return list(df.to_records(index=False))
