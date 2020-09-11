
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