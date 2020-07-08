import re


def time_in_seconds_from_list(list_of_time):
    s = int(list_of_time[0])
    ns_to_s = 0
    if len(list_of_time) == 2:
        ns = int(list_of_time[1])
        ns_to_s = ns / pow(10, 9)

    time_s = s + ns_to_s
    return time_s


def toInt(args):
    return int(args)


def extractTimeFromString(timeString):
    if not timeString:
        return 0
    time_list = re.findall('\d+', timeString)
    time_list_int = list(map(toInt, time_list))

    s = time_list_int[0]
    ns = time_list_int[1]
    ns = ns / pow(10, 9)
    return s+ns

def map_word_to_time(words, wordStartAndEndString):
    wordStartAndEndTimestamp = list(map(extractTimeFromString, wordStartAndEndString))
    final_tuples = []
    for index, time in enumerate(wordStartAndEndTimestamp):
        word_index = index//2
        if index%2 == 0:
            final_tuples.append((words[word_index], time, wordStartAndEndTimestamp[index+1]))
    return final_tuples
