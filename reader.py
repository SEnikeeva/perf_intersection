import logging
import re

from dateutil import parser

from helpers.utils import is_contains, to_dict
from services.read_service import read_df, processing_df


def model_reader(path):
    def parse_date(part_):
        date_str = part_[part_.find('\n') + 1:part_.find('/')]
        return parser.parse(date_str)

    def parse_intervals(part_):
        int_idxs = [m.start() for m in re.finditer('COMPDATMD', part_)]
        intervals_ = {}
        if len(int_idxs) > 0:
            int_idxs.append(len(part_))
            for idx in range(len(int_idxs) - 1):
                int_data = part_[int_idxs[idx]: int_idxs[idx + 1]].split('\n')
                start = False
                j = 0
                while j < len(int_data):
                    if int_data[j].strip().startswith('--'):
                        start = True
                        j += 1
                        continue
                    if start:
                        if int_data[j].strip() == '/':
                            break
                        else:
                            int_data_j = int_data[j].strip().split()[:6]
                            if len(int_data_j) < 6:
                                j += 1
                                continue
                            if intervals_.get(int_data_j[0][1:-1]) is None:
                                intervals_[int_data_j[0][1:-1]] = int_data_j[1:]
                            else:
                                intervals_[int_data_j[0][1:-1]].append(int_data_j[1:])
                    j += 1
            return intervals_

        else:
            return []

    with open(path, 'r') as f:
        perf_data = f.read()
    date_idxs = [m.start() for m in re.finditer('DATES', perf_data)] + [len(perf_data)]
    model_perf = {}
    for i in range(len(date_idxs) - 1):
        part = perf_data[date_idxs[i]: date_idxs[i + 1]]
        date = parse_date(part)
        intervals = parse_intervals(part)
        model_perf[date] = intervals
    return model_perf


def base_reader(path):
    perf_df = read_df(path)
    perf_df = processing_df(perf_df)
    fields = ['well', 'type_perf', 'type', 'date', 'top', 'bot', 'layer']
    for f in fields:
        if f not in perf_df.columns:
            logging.warning(f"в перфорациях из АРМИТЦ не указано поле: {f}")
            perf_df[f] = -1
            perf_df[f] = perf_df[f].astype(int)

    try:
        perf_df['date'] = perf_df['date'].dt.date
    except AttributeError:
        perf_df['date'] = perf_df['date'].apply(
            lambda str_date: parser.parse(str_date).date())
    perf_df['year'] = perf_df['date'].apply(lambda x: x.year)
    perf_df.sort_values(by=['well', 'date'], ascending=True,
                        inplace=True, kind='mergesort')
    perf_df.reset_index(drop=True, inplace=True)
    perf_df = perf_df[::-1]
    perf_df.reset_index(drop=True, inplace=True)
    # getting the perforation type
    perf_df['type'] = perf_df.apply(
        lambda x: get_type(x['type'], x['type_perf'], x['layer']),
        axis=1)
    perf_df.reset_index(drop=True, inplace=True)
    perf = to_dict(perf_df, ['type', 'date', 'top', 'bot'], 'well')
    return perf


def get_type(type_str, type_perf, layer):
    """
    :param type_str: цель перфорации
    :param type_perf: тип перфорации
    :param layer: название пласта
    :return: 1 - открытый, 0 - закрытый,
     3 - бурение бокового ствола, 2 - тип закрытого, который перекрывает нижележащие,
     4 - заливка цементом

    """
    key_words = {'-1': ['спец', 'наруш', 'циркуляц'],
                 '2': ['ый мост', 'пакером', 'гпш', 'рппк', 'шлипс', 'прк(г)'],
                 'd0': ['d0', 'd_0', 'д0', 'д_0']}
    if (type(type_str) is not str) or \
            is_contains(type_str.lower(), key_words['-1']):
        return -1
    elif 'переход' in type_str.lower():
        return 5
    elif ('отключ' in type_str.lower()) or \
            (('изоляц' in type_str.lower()) and (
                    'раб' in type_str.lower())):
        if (type(type_perf) is not str) or \
                ((type_perf.lower().strip() == 'изоляция пакером') and
                 (layer.lower().strip() in key_words['d0'])):
            return 0
        elif 'заливка цемент' in type_perf.lower():
            return 4
        elif is_contains(type_perf, key_words['2']):
            return 2
        else:
            return 0
    elif ('бок' in type_str.lower()) and ('ств' in type_str.lower()):
        return 3
    return 1