import logging
from dateutil import parser

from helpers.utils import is_contains, to_dict
from services.read_service import read_df, processing_df


def model_reader(path):
    pass


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