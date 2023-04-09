import logging

import pandas as pd
from tqdm import tqdm


# бинарный поиск индекса для вставки элемента в отсортированный массив
def bisect_left(a, x, lo=0, hi=None, param='bot'):
    if lo < 0:
        raise ValueError('lo must be non-negative')
    if hi is None:
        hi = len(a)
    while lo < hi:
        mid = (lo + hi) // 2
        if a[mid][param] < x:
            lo = mid + 1
        else:
            hi = mid
    return lo


def get_intervals_df(perf_ints, dates):
    intervals_df = []
    for well in tqdm(perf_ints.keys()):
        perfs, all_dates = get_perf(perf_ints, well, dates[0])
        if len(all_dates) == 0:
            continue
        else:
            prev_perfs = perfs
            if len(prev_perfs) > 0:
                intervals_df.extend(prev_perfs)
            for date in dates[1:]:
                if dates[0] > all_dates[0]:
                    perfs = prev_perfs
                else:
                    next_date = None
                    for i, d in enumerate(all_dates[1:]):
                        if dates[0] > d:
                            next_date = all_dates[i - 1]
                            break
                    if (next_date is not None) and (date < next_date):
                        perfs = prev_perfs
                    else:
                        perfs, dates_perf = get_perf(perf_ints, well, date)
                if len(perfs) > 0:
                    intervals_df.extend(perfs)
    return pd.DataFrame(intervals_df)


def get_perf(perf_ints, well, date):
    act_perf_well = list()
    height = 9999
    sb = False
    idx = 0
    if perf_ints.get(well) is None:
        logging.warning(f"Нет данных по перфорациям для скважины {well}")
        return [], []
    dates = []
    for row in perf_ints[well]:
        dates.append(row['date'])
        if row['date'] > date:
            idx += 1
            continue
        top = row['top']
        bot = row['bot']
        perf_type = row['type']
        idx_t = bisect_left(act_perf_well, top)
        if sb:
            # если встречается боковой ствол, вносится информация по нему,
            # все предыдущие (по дате) интервалы закрываются.
            if perf_type != 3:
                break
            else:
                perf_type = 1
        elif perf_type == 3:
            sb = True
            perf_type = 1
        # обработка случая с пакером
        if (top >= height) and (perf_type in [1, 3]):
            idx += 1
            continue
        else:
            if (bot > height) and (perf_type in [1, 3]):
                bot = height
        # обработка случая при заливке цементом
        if perf_type == 4:
            for i in range(idx, len(perf_ints[well])):
                top2 = perf_ints[well][i]['top']
                bot2 = perf_ints[well][i]['bot']
                if (top2 == top) and (bot < bot2) and ((bot2 - top2) * 0.8 <= (bot - top)):
                    bot = bot2
            perf_type = 0
        if perf_type == 2:
            if (idx_t < len(act_perf_well)) \
                    and (act_perf_well[idx_t]['top'] <= top) \
                    and (act_perf_well[idx_t]['bot'] >= bot) \
                    and (act_perf_well[idx_t]['perf_type'] == 1):
                perf_type = 0
            else:
                height = top
                perf_type = 0
        idx_t = bisect_left(act_perf_well, top)
        if idx_t == len(act_perf_well):
            act_perf_well.append({'well': well, 'top': top, 'bot': bot,
                                  'perf_type': perf_type, 'date': date})
        else:
            shift = 0
            if act_perf_well[idx_t]['top'] > top:
                if act_perf_well[idx_t]['perf_type'] == perf_type:
                    if act_perf_well[idx_t]['top'] < bot:
                        act_perf_well[idx_t]['top'] = top
                    else:
                        act_perf_well.insert(idx_t,
                                             {'well': well, 'top': top,
                                              'bot': bot,
                                              'perf_type': perf_type, 'date': date})
                        shift += 1
                else:
                    act_perf_well.insert(idx_t,
                                         {'well': well, 'top': top,
                                          'bot': bot if
                                          act_perf_well[idx_t]['top'] > bot else
                                          act_perf_well[idx_t]['top'],
                                          'perf_type': perf_type, 'date': date})
                    shift += 1
            idx_b = bisect_left(act_perf_well, bot, param='top')
            idx_t += shift
            to_append = []
            shift = 1
            if act_perf_well[idx_t]['bot'] < bot:
                end = idx_b if idx_b < len(act_perf_well) else len(act_perf_well)
                for i in range(idx_t, end):
                    if act_perf_well[i]['perf_type'] == perf_type:
                        if i == end - 1:
                            if bot > act_perf_well[i]['bot']:
                                act_perf_well[i]['bot'] = bot
                        else:
                            act_perf_well[i]['bot'] = act_perf_well[i + 1]['top']
                    else:
                        if i == end - 1:
                            if bot > act_perf_well[i]['bot']:
                                to_append.append(dict(well=well, top=act_perf_well[i]['bot'],
                                                      bot=bot, perf_type=perf_type, date=date, idx=i + shift))
                                shift += 1

                        else:
                            to_append.append(dict(well=well, top=act_perf_well[i]['bot'],
                                                  bot=act_perf_well[i + 1]['top'],
                                                  perf_type=perf_type, date=date, idx=i + shift))
                            shift += 1
                for el in to_append:
                    act_perf_well.insert(el['idx'],
                                         {'well': el['well'],
                                          'top': el['top'],
                                          'bot': el['bot'],
                                          'perf_type': el['perf_type'],
                                          'date': date
                                          }
                                         )
                if (idx_b == len(act_perf_well)) and (act_perf_well[-1]['bot'] < bot):
                    if act_perf_well[-1]['perf_type'] == perf_type:
                        act_perf_well[-1]['bot'] = bot
                    else:
                        act_perf_well.append({'well': well,
                                              'top': act_perf_well[-1]['bot'],
                                              'bot': bot,
                                              'perf_type': perf_type,
                                              'date': date})
            idx += 1
    return act_perf_well, dates
