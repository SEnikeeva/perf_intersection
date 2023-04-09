import datetime

import pandas as pd
from tqdm import tqdm

from helpers.utils import replace_slash


def drop_equals(df):
    del_idxs = []
    for i in tqdm(range(len(df) - 1)):
        if ((df.loc[i, 'top'] == df.loc[i + 1, 'bot']) or (
                df.loc[i, 'top'] == df.loc[i + 1, 'top'])) and (
                df.loc[i, 'well'] == df.loc[i + 1, 'well']) and (
                df.loc[i, 'perf_type'] == df.loc[i + 1, 'perf_type']) and (
                df.loc[i, 'date'] == df.loc[i + 1, 'date']):
            df.loc[i + 1, 'bot'] = df.loc[i, 'bot']
            del_idxs.append(i)
        elif ((df.loc[i, 'top'] == df.loc[i + 1, 'bot']) or (
                df.loc[i, 'top'] == df.loc[i + 1, 'top'])) and (
                df.loc[i, 'well'] == df.loc[i + 1, 'well']) and (
                df.loc[i, 'perf_type'] == df.loc[i + 1, 'perf_type']) and (
                df.loc[i, 'date'] == df.loc[i + 1, 'date']):
            df.loc[i + 1, 'bot'] = df.loc[i, 'bot']
            del_idxs.append(i)
    df.drop(del_idxs, inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def diff_detection(model_perf, base_perf, output_folder, cfg):
    def compare(df_base, df_model, out_path, intersec_path):
        df_model = df_model[df_model['well'].isin(df_base['well'])]

        df_base = df_base.drop_duplicates()
        df_model = df_model.drop_duplicates()

        df_base.reset_index(drop=True, inplace=True)
        df_model.reset_index(drop=True, inplace=True)

        df_base = df_base.sort_values(by=['well', 'date'], ascending=False, kind='merge')
        df_model = df_model.sort_values(by=['well', 'date'], ascending=False, kind='merge')
        df_model = df_model.drop(df_model[df_model['top'] == df_model['bot']].index)
        df_base = df_base.drop(df_base[df_base['top'] == df_base['bot']].index)
        df_model.reset_index(drop=True, inplace=True)
        df_base.reset_index(drop=True, inplace=True)
        df_base = drop_equals(df_base)
        df_model = drop_equals(df_model)
        mrg = df_model.merge(df_base, how='outer', on=['date', 'well'], suffixes=('_model', '_armits'))
        mrg['equal'] = ((mrg['top_model'] - mrg['top_armits']).abs() <= cfg.grid_delta) & (
                    (mrg['bot_model'] - mrg['bot_armits']).abs() <= cfg.grid_delta) & (mrg['perf_type_model'] == mrg[
                           'perf_type_armits'])
        intersection = mrg[mrg.equal]
        intersection_model = intersection[['well', 'date', 'top_model', 'bot_model', 'perf_type_model']].\
            rename(columns=lambda x: x.replace('_model', ''))
        intersection_base = intersection[['well', 'date', 'top_armits', 'bot_armits', 'perf_type_armits', 'level']].\
            rename(columns=lambda x: x.replace('_armits', ''))

        model_diff = pd.concat([intersection_model, df_model]).drop_duplicates(keep=False)
        base_diff = pd.concat([intersection_base, df_base]).drop_duplicates(keep=False)

        base_diff = base_diff.rename(
            columns={'top': 'top_armits', 'bot': 'bot_armits', 'perf_type': 'perf_type_armits'})
        model_diff = model_diff.rename(columns={'top': 'top_model', 'bot': 'bot_model', 'perf_type': 'perf_type_model'})

        model_diff['top_armits'] = None
        model_diff['bot_armits'] = None
        model_diff['perf_type_armits'] = None
        model_diff['level'] = None

        base_diff['top_model'] = None
        base_diff['bot_model'] = None
        base_diff['perf_type_model'] = None

        model_dict = model_diff.groupby(by=['date', 'well'], sort=False).apply(
            lambda x: [dict(zip(model_diff.columns[2:], e[2:]))
                       for e in x.values])
        base_dict = base_diff.groupby(by=['date', 'well'], sort=False).apply(
            lambda x: [dict(zip(base_diff.columns[2:], e[2:]))
                       for e in x.values])

        for k, v in base_dict.items():
            if model_dict.get(k) is not None:
                for i, interval in enumerate(v):
                    if i >= len(model_dict.get(k)):
                        model_dict[k].append(interval)
                    else:
                        for col in ['top_armits', 'bot_armits', 'perf_type_armits', 'level']:
                            model_dict[k][i][col] = interval[col]
            else:
                model_dict[k] = []
                for i, interval in enumerate(v):
                    model_dict[k].append(interval)

        mrg_dict = []
        for k, v in model_dict.items():
            date = k[0]
            well = k[1]
            for interval in v:
                interval['date'] = date
                interval['well'] = well
                mrg_dict.append(interval)

        mrg = pd.DataFrame(mrg_dict)
        mrg = mrg.sort_values(by=['date', 'well', 'top_model', 'top_armits'])

        mrg.set_index(['date', 'well']).to_excel(out_path)

        intersection.to_excel(intersec_path, index=False)

    with open(replace_slash(f"{output_folder}\\not_found_in_armits"), 'w') as f:
        f.write('\n'.join(model_perf[~model_perf['well'].isin(base_perf['well'])]['well'].unique()))

    base_perf_history = base_perf.copy()
    base_perf_history.loc[base_perf_history['type'].isin([2, 4]), 'type'] = 0
    base_perf_history.loc[base_perf_history['type'].isin([3, 5]), 'type'] = 1
    base_perf_history = base_perf_history[['well', 'date', 'top', 'bot', 'type', 'level']][base_perf_history.type != -1]
    base_perf_history['date'] = base_perf_history['date'].apply(
        lambda x: datetime.date(year=x.year, month=x.month, day=1))
    base_perf_history.rename(columns={'type': 'perf_type'}, inplace=True)

    model_perf_history = model_perf.copy().rename(columns={'type': 'perf_type'})
    min_date = cfg.min_date if cfg.min_date is not None else min(base_perf_history['date'].min(),
                                                                 model_perf_history['date'].min())
    max_date = cfg.max_date if cfg.max_date is not None else model_perf_history['date'].max()
    print(min_date, max_date)
    model_perf_history = model_perf_history[model_perf_history['date'].between(min_date, max_date)]
    base_perf_history = base_perf_history[base_perf_history['date'].between(min_date, max_date)]

    compare(base_perf_history,
            model_perf_history,
            replace_slash(f"{output_folder}\\perf_diff_history.xlsx"),
            replace_slash(f"{output_folder}\\perf_intersec_history.xlsx"))
