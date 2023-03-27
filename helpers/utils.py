import os
import sys


# конвертация путей файлов в зависимости от системы
def replace_slash(file_path):
    platform = sys.platform
    slash_map = {'win32': '\\',
                 'cygwin': '\\',
                 'darwin': '/',
                 'linux2': '/'}
    if platform not in slash_map.keys():
        platform = 'linux2'
    return file_path.replace('\\', slash_map[platform])


# очистка папки output_folder
def clear_out_folder(output_folder):
    files = os.listdir(output_folder)
    for f_ in files:
        path_dir = replace_slash(output_folder + "\\" + f_)
        os.remove(path_dir)


# метод заменяет в названиях скважин буквы с латиницы на кириллицу
# может принимать на вход массив или просто название (тогда аргумент one=True)
# пример использования: df['Number'] = df['Number'].apply(transliteration)
def transliteration(numbers, one=True):
    # словарь замен
    letters = {'bgs': 'бгс', 'bg': 'бг', 'gs': 'гс', 'gc': 'гс', 'a': 'а', 'd': 'д',
               'g': 'г', 'k': 'к', 'l': 'л', 'm': 'м', 'r': 'р',
               's': 'с', 'u': 'у', 'v': 'в', 'x': 'х', 'z': 'з'}

    # применение словаря замен к названию
    def trans(number):
        if type(number) is not str:
            return number
        else:
            number = number.lower()
        for letter in letters.keys():
            number = number.replace(letter, letters[letter])
        return number

    if one:
        return trans(numbers.lower())
    else:
        return [trans(num) for num in numbers]


def to_dict(df, columns, index_name):
    df.set_index(index_name, inplace=True)
    # перестановка столбцов для сохранения установленного порядка
    df = df.reindex(columns, axis=1)
    # преобразование DataFrame в словарь
    df_dict = df.groupby(level=0, sort=False) \
        .apply(lambda x: [dict(zip(columns, e))
                          for e in x.values]) \
        .to_dict()
    return df_dict


def is_contains(w, a):
    for v in a:
        if v in w:
            return True
    return False


def well_renaming(w_name):
    if type(w_name) is not str:
        w_name = str(w_name)
    w_name = w_name.lower().strip().split('/')[0]
    return w_name
