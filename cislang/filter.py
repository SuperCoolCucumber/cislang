import pandas as pd
import numpy as np
import re
import unidecode

ka_df = pd.read_csv('/home/daria/cislang/ka_df.csv', index_col=0)


# dropping extra columns and dropping nans
def drop_cols(df):
    return df.drop(['ru_link', 'en_link', 'ka_name', 'ka_link'], axis=1).dropna()


# aligning russian names to fit english:
# swapping surname and name and delelting the comma
mapper = {
            2: lambda name: ' '.join(name[::-1]).strip(','),
            3: lambda name: ' '.join(name[:-1][::-1]).strip(','),
            4: lambda name: ' '.join(name[:-2][::-1]).strip(','),
        }
def rus_name_align(name):
    if ',' in name:
        name = name.split()
        name = mapper[len(name)](name)
    return name



# deleting patronymics
def delete_patronymic(name):
    """ """
    name = name.split()
    if ',' not in name and len(name) == 3:
        del name[1]
    name = ' '.join(name)
    return name


# deleting trash ("родившиеся в Тбилиси")
# .drop(ka_df.index[ka_df["ru_name"] == "Категория:Родившиеся Тбилиси"])


# delete more trash (symbols)
# .mask(ka_df[['en_name', 'ru_name']].apply(lambda item: item.str.contains(r'\(|\)| X| I | II| V | VI |\,|\.| of '), axis=1)).dropna()

# fixing inconsistencies:
# rus and eng names must both consist of two words
def delete_inconsistencies(name):
    if name.split(' ').__len__() == 1:
        return np.nan
    return name
# .dropna()

# deleting long names
def delete_long(name):
    return name[name.str.split().str.len().lt(3)]


# splitting rows by name and surname
# .apply(lambda item: item.str.split(" "))
# .apply(pd.Series.explode)


# upper-case
# .apply(lambda item: item.str.upper(), axis=1)



# swapping columns
#.reindex(columns=['en_name', 'ru_name'])


# removing diacritics
def remove_diacr(name):
    return name.apply(unidecode.unidecode)


# dropping duplicates
#.drop_duplicates()





(ka_df
    .apply({'ru_name': rus_name_align, 'en_name': lambda x: x})
    .apply({'ru_name': delete_patronymic, 'en_name': delete_patronymic})
    .drop(ka_df.index[ka_df["ru_name"] == "Категория:Родившиеся Тбилиси"])
    .mask(ka_df[['en_name', 'ru_name']].apply(lambda item: item.str.contains(r'\(|\)| X| I | II| V | VI |\,|\.| of '), axis=1)).dropna()
    .apply({'ru_name': delete_inconsistencies, 'en_name': delete_inconsistencies})
    .dropna()
    .apply({'ru_name': delete_long, 'en_name': lambda x: x})
    .apply(lambda item: item.str.split(" "))
    .apply(pd.Series.explode)
    .apply(lambda item: item.str.upper(), axis=1)
    .reindex(columns=['en_name', 'ru_name'])
    .apply({'ru_name': lambda x: x, 'en_name': remove_diacr})
    .drop_duplicates()
)

    

ka_df.to_csv('/home/daria/cislang/georgian_tst.csv', sep=';', index=False)
