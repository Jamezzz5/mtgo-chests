import logging
import pandas as pd
import chests.scraper as scr
import chests.utils as utl


chest_info_url = ('https://magic.wizards.com/en/MTGO/articles/archive/'
                  'magic-online/treasure-chest-card-list-info')
price_base_url = 'https://www.mtggoldfish.com/index/{}#online'

pack_path = 'pack_ratios/'
price_path = 'set_prices/'

cur_fn = 'Card Name_Set_Frequency Ratio.csv'
pp_fn = 'Play Points_Frequency Ratio.csv'
chest_fn = ('Slot_Curated_Modern Rare-Mythic_Avatar_'
            'Play Points_Standard Common-Uncommon.csv')

chest_card_out_fn = 'chest_card_prices.csv'
chest_val_out_fn = 'chest_value.csv'

freq_col = 'Frequency Ratio'
pp_col = 'Play Points'
price_col = 'Price'
ppo_col = 'Price Per Open'
cur_col = 'Curated'
set_col = 'Set'
card_col = 'Card'
old_card_col = 'Card Name'


def get_chest_info(file_path=pack_path):
    wt = scr.WebTable(chest_info_url)
    wt.all_tables_to_df(col_loc=1, file_path=file_path)


def get_curated_card_df(file_path=pack_path, file_name=cur_fn):
    df = utl.import_read_csv(file_path, file_name)
    df = df.rename(columns={old_card_col: card_col})
    return df


def get_card_prices_curated_cards(df):
    wt = scr.WebTable()
    cur_df = pd.DataFrame()
    card_sets = df[set_col].unique()
    for card_set in card_sets:
        card_set_df = get_card_price_df(card_set, wt)
        tdf = pd.merge(df, card_set_df, how='inner', on=[card_col, set_col])
        cur_df = cur_df.append(tdf, ignore_index=True)
    return cur_df


def get_card_price_df(card_set, wt=None):
    if not wt:
        wt = scr.WebTable()
    url = price_base_url.format(card_set)
    wt.all_tables_to_df(url=url, col_loc='head', file_path=price_path,
                        file_name=card_set, table_idx=2)
    return wt.df


def calculate_curated_ev(df=None):
    df = utl.df_col_to_float(df, [freq_col, price_col])
    df[ppo_col] = ((df[freq_col] / df[freq_col].sum()) * df[price_col])
    utl.df_to_csv(df, chest_card_out_fn)
    cur_val = df[ppo_col].sum()
    return cur_val


def calculate_pp_ev(file_path=pack_path, file_name=pp_fn):
    pp_df = utl.import_read_csv(file_path, file_name)
    pp_df = utl.df_col_to_float(pp_df, [freq_col, pp_col])
    pp_df[ppo_col] = ((pp_df[freq_col] / pp_df[freq_col].sum()) *
                      (pp_df[pp_col] / 10))
    pp_val = pp_df[ppo_col].sum()
    return pp_val


def calculate_chest_ev(cur_val, pp_val, file_path=pack_path,
                       file_name=chest_fn):
    df = utl.import_read_csv(file_path, file_name)
    df = utl.df_col_to_float(df, [cur_col, pp_col])
    for col in [cur_col, pp_col]:
        df[col] = df[col] / 100
    for col, val in {pp_col: pp_val, cur_col: cur_val}.items():
        df[col] = df[col] * val
    utl.df_to_csv(df, chest_val_out_fn)
    tot_cur_ev = df[cur_col].sum()
    tot_pp_ev = df[pp_col].sum()
    tot_ev = tot_cur_ev + tot_pp_ev
    logging.info('Expected value of curated cards is: ' + str(tot_cur_ev))
    logging.info('Expected value of play points is: ' + str(tot_pp_ev))
    logging.info('Expected chest value is: ' + str(tot_ev))
