from multiprocessing import current_process
from re import L
import time
from bs4 import BeautifulSoup
import requests
import pandas as pd
from tqdm import tqdm
import numpy as np


def isfloat(num):
    try:
        float(num)
        return True
    except ValueError:
        return False


def one_hot_encode_position(position):
    if position == 'QB':
        return 1
    elif position == 'RB':
        return 2
    elif position == 'WR':
        return 3
    elif position == 'TE':
        return 4
    else:
        return 5


def get_player_stats():
    column_list = ['year', 'player', 'team', 'fantasy_pos', 'age', 'g', 'gs', 'pass_cmp', 'pass_att', 'pass_yds', 'pass_td', 'pass_int', 'rush_att', 'rush_yds', 'rush_yds_per_att', 'rush_td', 'rec', 'rec_yds', 'rec_yds_per_rec',
                   'rec_td', 'fumbles', 'fumbles_lost', 'all_td', 'two_pt_md', 'two_pt_pass', 'fantasy_points', 'fantasy_points_ppr', 'draftkings_points', 'fanduel_points', 'vbd', 'fantasy_rank_pos', 'fantasy_rank_overall']
    df = pd.DataFrame(columns=column_list)

    for year_num in range(1970, 2022):
        start = time.time()
        year = str(year_num)
        url = 'https://www.pro-football-reference.com/years/{}/fantasy.htm'.format(
            year)
        data = requests.get(url).text
        soup = BeautifulSoup(data, 'html.parser')
        table = soup.find('table')

        for row in table.tbody.find_all('tr'):
            columns = row.find_all('td')
            current_player = {}
            for item in columns:
                header = item.attrs['data-stat']
                if header == 'player':
                    player_id = item.attrs['data-append-csv']
                value = item.text.strip().replace('*', '').replace('+', '')
                if value.isdigit():
                    cleaned_value = int(value)
                elif isfloat(value):
                    cleaned_value = float(value)
                else:
                    cleaned_value = value
                current_player[header] = cleaned_value
            if current_player != {}:
                current_player['year'] = year_num
                df = df.append(current_player, ignore_index=True)

        end = time.time()
        print(end - start)
        print('done with', year)
        if end - start < 3:
            time.sleep(1)
    df.to_csv('ff_dl/data/fantasy_player_data.csv', index=False)


def reshape_player_stats():
    df = pd.read_csv('ff_dl/data/fantasy_player_data.csv')
    df = df.drop(['draftkings_points', 'fanduel_points'], axis=1)
    df = df.sort_values(by=['year'])
    col_list = ['player', 'latest_yr_played', 'latest_age', 'num_years_played']
    i = 1
    for year in range(1, 23):
        for col in df.columns:
            if col != 'player':
                new_col = '{}_{}'.format(str(i), col)
                col_list.append(new_col)
        i += 1
    for col in df.columns:
        if col != 'player':
            new_col = 'next_{}'.format(col)
            col_list.append(new_col)

    new_df = pd.DataFrame(columns=col_list)

    player_exp = {}
    data = {'1': {}}
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        name = row['player']
        pos = row['fantasy_pos']
        age = row['age']
        yr = row['year']
        if not new_df.loc[(new_df['player'] == name) & (new_df['latest_yr_played'] == yr - 1) & (new_df['latest_age'] == age-1)].empty:
            # existing year exists
            copy_of_row = new_df.loc[(new_df['player'] == name) & (
                new_df['latest_yr_played'] == yr - 1) & (new_df['latest_age'] == age-1)].copy(deep=True)
            exp = copy_of_row['num_years_played'].values[0]
            copy_of_row['latest_yr_played'] = yr
            copy_of_row['latest_age'] = age
            copy_of_row['num_years_played'] = exp + 1

            for col in df.columns:
                next_col_name = 'next_{}'.format(col)
                latest_exp_col_name = '{}_{}'.format(exp, col)
                if col != 'player':
                    copy_of_row[latest_exp_col_name] = copy_of_row[next_col_name]
                    copy_of_row[next_col_name] = row[col]
            new_df = pd.concat([new_df, copy_of_row])

        else:
            entry = {'player': name, 'latest_yr_played': yr,
                     'latest_age': age, 'num_years_played': 1}
            for col in df.columns:
                col_name = 'next_{}'.format(col)
                if col != 'player':
                    entry[col_name] = row[col]
            new_df = new_df.append(entry, ignore_index=True)

    print('done')
    new_df.to_csv('ff_dl/data/fantasy_player_data_reshape.csv', index=False)


def add_half_ppr():
    df = pd.read_csv('ff_dl/data/fantasy_player_data_reshape.csv')
    i = 1
    for year in range(1, 23):
        col_index = df.columns.get_loc('{}_fantasy_points'.format(i)) + 1
        half_ppr_val = df['{}_fantasy_points'.format(
            i)] + (0.5 * df['{}_rec'.format(i)])
        df.insert(col_index, '{}_fantasy_points_half_ppr'.format(
            i), half_ppr_val, True)
        i += 1
    col_index = df.columns.get_loc('next_fantasy_points') + 1
    half_ppr_val = df['next_fantasy_points'] + (0.5 * df['next_rec'])
    df.insert(col_index, 'next_fantasy_points_half_ppr', half_ppr_val, True)
    df.to_csv('ff_dl/data/fantasy_player_data_half_ppr.csv')


def remove_all_but_target_fantasy_points():
    df = pd.read_csv('ff_dl/data/fantasy_player_data_half_ppr.csv')
    df = df.drop(columns=['next_year', 'next_team', 'next_fantasy_pos', 'next_age', 'next_g', 'next_gs', 'next_pass_cmp', 'next_pass_att', 'next_pass_yds', 'next_pass_td', 'next_pass_int', 'next_rush_att', 'next_rush_yds', 'next_rush_yds_per_att', 'next_rush_td', 'next_rec',
                 'next_rec_yds', 'next_rec_yds_per_rec', 'next_rec_td', 'next_fumbles', 'next_fumbles_lost', 'next_all_td', 'next_two_pt_md', 'next_two_pt_pass', 'next_fantasy_points', 'next_fantasy_points_ppr', 'next_vbd', 'next_fantasy_rank_pos', 'next_fantasy_rank_overall', 'next_targets'])
    df.to_csv('ff_dl/data/fantasy_player_data_train_val.csv')


def create_test_set():
    df = pd.read_csv('ff_dl/data/fantasy_player_data_half_ppr.csv')
    df = df[df['latest_yr_played'] == 2021]
    column_list = ['year', 'team', 'fantasy_pos', 'age', 'g', 'gs', 'pass_cmp', 'pass_att', 'pass_yds', 'pass_td', 'pass_int', 'rush_att', 'rush_yds', 'rush_yds_per_att', 'rush_td', 'rec', 'rec_yds', 'rec_yds_per_rec',
                   'rec_td', 'fumbles', 'fumbles_lost', 'all_td', 'two_pt_md', 'two_pt_pass', 'fantasy_points', 'fantasy_points_half_ppr', 'fantasy_points_ppr', 'vbd', 'fantasy_rank_pos', 'fantasy_rank_overall', 'targets']
    for index, row in tqdm(df.iterrows(), total=df.shape[0]):
        exp = row['num_years_played']
        row['latest_yr_played'] = row['latest_yr_played'] + 1
        row['latest_age'] = row['latest_age'] + 1
        row['num_years_played'] = exp + 1
        for col in column_list:
            column = '{}_{}'.format(exp, col)
            next_column = 'next_{}'.format(col)
            row[column] = row[next_column]
            row[next_column] = np.nan
        df.loc[index] = row
    df = df.drop(columns=['next_year', 'next_team', 'next_fantasy_pos', 'next_age', 'next_g', 'next_gs', 'next_pass_cmp', 'next_pass_att', 'next_pass_yds', 'next_pass_td', 'next_pass_int', 'next_rush_att', 'next_rush_yds', 'next_rush_yds_per_att', 'next_rush_td', 'next_rec',
                          'next_rec_yds', 'next_rec_yds_per_rec', 'next_rec_td', 'next_fumbles', 'next_fumbles_lost', 'next_all_td', 'next_two_pt_md', 'next_two_pt_pass', 'next_fantasy_points', 'next_fantasy_points_ppr', 'next_vbd', 'next_fantasy_rank_pos', 'next_fantasy_rank_overall', 'next_targets', 'next_fantasy_points_half_ppr'])
    df.to_csv('ff_dl/data/fantasy_player_data_test.csv')


def get_train_and_val():
    data = []
    labels = []
    with open('ff_dl/data/fantasy_player_data_train_val.csv', 'r') as f:
        lines = f.readlines()
        first_line = True
        for line in lines:
            if first_line:
                first_line = False
            else:
                line_list = line.split(',')
                x = line_list[2:-1]
                y = line_list[-1]
                data.append(x)
                labels.append(y)
    train_pct = 0.8
    train_num = int(len(data) * train_pct)
    train_data = data[:train_num]
    val_data = data[train_num:]
    train_label = labels[:train_num]
    val_label = labels[train_num:]
    return train_data, train_label, val_data, val_label

def get_test_data():
    data = []
    with open('ff_dl/data/fantasy_player_data_test.csv', 'r') as f:
        lines = f.readlines()
        first_line = True
        for line in lines:
            if first_line:
                first_line = False
            else:
                line_list = line.split(',')
                x = line_list[2:-1]
                data.append(x)
    return data


if __name__ == '__main__':
    get_player_stats()
    # reshape_player_stats()
    # add_half_ppr()
    # remove_all_but_target_fantasy_points()
    # create_test_set()
    train_data, train_label, val_data, val_label = get_train_and_val()
    test_data = get_test_data()
    test = ''