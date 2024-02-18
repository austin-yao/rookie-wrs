import pandas as pd

abbrv_to_name = {}
abbrv_to_name["ARI"] = "Arizona Cardinals"
abbrv_to_name["ATL"] = "Atlanta Falcons"
abbrv_to_name["BAL"] = "Baltimore Ravens"
abbrv_to_name["BUF"] = "Buffalo Bills"
abbrv_to_name["CAR"] = "Carolina Panthers"
abbrv_to_name["CHI"] = "Chicago Bears"
abbrv_to_name["CIN"] = "Cincinnati Bengals"
abbrv_to_name["CLE"] = "Cleveland Browns"
abbrv_to_name["DAL"] = "Dallas Cowboys"
abbrv_to_name["DEN"] = "Denver Broncos"
abbrv_to_name["DET"] = "Detroit Lions"
abbrv_to_name["GNB"] = "Green Bay Packers"
abbrv_to_name["HOU"] = "Houston Texans"
abbrv_to_name["IND"] = "Indianapolis Colts"
abbrv_to_name["JAX"] = "Jacksonville Jaguars"
abbrv_to_name["KAN"] = "Kansas City Chiefs"
abbrv_to_name["LVR"] = "Las Vegas Raiders"
abbrv_to_name["LAC"] = "Los Angeles Chargers"
abbrv_to_name["LAR"] = "Los Angeles Rams"
abbrv_to_name["MIA"] = "Miami Dolphins"
abbrv_to_name["MIN"] = "Minnesota Vikings"
abbrv_to_name["NWE"] = "New England Patriots"
abbrv_to_name["NOR"] = "New Orleans Saints"
abbrv_to_name["NYG"] = "New York Giants"
abbrv_to_name["NYJ"] = "New York Jets"
abbrv_to_name["PHI"] = "Philadelphia Eagles"
abbrv_to_name["PIT"] = "Pittsburgh Steelers"
abbrv_to_name["SFO"] = "San Francisco 49ers"
abbrv_to_name["SEA"] = "Seattle Seahawks"
abbrv_to_name["TAM"] = "Tampa Bay Buccaneers"
abbrv_to_name["TEN"] = "Tennessee Titans"
abbrv_to_name["WAS"] = "Washington Commanders"
abbrv_to_name["OAK"] = "Oakland Raiders"

def fetch_value(key, df, year):
    if key not in abbrv_to_name:
        print(abbrv_to_name)
        print(year)
    team_name = abbrv_to_name[key]
    row = df[df['Tm'] == team_name]
    if not row.empty:
        return row[['PF', 'Att']].iloc[0]
    else:
        if key == "WAS":
            row = df[df['Tm'] == "Washington Redskins"]
            if not row.empty:
                return row[['PF', 'Att']].iloc[0]
            else:
                row = df[df['Tm'] == "Washington Football Team"]
                if not row.empty:
                    return row[['PF', 'Att']].iloc[0]
                else:
                    return None
        return None

def main():
    dfs = []
    for i in range(2014, 2024):
        players2023 = pd.read_csv(f'player_data/player_stats_{i}.csv')
        players2023 = players2023.replace(r'[*+]', '', regex=True)
        data2023 = (players2023[players2023['Pos'] == 'WR']).loc[:, ['Player', 'Tm', 'Rec', 'TD']]
        snaps2023 = pd.read_csv(f'snap_data/snap_counts_{i}.csv')
        wrsnaps2023 = (snaps2023[snaps2023['position'] == 'WR']).groupby('player')['offense_snaps'].sum()
        draft2023 = pd.read_csv(f'draft_data/draft_{i}.csv')
        rookies2023 = (draft2023[draft2023['Pos'] == 'WR']).loc[:, ['Player', 'Rnd', 'College/Univ']]
        team2023 = pd.read_csv(f'team_data/team_stats_{i}.csv').iloc[:, [1, 3, 11]]
        temp = pd.merge(rookies2023, wrsnaps2023, left_on='Player', right_on='player', how='inner')
        combined = pd.merge(temp, data2023, on='Player', how='inner')
        combined[['PF', 'Att']] = combined["Tm"].apply(fetch_value, df=team2023, year = i)
        dfs.append(combined)
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df.to_csv('output.csv', index=False)
        

main()