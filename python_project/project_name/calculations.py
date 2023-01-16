import argparse

import pandas as pd


def calculations(input_path, input_year):
    df = pd.read_csv(input_path, parse_dates=['dt'])
    df['year'] = df['dt'].apply(lambda x: x.year)
    df['month'] = df['dt'].apply(lambda x: x.month)
    warmth = (
        df[df.year == int(input_year)]
        .groupby(['City', 'month'], as_index=False)['AverageTemperature']
        .mean()
    )
    warmth = warmth.sort_values(by=['AverageTemperature'], ascending=False)
    return warmth['month'].values[0], warmth['City'].values[0]


def main():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument(type=str, help='', dest='a')
    parser.add_argument(type=int, help='', dest='b')
    args = parser.parse_args()
    input_path, input_year = args.a, args.b

    print(calculations(input_path, input_year))


if __name__ == "__main__":
    main()
