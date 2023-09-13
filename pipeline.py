import pandas as pd
import argparse

def main(source, target):
    print("Starting")

    # Extract data
    data = pd.read_csv(source)

    # Transform data
    data.drop(["Industry_code_ANZSIC06"], axis=1, inplace=True)

    # Load data
    data.to_csv(target, index=False)

    print("Complete")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source.csv')
    parser.add_argument('target', help='target.csv')
    args = parser.parse_args()

    main(args.source, args.target)
