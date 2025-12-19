import argparse
import json
import pandas as pd
import sys

from pathlib import Path

def parseArgs():
    parser = argparse.ArgumentParser(description="Combine Olink counts files.")
    parser.add_argument('-f', '--folder', default=Path.cwd(), type=Path, help="The folder where the Olink counts files are located.")
    parser.add_argument('-c', '--counts', default='olink_counts_total.csv', type=str, help="The combined counts file name.")
    parser.add_argument('-m', '--meta', default='olink_counts_total.json', type=str, help="The combined ngs2counts meta file name.")
    parser.add_argument('--force', action="store_true", help="Force overwriting of the output file if it exists.")
    return parser.parse_args()

def combineOLink(folder: Path, counts_file_name: str, meta_file_name: str, force: bool = False):

    combined_counts_file = folder / counts_file_name
    if combined_counts_file.exists() and not force:
        sys.exit(f"The file {combined_counts_file} already exists.")

    combined_meta_file = folder / meta_file_name
    if combined_meta_file.exists() and not force:
        sys.exit(f"The file {combined_counts_file} already exists.")

    counts_pattern = "*.olink_counts.csv"
    meta_pattern = "*.olink_meta.json"

    olink_counts_files = list(folder.glob(counts_pattern))
    olink_meta_files = list(folder.glob(meta_pattern))

    if not olink_counts_files:
        sys.exit(f"No Olink counts files found matching '{counts_pattern}' in {folder}")
    if not olink_meta_files:
        sys.exit(f"No Olink meta files found matching '{meta_pattern}' in {folder}")

    # Define column types (dtypes for pandas)
    column_types = {
        'sample_index': str,
        'forward_barcode': str,
        'reverse_barcode': str,
        'count': int
    }

    join_columns = ['sample_index', 'forward_barcode', 'reverse_barcode']

    # Read the first file and sort values
    counts = pd.read_csv(olink_counts_files[0], delimiter=';', dtype=column_types)
    counts = counts.sort_values(by=join_columns)

    # Process additional files if any
    for olink_file in olink_counts_files[1:]:
        additional = pd.read_csv(olink_file, delimiter=';', dtype=column_types)

        # Ensure no rows exist in counts that are not in the additional file
        check = counts.merge(additional, on=join_columns, how='outer', indicator=True)
        mismatched = check[check['_merge'] != 'both']
        if not mismatched.empty:
            mismatched_info = f"We have {len(mismatched)} count rows that are not in both {olink_counts_files[0].name} and {olink_file.name}. Are the files from the same pools?"
            sys.exit(mismatched_info)

        # Combine data and summarize counts
        counts = pd.merge(counts, additional, on=join_columns, how='outer', suffixes=('.x', '.y'))
        counts['count'] = counts[['count.x', 'count.y']].sum(axis=1, skipna=True)
        counts = counts[['sample_index', 'forward_barcode', 'reverse_barcode', 'count']]

    # Read the first meta file
    with olink_meta_files[0].open('rt') as fp:
        meta = json.load(fp)
    meta_lib = meta['libraries'][0]
    meta_unit = meta['runUnits'][0]

    # Add counts for all other meta files.
    for meta_file in olink_meta_files[1:]:
        with meta_file.open('rt') as fp:
            additional = json.load(fp)

        meta_lib['reads'] += additional['libraries'][0]['reads']
        meta_lib['readsPf'] += additional['libraries'][0]['readsPf']

        meta_unit['matchedCounts'] += additional['runUnits'][0]['matchedCounts']

    meta_lib['percentReadsPf'] = meta_lib['readsPf'] / meta_lib['reads'] * 100.0
    meta_unit['countsFileName'] = counts_file_name

    # Write counts and meta

    counts.to_csv(combined_counts_file, sep=';', index=False)
    with combined_meta_file.open('wt') as fp:
        json.dump(meta, fp, indent = 2)

    print(f"Combined Olink counts file has been saved to {combined_counts_file}")
    print(f"The updated meta file has been saved to {combined_meta_file}")

if __name__ == '__main__':
    args = parseArgs()
    combineOLink(args.folder, args.counts, args.meta, args.force)
