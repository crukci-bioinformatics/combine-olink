import bz2
import json
import os
import pandas as pd

from pathlib import Path

with open('combineOLinkCounts.py', 'rt') as script:
    exec(''.join(script.readlines()))

def test_combine_files():

    testDataDir = Path('testdata')
    combinedCountsFile = testDataDir / 'combined_counts_DELETE.csv'
    combinedMetaFile = testDataDir / 'combined_counts_DELETE.json'

    try:
        combineOLink(testDataDir, combinedCountsFile.name, combinedMetaFile.name, True)

        assert combinedCountsFile.exists()
        assert combinedMetaFile.exists()

        column_types = {
            'sample_index': str,
            'forward_barcode': str,
            'reverse_barcode': str,
            'count': int
        }

        combinedCounts = pd.read_csv(combinedCountsFile, delimiter=';', dtype=column_types)

        count = combinedCounts\
            .query("sample_index == 'i3001' and forward_barcode == 'FB201772' and reverse_barcode == 'RB201772'")\
            ['count']\
            .iloc[0]

        assert count == 25460 + 25102

        count = combinedCounts\
            .query("sample_index == 'i3002' and forward_barcode == 'FB200009' and reverse_barcode == 'RB200009'")\
            ['count']\
            .iloc[0]

        assert count == 1028 + 1041

        with open(combinedMetaFile, 'rt') as file:
            combinedMeta = json.load(file)

        metaLib = combinedMeta['libraries'][0]
        metaUnit = combinedMeta['runUnits'][0]

        assert metaLib['reads'] == 1394529920 * 2
        assert metaLib['readsPf'] == 841451332 + 831476182
        assert metaUnit['matchedCounts'] == 670225582 + 663298405
        assert metaUnit['countsFileName'] == combinedCountsFile.name

    finally:
        combinedCountsFile.unlink(True)
        combinedMetaFile.unlink(True)
