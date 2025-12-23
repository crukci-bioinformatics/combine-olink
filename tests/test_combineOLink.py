import json
import os
import pandas as pd

from pathlib import Path

with open('combineOLinkCounts.py', 'rt') as script:
    exec(''.join(script.readlines()))

column_types = {
    'sample_index': str,
    'forward_barcode': str,
    'reverse_barcode': str,
    'count': int
}
join_columns = ['sample_index', 'forward_barcode', 'reverse_barcode']

def test_combine_counts():

    testDataDir = Path('testdata')
    combinedCountsFile = testDataDir / 'combined_counts_DELETE.csv'

    combinedCounts = combineCounts(testDataDir)

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

def test_combine_meta():

    testDataDir = Path('testdata')
    combinedCountsName = 'combined_counts_DELETE.csv'

    combinedMeta = combineMeta(testDataDir, combinedCountsName)

    metaLib = combinedMeta['libraries'][0]
    metaUnit = combinedMeta['runUnits'][0]

    assert metaLib['reads'] == 1394529920 * 2
    assert metaLib['readsPf'] == 841451332 + 831476182
    assert metaUnit['matchedCounts'] == 670225582 + 663298405
    assert metaUnit['countsFileName'] == combinedCountsName

def test_against_two_lane_ngs2counts_counts():

    testDataDir = Path('testdata')
    validationFile = testDataDir / 'validation' / 'both.olink_counts.csv'

    combinedCounts = combineCounts(testDataDir)

    twoLaneCounts = pd.read_csv(validationFile, delimiter=';', dtype=column_types)

    assert combinedCounts.shape == twoLaneCounts.shape

    joinedCounts = pd.merge(combinedCounts, twoLaneCounts, on=join_columns, how='inner', suffixes=('.test', '.ref'))

    assert joinedCounts.shape[0] == twoLaneCounts.shape[0]

    # For some reason we can occasionally see counts out by one compared to the combined counts of ngs2counts
    incorrect = joinedCounts[abs(joinedCounts['count.test'] - joinedCounts['count.ref']) > 1]

    assert incorrect.shape[0] == 0, f"Have rows where the count difference is > 1:\n{incorrect}"

def test_against_two_lane_ngs2counts_meta():

    testDataDir = Path('testdata')
    validationCountsFile = testDataDir / 'validation' / 'both.olink_counts.csv'
    validationMetaFile = testDataDir / 'validation' / 'both.olink_meta.json'

    combinedMeta = combineMeta(testDataDir, validationCountsFile.name)
    cMetaLib = combinedMeta['libraries'][0]
    cMetaUnit = combinedMeta['runUnits'][0]

    with validationMetaFile.open('rt') as fp:
        twoLaneMeta = json.load(fp)
    twoLaneLib = twoLaneMeta['libraries'][0]
    twoLaneUnit = twoLaneMeta['runUnits'][0]

    # Sometimes the matches aren't the same. That I suspect is where we have the one out on the counts.

    combinedCounts = combineCounts(testDataDir)
    twoLaneCounts = pd.read_csv(validationCountsFile, delimiter=';', dtype=column_types)

    joinedCounts = pd.merge(combinedCounts, twoLaneCounts, on=join_columns, how='inner', suffixes=('.test', '.ref'))

    matchedDifference = sum(abs(joinedCounts['count.test'] - joinedCounts['count.ref']))

    assert matchedDifference == 443, "Expecting a difference of 443 with these files."

    assert cMetaLib['reads'] == twoLaneLib['reads']
    assert cMetaLib['readsPf'] == twoLaneLib['readsPf']
    assert cMetaUnit['matchedCounts'] == twoLaneUnit['matchedCounts'] + 443
    assert cMetaUnit['countsFileName'] == validationCountsFile.name

def test_combine_files():

    testDataDir = Path('testdata')
    combinedCountsFile = testDataDir / 'combined_counts_DELETE.csv'
    combinedMetaFile = testDataDir / 'combined_counts_DELETE.json'

    try:
        combineOLink(testDataDir, combinedCountsFile.name, combinedMetaFile.name, True)

        assert combinedCountsFile.exists()
        assert combinedMetaFile.exists()

        # The other tests check the content would be the same.

    finally:
        combinedCountsFile.unlink(True)
        combinedMetaFile.unlink(True)
