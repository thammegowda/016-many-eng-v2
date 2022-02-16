#!/usr/bin/env python3

import argparse
import random

random.seed(9891)
SCHEMA = 'did STRING, src STRING, eng STRING'


def is_select(did, threshold):
    parts = did.split('-')
    assert len(parts) == 5
    lang1, lang2 = parts[-2:]
    if lang1.startswith('eng'):
        assert not lang2.startswith('eng')
        src_lang = lang2
    else:
        src_lang = lang1
    src_lang = src_lang.split('_')[0]
    select = random.uniform(0, 1) < threshold.get(src_lang, 1)
    return select


def downsample_sents(spark, inp_file, out_file, threshold: dict[str,float]):
    print(f"Downsampling train_file {inp_file} ->  {out_file}")
    spark.read.csv(inp_file, sep='\t', schema=SCHEMA) \
        .rdd.map(lambda row: (row.did, row.src, row.eng)) \
        .filter(lambda row: is_select(row[0], threshold)) \
        .map('\t'.join).saveAsTextFile(out_file)
    print("Done")


def read_stats(path) -> dict[str, int]:
    res = {}
    with open(path, encoding='utf-8') as out:
        for i, line in enumerate(out):
            if i == 0:
                continue    # header
            lang, sents, *rest = line.strip().split('\t')
            if lang == 'TOTAL':
                continue
            assert lang not in res
            res[lang] = int(sents)
    return res


def lang_threshold(stats: dict[str, int], max_sents=1_000_000):
    res: dict[str, float] = {}
    for lang, total in stats.items():
        if total <= max_sents:
            res[lang] = 1
        else:
            res[lang] = max_sents / total
    return res


def main(train_in, train_out, stats, max_sents):
    stats = read_stats(stats)
    threshold = lang_threshold(stats, max_sents=max_sents)

    from myspark import spark
    downsample_sents(spark, train_in, train_out, threshold=threshold)


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-ti", "--train-in", default='train-all.cleandedupe.tok.tsv',
                        help="Training input file in TSV format: DID\\tSRC\\tENG")
    parser.add_argument("-to", "--train-out",  default="train-all.cleandedupe.downsample.tok.tsv",
                        help="Training output file")
    parser.add_argument("-si", "--stats", default="train-all.cleandedupe.tok.stats.tsv",
                        help="Stats file having LANG\\tSENTS\\t.*")
    parser.add_argument("-ms", "--max-sents", default=1_000_000,
                        help="Max sentences per language")
    args = parser.parse_args()
    return vars(args)


if __name__ == '__main__':
    main(**parse_args())

