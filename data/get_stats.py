#!/usr/bin/env python3

import argparse
from datetime import datetime

delim = '\t'
SCHEMA = 'did STRING, src STRING, eng STRING'


def get_src_lang(did):
    if not did:
        return 'ERROR'
    parts = did.split('-')
    if len(parts) != 5:
        return 'ERROR'
    l1, l2 = parts[3], parts[4]

    if 'eng' in l1:
        src = l2
    else:
        src = l1
    src = src.split('_')[0]  # ISO 639-3 only
    return src


def get_row_stats(row):
    src_lang = get_src_lang(row.did)
    src_toks = len((row.src or '').split())
    eng_toks = len((row.eng or '').split())
    return src_lang, (1, src_toks, eng_toks)


def main(inp_file, out_file, spark=None):
    start = datetime.now()
    if spark is None:
        from myspark import spark

    stats = spark.read.csv(inp_file, sep='\t', schema=SCHEMA) \
        .rdd \
        .map(get_row_stats) \
        .reduceByKey(lambda a, b: tuple(ai + bi for ai, bi in zip(a, b))) \
        .collectAsMap()

    stats = list(sorted(stats.items(), key=lambda x: x[1][0], reverse=True))
    total = [0, 0, 0]
    for _, v in stats:
        total[0] += v[0]
        total[1] += v[1]
        total[2] += v[2]

    header = f'Language{delim}Sentences{delim}SourceToks{delim}EnglishToks'
    with open(out_file, 'w') as out:
        out.write(header + '\n')
        tstat = delim.join(str(x) for x in total)
        out.write(f'TOTAL{delim}{tstat}\n')
        for lang, lstat in stats:
            lstat = delim.join(str(x) for x in lstat)
            out.write(f'{lang}{delim}{lstat}\n')

    end = datetime.now()
    print(f"{end} :: Done {inp_file}")
    print(f"time taken: {end - start}")


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--inp-file",  default='train-all-dedup-smtok-nourl.tsv',
                        help="Input TSV file having DID\\tSRC\\tENG per line")
    parser.add_argument("-o", "--out-file", help="Stats output file. Default: <inp_file>.stats.tsv")
    args = vars(parser.parse_args())
    if not args.get('out_file'):
        args['out_file'] = args.get('inp_file').replace('.tsv', '') + '.stats.tsv'
    return args


if __name__ == '__main__':
    main(**vars(parse_args()))
