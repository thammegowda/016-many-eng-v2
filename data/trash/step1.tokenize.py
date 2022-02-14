#!/usr/bin/env python3

from datetime import datetime
from pathlib import Path
from myspark import spark
from sm_tokenizer import tokenize_eng, tokenize_src

INP_FILES = ['devs-tests-all.tsv', 'train-all-dedup.tsv']
INP_FILES = ['train.JW300.raw.v2.tsv']

SCHEMA = 'did STRING, src STRING, eng STRING'
MAX_TOKENS = 512


def is_good(text):
    if not text:
        return False
    if text.startswith('<TOKERR> '):
        return False
    if len(text.split()) > MAX_TOKENS:
        return False
    return True


def row_to_tsv(row):
    return f'{row.did}\t{row.src}\t{row.eng}'


for inp_file in INP_FILES:
    is_train = 'train' in inp_file
    out_file = inp_file.replace('.tsv', '') + '-smtok.tsv'
    if (Path(out_file) / '_SUCCESS').exists():
        print(f"WARNING {out_file} is valid; skipping")
        continue

    start = datetime.now()
    print(f"{start} :: Tokenize {inp_file} ->  {out_file}")
    rdd = spark.read.csv(inp_file, sep='\t', schema=SCHEMA).rdd
    if is_train:  # tokenize and clean up
        rdd = rdd.map(lambda row: (row.did or '', tokenize_src(row.src) or '', tokenize_eng(row.eng) or '')) \
            .filter(lambda row: row[0] and is_good(row[1]) and is_good(row[2]))
    else:  # tokenize and 1) also keep the untokenized text for evaluation;  2) do not drop/filter any record
        # we have some None values which crash our pipeline, so replace them with empty string
        rdd = rdd.map(lambda row: (row.did or '',
                                   tokenize_src(row.src) or '',
                                   tokenize_eng(row.eng) or '',
                                   row.src or '',
                                   row.eng or ''))

    rdd.map(lambda x: "\t".join(x)).saveAsTextFile(out_file)
    end = datetime.now()
    print(f"{end} :: Done {inp_file}")
    print(f"time taken: {end - start}")
