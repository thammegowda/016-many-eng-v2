#!/usr/bin/env python3

import unicodedata
import html
import hashlib
import sys
import argparse
from tqdm import tqdm

#from myspark import spark
from sm_tokenizer import tokenize_src, tokenize_eng, TOK_ERR_PREF

MAX_CHARS = 1024
LEN_RATIO = 5
MAX_TOKS = 512
SCHEMA = 'did STRING, src STRING, eng STRING'


def read_tsv(path):
    with open(path, 'r', encoding='utf-8', errors='ignore') as rdr:
        for line in rdr:
            yield line.rstrip('\n').split('\t')


def hash_text(text):
    return int(hashlib.md5(text.encode('utf-8')).hexdigest(), 16)


def clean(text):
    if not text:
        return ''
    text = ' '.join(text.replace('\r', ' ').replace('\t', ' ').replace('\n', ' ').split()).strip()
    text = html.unescape(text)
    text = unicodedata.normalize('NFKC', text)
    return text


def hash_tsv_segs(path):
    mem = set()
    for id_, src, eng in tqdm(read_tsv(path), desc='Hashing heldout sents'):
        mem.add(hash_text(clean(src)))
        mem.add(hash_text(clean(eng)))
    return mem


def row_to_tsv(row):
    return f'{row.did}\t{row.src}\t{row.eng}'


def is_good(src, eng, exclude_hashes):
    if not src or not eng:  # empty
        return False
    if src == eng:  # copy
        return False
    if max(len(src), len(eng)) >= MAX_CHARS:  # too long
        return False

    ratio = len(src) / len(eng)
    if ratio < 1 / LEN_RATIO or ratio > LEN_RATIO:
        return False
    for pattn in ['http', 'href', 'HTTP']:
        if pattn in src or pattn in eng:
            return False

    # english sentence has many non-ascii chars
    if sum(1 for x in eng if ord(x) > 255) >= 0.25 * len(eng):
        return False

    # dev or test sentence
    if hash_text(src) in exclude_hashes or hash_text(eng) in exclude_hashes:
        return False

    # could not reject with any known rules ==> good
    return True


def is_valid_row(row):
    if len(row) != 3:  # DID, SRC, ENG
        return False
    did, src, eng = row
    if not did or not src or not eng:
        return False
    if not len(did.split('-')) != 5:  # Group-name-version-lang1-lang2
        return False
    if src.startswith(TOK_ERR_PREF) or eng.startswith(TOK_ERR_PREF):
        return False
    if len(src.split()) >= MAX_TOKS and len(eng.split()) > MAX_TOKS:
        return False
    return True


def prepare_train(spark, inp_file, out_file, exclude_hashes):
    #if isinstance(inp_file, list):
    #    inp_file = ','.join(inp_file)
    assert isinstance(inp_file, (str,list))
    assert isinstance(out_file, str)
    assert isinstance(exclude_hashes, set)

    print(f"Clean train_file {inp_file} ->  {out_file}")
    
    spark.read.csv(inp_file, sep='\t', schema=SCHEMA) \
        .rdd.map(lambda row: (row.did, clean(row.src), clean(row.eng))) \
        .filter(lambda row: is_good(src=row[1], eng=row[2], exclude_hashes=exclude_hashes)) \
        .toDF(schema=SCHEMA).dropDuplicates(['src', 'eng']).rdd \
        .map(lambda row: (row.did, tokenize_src(row.src), tokenize_eng(row.eng))) \
        .filter(lambda row: is_valid_row(row)) \
        .map('\t'.join).saveAsTextFile(out_file)
    print("Done")


def prepare_held_out(spark, inp_file, out_file):
    print(f"Tokenize held-out file {inp_file} ->  {out_file}")
    spark.read.csv(inp_file, sep='\t', schema=SCHEMA) \
        .rdd.map(lambda row: (
            row.did or '', tokenize_src(row.src) or '', tokenize_eng(row.eng) or '',
            (row.src or '').replace('\r', ' ').replace('\t', ' '),
            (row.eng or '').replace('\r', ' ').replace('\t', ' '))) \
        .map('\t'.join).saveAsTextFile(out_file)
    print("Done")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-ti", "--train-in",  nargs='+', help="Training input file in TSV format: DID\\tSRC\\tENG")
    parser.add_argument("-to", "--train-out",  required=True, help="Training output file")
    parser.add_argument("-hi", "--heldout-in", help="Dev and Test Heldout input file: DID\\tSRC\\tENG")

    args = parser.parse_args()
    return vars(args)


def main(train_in, train_out, heldout_in):

    #inp_file = 'train-all.tsv'
    #out_file = 'train-all.prepared.tsv'
    #held_out_file = 'devs-tests-all.tsv'  # these sentences are to be excluded
    from myspark import spark
    exclude_hashes = heldout_in and hash_tsv_segs(heldout_in) or set()
    prepare_train(spark, train_in, train_out, exclude_hashes=exclude_hashes)

    heldout_out = heldout_in.replace('.tsv', '') + '.tok.tsv'
    prepare_held_out(spark, heldout_in, heldout_out)


if __name__ == '__main__':
    main(**parse_args())

