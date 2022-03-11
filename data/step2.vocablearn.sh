#!/usr/bin/env bash

set -euo pipefail

export SPARK_CPUS="${SPARK_CPUS:-8}"
export SPARK_MEM="${SPARK_MEM:-60g}"
export SPARK_LOCAL_DIR="${SPARK_TMPDIR:-/scratch2/$USER/tmp/spark}"


TRAIN_SRC='train-all.cleandedupe.downsample.src.tok'
TRAIN_ENG='train-all.cleandedupe.downsample.eng.tok'

VOCAB_SHARED='nlcodec.clip1M.shared.vocab'
VOCAB_ENG='nlcodec.clip1M.tgt.vocab'




[[ -f ${VOCAB_SHARED}._OK ]] || {
    echo "Going to create Source+English vocab "
    # include almost all chars
    nlcodec-learn -i "$TRAIN_SRC" "$TRAIN_ENG" -m $VOCAB_SHARED \
                  -vs -1 -l bpe -mce 100 -cv 0.999995 \
                  -spark "local[$SPARK_CPUS]" -dm "$SPARK_MEM" --dedup \
        && touch ${VOCAB_SHARED}._OK
}

[[ -f ${VOCAB_ENG}._OK ]] || {
    echo "Going to create English vocab"
    # lower char coverage
    nlcodec-learn -i "$TRAIN_ENG" -m $VOCAB_ENG \
                  -vs -1 -l bpe -mce 100 -cv 0.9998 \
                  -spark "local[$SPARK_CPUS]" -dm "$SPARK_MEM" --dedup \
        && touch ${VOCAB_ENG}._OK
}



# FULL corpus. this might crash, but we shall try
TRAIN_SRC='train-all.cleandedupe.src.tok'
TRAIN_ENG='train-all.cleandedupe.eng.tok'

VOCAB_SHARED='nlcodec.full.shared.vocab'
VOCAB_ENG='nlcodec.full.tgt.vocab'


[[ -f ${VOCAB_SHARED}._OK ]] || {
    echo "Going to create Source+English vocab"
    # include almost all chars
    nlcodec-learn -i "$TRAIN_SRC" "$TRAIN_ENG" -m $VOCAB_SHARED \
                  -vs -1 -l bpe -mce 100 -cv 0.999995 \
                  -spark "local[$SPARK_CPUS]" -dm "$SPARK_MEM" --dedup \
        && touch ${VOCAB_SHARED}._OK
}

[[ -f ${VOCAB_ENG}._OK ]] || {
    echo "Going to create English vocab"
    # lower char coverage
    nlcodec-learn -i "$TRAIN_ENG" -m $VOCAB_ENG \
                  -vs -1 -l bpe -mce 100 -cv 0.9998 \
                  -spark "local[$SPARK_CPUS]" -dm "$SPARK_MEM" --dedup \
        && touch ${VOCAB_ENG}._OK
}
