#!/usr/bin/env bash

set -euo pipefail

TRAIN_SRC=''
TRAIN_ENG=''

VOCAB_SHARED='nlcodec.shared.vocab'
VOCAB_ENG='nlcodec.tgt.vocab'


[[ -f ${VOCAB_SHARED}._OK ]] || {
  echo "Going to create Source+English vocab"
  nlcodec-learn -i "$TRAIN_SRC" "$TRAIN_ENG" -m VOCAB_SHARED \
      -vs -1 -l bpe -mce 100 -cv 0.999995 \
      -sm "local[$SPARK_CPUS]" -dm "$SPARK_MEM" --dedup \
       && touch ${VOCAB_SHARED}._OK
}

[[ -f ${VOCAB_ENG}._OK ]] ||
  echo "Going to create English vocab"
 nlcodec-learn -i "$TRAIN_ENG" -m $VOCAB_ENG \
      -vs -1 -l bpe -mce 100 -cv 0.999995 \
      -sm "local[$SPARK_CPUS]" -dm "$SPARK_MEM" --dedup \
       && touch ${VOCAB_ENG}._OK
}


