#!/usr/bin/env bash

set -euo pipefail
INP=train-all.cleandedupe.downsample
OUT=train-all.cleandedupe.downsample.aug

python augment.py -si $INP.src.tok -ti $INP.eng.tok -so $OUT.src.tok -to $OUT.eng.tok -mo $OUT.meta -cp -dt -cat 1
