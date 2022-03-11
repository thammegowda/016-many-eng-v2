#!/usr/bin/env bash

set -euo pipefail

export SPARK_CPUS="${SPARK_CPUS:-8}"
export SPARK_MEM="${SPARK_MEM:-60g}"
export SPARK_LOCAL_DIR="${SPARK_TMPDIR:-/scratch2/$USER/tmp/spark}"

SRC_VOCAB_SIZE=512001   # plus 1 added due to the header line
TGT_VOCAB_SIZE=64001  


#EXP_PATH=../runs/01-tfm-768d-512k64k-all-r1
# 
printf "../runs/02-tfm-768d-9E6D-512k64k-clip1M-r1 nlcodec.clip1M
../runs/01-tfm-768d-9E6D-512k64k-full-r1 nlcodec.full\n" |
    while read exp_path vocab_pref; do

    VOCAB_SHARED=$vocab_pref.shared.vocab  #'nlcodec.full.shared.vocab'
    VOCAB_ENG=$vocab_pref.tgt.vocab        #'nlcodec.full.tgt.vocab'
    
    [[ -f $exp_path/conf.yml ]] || {
	echo "ERROR: $exp_path/conf.yml must exist"
	exit 2
    }
    [[ -d  $exp_path/data ]] || mkdir -p $exp_path/data
    [[ -f $exp_path/data/nlcodec.src.model ]] || head -$SRC_VOCAB_SIZE $VOCAB_SHARED > $exp_path/data/nlcodec.src.model
    [[ -f $exp_path/data/nlcodec.tgt.model ]] || head -$TGT_VOCAB_SIZE $VOCAB_ENG > $exp_path/data/nlcodec.tgt.model
        
    [[ -f $exp_path/_PREPARED ]] || {
	#sed -i.bak "s/\(spark.master:\) .*/\1 local[$SPARK_CPUS] /;s/\(spark.driver.memory:\) .*/\1 $SPARK_MEM /;s/\(spark.local.dir:\) .*/\1 $SPARK_LOCAL_DIR /" $exp_path/conf.yml
	OLD_DIR=$PWD
	cd $(dirname $exp_path)
	rtg-prep $(basename $exp_path)
	cd $OLD_DIR
    }
done
