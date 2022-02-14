#!/usr/bin/env bash

# this is large memory, useful for deduplication steps
## SBATCH -p largemem -t 1-00:00:00 --mem=960G -N 1 -n 1 -c 64
#SBATCH -p largemem -t 1-00:00:00 --mem=500G -N 1 -n 1 -c 50

#SBATCH --output=R-%x.out.%j --error=R-%x.err.%j  --export=NONE


source ~/.bashrc
conda activate rtg-py39  # conda env should have pyspark installed


# EDIT THESE
export SPARK_CPUS=60
export SPARK_MEM="500G"
export SPARK_TMPDIR="/scratch2/tnarayan/tmp/spark"


script="$1"
shift
args=( $@ )
echo "$script ${args[@]}"
if [[ ! -f $script ]] ; then
    echo "script $script not found"
    exit 2
fi
$script ${args[@]}

