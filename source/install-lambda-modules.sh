#!/bin/bash
# 
# This script will install packages for all lambdas for PRODUCTION (dev and test dependencies will be excluded).
# Must be run prior any cdk operation.
#
#  Arguments:
#     <no of parallel threads>
#
# Defaults to 4 - four parallel threads will run npm install
#

[ "$DEBUG" == 'true' ] && set -x
set -e

function msg() {
    l=$(echo -e "cols" | tput -S)
    s=$(printf "%-${l}s" "-")
    echo -e -n "${s// /-}\n${1}\n${s// /-}\n"
}

function package_lambda() {
    cd "$1"
    [[ -f "package.json" ]] || return

    function_name=${PWD##*/}
    msg "  ${function_name} from ${PWD}"
    npm ci --only=prod
}

echo "------------------------------------------------------------------------------"
echo "Starting Lambda npm install"
echo "------------------------------------------------------------------------------"
source_dir="$PWD"
cd $source_dir/lambda

N=${1:-4} # Uses Parallel = 1 unless other value passed as $1 argument
(
    for folder in */ ; do
        ((i=i%N)); ((i++==0)) && wait
        package_lambda "$folder" &
    done
    wait
)

cd $source_dir

