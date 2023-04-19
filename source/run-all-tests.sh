#!/bin/bash
#
# This script runs all tests for the root CDK project and Lambda functions.
# These include unit tests, integration tests, and snapshot tests.

[ "$DEBUG" == 'true' ] && set -x
set -e

source_dir="$PWD"

npx prettier --check .

echo "------------------------------------------------------------------------------"
echo "Starting Lambda Unit Tests"
echo "------------------------------------------------------------------------------"
cd $source_dir/lambda

for folder in */ ; do
    cd "$folder"
    function_name=${PWD##*/}
    cd $source_dir/lambda/$function_name
    if [ -e "package.json" ]; then
        echo "------------------------------------------------------------------------------"
        echo "Testing " $function_name " from " $PWD   
        echo "------------------------------------------------------------------------------"
        npm install --test
        npm test
        npm prune --production
    fi
    cd ..
done

cd $source_dir

echo "------------------------------------------------------------------------------"
echo "Starting CDK Unit Test"
echo "------------------------------------------------------------------------------"
npm ci && npm run test -- -u

echo "------------------------------------------------------------------------------"
echo "Unit tests complete"
echo "------------------------------------------------------------------------------"
