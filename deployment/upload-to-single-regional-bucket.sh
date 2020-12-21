# 
# The script uploads template from global-s3-assets 
# and lambda packages from regional-s3-assets to a SINGLE provided bucket
#
# Arguments:
#  - BUCKET      : actual bucket name to upload to. Must match <BUCKET_BASE>-<REGION> format, where BUCKET_BASE is as defined for build-dist-s3.sh
#  - SOLUTION
#  - VERSION

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
    echo "Please provide all required parameters for the build script"
    echo "For example: ./build-s3-dist.sh fulle-bucket-name trademarked-solution-name v1.0.0"
    exit 1
fi

BUCKET=${1}
SOLUTION=${2}
VERSION=${3}

aws s3 rm s3://${BUCKET}/${SOLUTION}/${VERSION} --recursive

aws s3 cp ./global-s3-assets/   s3://${BUCKET}/${SOLUTION}/${VERSION} --recursive --acl public-read --acl bucket-owner-full-control
aws s3 cp ./regional-s3-assets/ s3://${BUCKET}/${SOLUTION}/${VERSION} --recursive --acl public-read --acl bucket-owner-full-control 

echo "------------------------------------------------------------------------------"
echo "Cloudformation temmplate link:"
echo "------------------------------------------------------------------------------"

echo "https://${BUCKET}.s3.amazonaws.com/${SOLUTION}/${VERSION}/${SOLUTION}.template"

echo ""
echo ""