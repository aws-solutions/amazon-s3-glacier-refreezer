# Change Log

All notable changes to this project will be documented in this file.

## [1.1.0] - 2021-07-25
### Added
- Amazon S3 Glacier Re:Freezer detects service throttling and automatically adjusts requestArchive call rate to allow extra time to process the vault aligned to the throttled metrics
- New CloudWatch Metrics: 
  - BytesRequested
  - BytesStaged
  - BytesValidated
  - BytesCompleted
  - ThrottledBytes
  - ThrottledErrorCount
  - FailedArchivesBytes
  - FailedArchivesErrorCount

### Changed
- copyToDestination split from calculateTreehash as a separate SQS Queue and Lambda function
- downloading archives from Glacier is handled only by copyChunk function
- CloudWatch Metrics Dimension Name changed to "CloudFormationStack"
- CloudWatch Metrics metric names have been renamed as "ArchiveCount<Metric>"

## [1.0.1] - 2021-06-09
### Changed
- Retrieval requests are evenly distributed throughout the runtime period
- Updated to Athena engine version 2
- Updated CDK Version to 1.107.0
- Switched to use native StepFunctions resultSelector

## [1.0.0] - 2021-02-01
### Added
- All files, initial version
