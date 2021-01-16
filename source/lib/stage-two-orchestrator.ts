/*********************************************************************************************************************
 *  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
 *                                                                                                                    *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    *
 *  with the License. A copy of the License is located at                                                             *
 *                                                                                                                    *
 *      http://www.apache.org/licenses/LICENSE-2.0                                                                    *
 *                                                                                                                    *
 *  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES *
 *  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    *
 *  and limitations under the License.                                                                                *
 *********************************************************************************************************************/

/**
 * @author Solution Builders
 */

'use strict';

import * as cdk from '@aws-cdk/core';
import * as lambda from '@aws-cdk/aws-lambda';
import * as logs from '@aws-cdk/aws-logs';
import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import * as iamSec from './iam-permissions';
import {GlueDataCatalog} from "./glue-data-catalog";
import {DynamoDataCatalog} from "./ddb-data-catalog";
import {AthenaGetQueryResultsSelector} from "./athena-query-result-selector";
import {CfnNagSuppressor} from "./cfn-nag-suppressor";

export interface StageTwoOrchestratorProps {
    readonly stagingBucket: s3.IBucket;
    readonly glueDataCatalog: GlueDataCatalog;
    readonly dynamoDataCatalog: DynamoDataCatalog;
    readonly sendAnonymousStats: lambda.IFunction;
    readonly requestArchives: lambda.IFunction;
    readonly glueJobName: string;
}

export class StageTwoOrchestrator extends cdk.Construct {
    public readonly stateMachine: sfn.StateMachine;

    constructor(scope: cdk.Construct, id: string, props: StageTwoOrchestratorProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Tasks
        const taskStartInventoryQuery = new tasks.AthenaStartQueryExecution(this, 'Start Inventory Query', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString:
                `SELECT COUNT(1) AS archiveCount, ` +
                `COALESCE(SUM(size),0) AS vaultSize ` +
                `FROM "${props.glueDataCatalog.inventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: '$.QueryExecution'
        });

        const taskGetInventoryResults = new tasks.AthenaGetQueryResults(this, 'Get Inventory Results', {
            queryExecutionId: sfn.JsonPath.stringAt('$.QueryExecutionId'),
            outputPath: '$.ResultSet.Rows[1]'
        });

        const taskStartPartitionedQuery = new tasks.AthenaStartQueryExecution(this, 'Start Partitioned Query', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString:
                `SELECT COUNT(1) AS archiveCount ` +
                `FROM "${props.glueDataCatalog.partitionedInventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: '$.QueryExecution'
        });

        const taskGetPartitionedResults = new tasks.AthenaGetQueryResults(this, 'Get Partitioned Results', {
            queryExecutionId: sfn.JsonPath.stringAt('$.QueryExecutionId'),
            outputPath: '$.ResultSet.Rows[1]'
        });

        // To be replaced with ResultSelect once available for Parallel Step
        const taskMergeQueryResults = new sfn.Pass(this, 'Merge Query Results', {
            parameters: {
                inventoryTable: {
                    archiveCount: sfn.JsonPath.numberAt('$[0].Data[0].VarCharValue'),
                    vaultSize: sfn.JsonPath.numberAt('$[0].Data[1].VarCharValue')
                },
                partitionedTable: {
                    archiveCount: sfn.JsonPath.numberAt('$[1].Data[0].VarCharValue')
                }
            },
            resultPath: '$',
        });

        const taskGluePartitioningJob = new tasks.GlueStartJobRun(this, 'Run Glue Partitioning', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            glueJobName: props.glueJobName
        });

        const taskUpdateMetricTable = new tasks.DynamoPutItem(this, 'Update Metrics Table', {
            item: {
                pk: tasks.DynamoAttributeValue.fromString('count'),
                total: tasks.DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt('$.inventoryTable.archiveCount'))
            },
            table: props.dynamoDataCatalog.metricTable,
            resultPath: '$.putItemResult'
        });

        const taskSubmitAnonymousStatistics = new tasks.LambdaInvoke(this, 'Send Anonymous Statistics', {
            lambdaFunction: props.sendAnonymousStats,
            inputPath: '$.inventoryTable'
        });

        taskSubmitAnonymousStatistics.addRetry({
            interval: cdk.Duration.seconds(2),
            maxAttempts: 6,
            backoffRate: 2
        });

        // Ignore even unrecoverable errors to avoid interfering with the main process
        taskSubmitAnonymousStatistics.addCatch(new sfn.Pass(this, 'Ignore SendStats Errors', {}),
            {errors: ["States.ALL"]});

        const taskStartMaxPartitionQuery = new tasks.AthenaStartQueryExecution(this, 'Start Max Partition Query', {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString:
                `SELECT '{ "nextPartition": 0, "maxPartition" :' || CAST(MAX(part) AS VARCHAR) || '}'` +
                `FROM "${props.glueDataCatalog.partitionedInventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: '$.QueryExecution'
        });

        // const taskGetMaxPartitionResult = new tasks.AthenaGetQueryResults(this, 'Get Max Partition Result', {
        const taskGetMaxPartitionResult = new AthenaGetQueryResultsSelector(this, 'Get Max Partition Result', {
            queryExecutionId: sfn.JsonPath.stringAt('$.QueryExecutionId'),
            resultSelector: {
                'result.$': 'States.StringToJson($.ResultSet.Rows[1].Data[0].VarCharValue)'
            },
            outputPath: '$.result'
        });

        const taskRequestArchives = new tasks.LambdaInvoke(this, 'Request Archives Retrieval', {
            lambdaFunction: props.requestArchives,
            outputPath: '$.Payload'
        });

        taskRequestArchives.addRetry({
            maxAttempts: 10000,
            backoffRate: 1,
            interval: cdk.Duration.seconds(15)
        });

        // failures
        const failOnEmptyInventory = new sfn.Fail(this, 'FAIL: Inventory Empty', {error: 'Vault Inventory Table is empty. Has it been downloaded?'});
        const failOnInventoryMismatch = new sfn.Fail(this, 'FAIL: Inventory-Partitioned Mismatch', {
            error: 'Inventory and Partitioned table counts are greater than 0 and do not match. Cannot proceed.'
        });

        // conditionals
        const isInventoryEmpty = sfn.Condition.stringEquals('$.inventoryTable.archiveCount', "0");

        const equalsPartitionedCountInventory =
            sfn.Condition.stringEqualsJsonPath(
                '$.inventoryTable.archiveCount',
                '$.partitionedTable.archiveCount');

        const notConsistentPartitionedTable =
            sfn.Condition.and(
                sfn.Condition.not(sfn.Condition.stringEquals('$.partitionedTable.archiveCount', "0")),
                sfn.Condition.not(equalsPartitionedCountInventory));

        const isComplete = sfn.Condition.numberGreaterThanJsonPath('$.nextPartition', '$.maxPartition');

        // branching
        const parallelQueries = new sfn.Parallel(this, 'Parallel Queries', {});
        const parallelPartitioning = new sfn.Parallel(this, 'Parallel Partitioning and Stats update', {});
        const checkPartitionStatus = new sfn.Choice(this, 'Partitioning Required ?');
        const checkInventory = new sfn.Choice(this, 'Check Inventory State');

        const success = new sfn.Succeed(this, 'Success');
        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Graph

        const graphDefinition = parallelQueries
            .branch(taskStartInventoryQuery.next(taskGetInventoryResults))
            .branch(taskStartPartitionedQuery.next(taskGetPartitionedResults))
            .next(taskMergeQueryResults)
            .next(checkInventory);

        checkInventory
            .when(isInventoryEmpty, failOnEmptyInventory)
            .when(notConsistentPartitionedTable, failOnInventoryMismatch)
            .otherwise(checkPartitionStatus);

        checkPartitionStatus
            .when(equalsPartitionedCountInventory, taskStartMaxPartitionQuery)
            .otherwise(parallelPartitioning);

        parallelPartitioning
            .branch(taskGluePartitioningJob)
            .branch(taskUpdateMetricTable.next(taskSubmitAnonymousStatistics))
            .next(taskStartMaxPartitionQuery);

        taskStartMaxPartitionQuery
            .next(taskGetMaxPartitionResult)
            .next(taskRequestArchives);

        taskRequestArchives
            .next(new sfn.Choice(this, 'Is Complete?')
                .when(isComplete, success)
                .otherwise(taskRequestArchives)); // loop

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator
        const stageTwoOrchestratorLogGroup = logs.LogGroup.fromLogGroupName(this, 'Orchestrator', `/aws/vendedlogs/states/${cdk.Aws.STACK_NAME}-stageTwoOrchestrator`);

        // Stage Two Orchestrator :: IAM
        const stageTwoOrchestratorRole = new iam.Role(this, 'OrchestratorRole', {
            assumedBy: new iam.ServicePrincipal('states.amazonaws.com')
        });

        const stageTwoOrchestratorRolePolicy = new iam.Policy(this, 'OrchestratorRolePolicy');

        stageTwoOrchestratorRolePolicy.addStatements(
            new iam.PolicyStatement({
                sid: 'allowS3ReadWrite',
                effect: iam.Effect.ALLOW,
                actions: [
                    's3:ListBucket',
                    's3:ListBucketVersions',
                    's3:GetBucketLocation',
                    's3:GetObject',
                    's3:GetObjectAcl',
                    's3:PutObject',
                    's3:DeleteObject',
                    's3:ListBucketMultipartUploads',
                    's3:ListMultipartUploadParts',
                    's3:AbortMultipartUpload'
                ],
                resources: [
                    props.stagingBucket.bucketArn,
                    `${props.stagingBucket.bucketArn}/*`,
                ]}),
            iamSec.IamPermissions.athena(
                [
                    props.glueDataCatalog.inventoryDatabase.catalogArn,
                    props.glueDataCatalog.inventoryDatabase.databaseArn,
                    `arn:aws:athena:*:${cdk.Aws.ACCOUNT_ID}:workgroup/${props.glueDataCatalog.athenaWorkgroup.name}`,
                    props.glueDataCatalog.inventoryTable.tableArn,
                    props.glueDataCatalog.partitionedInventoryTable.tableArn
                ]
            ),
            new iam.PolicyStatement({
                sid: 'allowGlueJobRun',
                effect: iam.Effect.ALLOW,
                actions: [
                    'glue:StartJobRun',
                    'glue:GetJobRun',
                    'glue:GetJobRuns'
                ],
                resources: [`arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:job/${props.glueJobName}`],
            }),
            new iam.PolicyStatement({
                sid: 'allowLambdaInvocations',
                effect: iam.Effect.ALLOW,
                actions: [
                    'lambda:InvokeFunction'
                ],
                resources: [
                    props.requestArchives.functionArn,
                    props.sendAnonymousStats.functionArn
                ]
            }),
            new iam.PolicyStatement({
                sid: 'allowPutTotalArchiveMetric',
                effect: iam.Effect.ALLOW,
                actions: [
                    'dynamodb:PutItem'
                ],
                resources: [
                    props.dynamoDataCatalog.metricTable.tableArn
                ]
            }),
            new iam.PolicyStatement({
                sid: 'allowLogOutput',
                effect: iam.Effect.ALLOW,
                actions: [
                    'logs:CreateLogGroup',
                    'logs:CreateLogStream',
                    'logs:PutLogEvents'
                ],
                resources: [
                    stageTwoOrchestratorLogGroup.logGroupArn,
                ]
            }),
            new iam.PolicyStatement({
                sid: 'allowLogDelivery',
                effect: iam.Effect.ALLOW,
                actions: [
                    'logs:CreateLogDelivery',
                    'logs:GetLogDelivery',
                    'logs:UpdateLogDelivery',
                    'logs:DeleteLogDelivery',
                    'logs:ListLogDeliveries',
                    'logs:PutResourcePolicy',
                    'logs:DescribeResourcePolicies',
                    'logs:DescribeLogGroups'
                ],
                resources: [
                    '*'
                ]
            }),
        );
        stageTwoOrchestratorRolePolicy.attachToRole(stageTwoOrchestratorRole);
        CfnNagSuppressor.addSuppressions(stageTwoOrchestratorRolePolicy,
            [
                {
                    id: 'W12',
                    reason: '[*] Access granted as per documentation: https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html'
                },
                {
                    id: 'W76',
                    reason: 'SPCM complexity greater then 25 is appropriate for the logic implemented'
                }
            ]
        );

        // Stage Two Orchestrator :: StepFunction
        this.stateMachine = new sfn.StateMachine(this, 'StageTwoOrchestrator', {
            stateMachineName: `${cdk.Aws.STACK_NAME}-stageTwoOrchestrator`,
            stateMachineType: sfn.StateMachineType.STANDARD,
            definition: graphDefinition,
            role: stageTwoOrchestratorRole.withoutPolicyUpdates(),
            logs: {
                destination: stageTwoOrchestratorLogGroup,
                level: sfn.LogLevel.ALL,
            }
        });
        (this.stateMachine.node.defaultChild as sfn.CfnStateMachine).overrideLogicalId(`stageTwoOrchestrator`);
        this.stateMachine.node.addDependency(stageTwoOrchestratorRolePolicy);
    }
}
