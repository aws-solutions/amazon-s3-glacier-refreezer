// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

import { Construct } from "constructs";
import { CfnResource, CustomResource, Duration, Aws } from "aws-cdk-lib";
import { aws_lambda as lambda } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_logs as logs } from "aws-cdk-lib";
import { aws_stepfunctions as sfn } from "aws-cdk-lib";
import { aws_stepfunctions_tasks as tasks } from "aws-cdk-lib";
import { aws_s3 as s3 } from "aws-cdk-lib";
import * as iamSec from "./iam-permissions";
import { GlueDataCatalog } from "./glue-data-catalog";
import { DynamoDataCatalog } from "./ddb-data-catalog";

export interface StageTwoOrchestratorProps {
    readonly stagingBucket: s3.IBucket;
    readonly glueDataCatalog: GlueDataCatalog;
    readonly dynamoDataCatalog: DynamoDataCatalog;
    readonly sendAnonymousStats: lambda.IFunction;
    readonly requestArchives: lambda.IFunction;
    readonly glueJobName: string;
}

export class StageTwoOrchestrator extends Construct {
    public readonly stateMachine: sfn.StateMachine;

    constructor(scope: Construct, id: string, props: StageTwoOrchestratorProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Tasks
        const taskStartInventoryQuery = new tasks.AthenaStartQueryExecution(this, "Start Inventory Query", {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString:
                `SELECT COUNT(1) AS archiveCount, ` +
                `COALESCE(SUM(size),0) AS vaultSize ` +
                `FROM "${props.glueDataCatalog.inventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName,
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: "$.QueryExecution",
        });

        const taskGetInventoryResults = new tasks.AthenaGetQueryResults(this, "Get Inventory Results", {
            queryExecutionId: sfn.JsonPath.stringAt("$.QueryExecutionId"),
            outputPath: "$.ResultSet.Rows[1]",
        });

        const taskStartPartitionedQuery = new tasks.AthenaStartQueryExecution(this, "Start Partitioned Query", {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString:
                `SELECT COUNT(1) AS archiveCount ` +
                `FROM "${props.glueDataCatalog.partitionedInventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName,
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: "$.QueryExecution",
        });

        const taskGetPartitionedResults = new tasks.AthenaGetQueryResults(this, "Get Partitioned Results", {
            queryExecutionId: sfn.JsonPath.stringAt("$.QueryExecutionId"),
            outputPath: "$.ResultSet.Rows[1]",
        });

        const parallelQueries = new sfn.Parallel(this, "Parallel Queries", {
            resultSelector: {
                inventoryTable: {
                    archiveCount: sfn.JsonPath.numberAt("$[0].Data[0].VarCharValue"),
                    vaultSize: sfn.JsonPath.numberAt("$[0].Data[1].VarCharValue"),
                },
                partitionedTable: {
                    archiveCount: sfn.JsonPath.numberAt("$[1].Data[0].VarCharValue"),
                },
            },
        });

        const taskGluePartitioningJob = new tasks.GlueStartJobRun(this, "Run Glue Partitioning", {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            glueJobName: props.glueJobName,
            arguments: sfn.TaskInput.fromObject({
                "--ARCHIVE_COUNT.$": "$.inventoryTable.archiveCount",
                "--VAULT_SIZE.$": "$.inventoryTable.vaultSize",
            }),
        });

        const taskUpdateMetricCount = new tasks.DynamoPutItem(this, "Update Count Metric", {
            item: {
                pk: tasks.DynamoAttributeValue.fromString("count"),
                total: tasks.DynamoAttributeValue.numberFromString(
                    sfn.JsonPath.stringAt("$.inventoryTable.archiveCount")
                ),
            },
            table: props.dynamoDataCatalog.metricTable,
            resultPath: "$.putItemResult",
        });

        const taskUpdateMetricSize = new tasks.DynamoPutItem(this, "Update Size Metric", {
            item: {
                pk: tasks.DynamoAttributeValue.fromString("volume"),
                total: tasks.DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt("$.inventoryTable.vaultSize")),
            },
            table: props.dynamoDataCatalog.metricTable,
            resultPath: "$.putItemResult",
        });

        const taskSubmitAnonymousStatistics = new tasks.LambdaInvoke(this, "Send Anonymous Statistics", {
            lambdaFunction: props.sendAnonymousStats,
            inputPath: "$.inventoryTable",
        });

        taskSubmitAnonymousStatistics.addRetry({
            interval: Duration.seconds(2),
            maxAttempts: 6,
            backoffRate: 2,
        });

        // Ignore even unrecoverable errors to avoid interfering with the main process
        taskSubmitAnonymousStatistics.addCatch(new sfn.Pass(this, "Ignore SendStats Errors", {}), {
            errors: ["States.ALL"],
        });

        const taskStartMaxPartitionQuery = new tasks.AthenaStartQueryExecution(this, "Start Max Partition Query", {
            integrationPattern: sfn.IntegrationPattern.RUN_JOB,
            queryString:
                `SELECT '{ "nextPartition": 0, "maxPartition" :' || CAST(MAX(part) AS VARCHAR) || '}'` +
                `FROM "${props.glueDataCatalog.partitionedInventoryTable.tableName}";`,
            queryExecutionContext: {
                databaseName: props.glueDataCatalog.inventoryDatabase.databaseName,
            },
            workGroup: props.glueDataCatalog.athenaWorkgroup.name,
            outputPath: "$.QueryExecution",
        });

        const taskGetMaxPartitionResult = new tasks.AthenaGetQueryResults(this, "Get Max Partition Result", {
            queryExecutionId: sfn.JsonPath.stringAt("$.QueryExecutionId"),
            resultSelector: {
                "result.$": "States.StringToJson($.ResultSet.Rows[1].Data[0].VarCharValue)",
            },
            outputPath: "$.result",
        });

        const taskRequestArchives = new tasks.LambdaInvoke(this, "Request Archives Retrieval", {
            lambdaFunction: props.requestArchives,
            outputPath: "$.Payload",
        });

        taskRequestArchives.addRetry({
            maxAttempts: 10000,
            backoffRate: 1,
            interval: Duration.seconds(15),
        });

        const taskWaitX = new sfn.Wait(this, "Wait X Seconds", {
            time: sfn.WaitTime.secondsPath("$.timeout"),
        });

        // failures
        const failOnEmptyInventory = new sfn.Fail(this, "FAIL: Inventory Empty", {
            error: "Vault Inventory Table is empty. Has it been downloaded?",
        });
        const failOnInventoryMismatch = new sfn.Fail(this, "FAIL: Inventory-Partitioned Mismatch", {
            error: "Inventory and Partitioned table counts are greater than 0 and do not match. Cannot proceed.",
        });

        // conditionals
        const isInventoryEmpty = sfn.Condition.stringEquals("$.inventoryTable.archiveCount", "0");

        const equalsPartitionedCountInventory = sfn.Condition.stringEqualsJsonPath(
            "$.inventoryTable.archiveCount",
            "$.partitionedTable.archiveCount"
        );

        const notConsistentPartitionedTable = sfn.Condition.and(
            sfn.Condition.not(sfn.Condition.stringEquals("$.partitionedTable.archiveCount", "0")),
            sfn.Condition.not(equalsPartitionedCountInventory)
        );

        const isComplete = sfn.Condition.numberGreaterThanJsonPath("$.nextPartition", "$.maxPartition");

        // branching
        const parallelPartitioning = new sfn.Parallel(this, "Parallel Partitioning and Stats update", {});
        const checkPartitionStatus = new sfn.Choice(this, "Partitioning Required ?");
        const checkInventory = new sfn.Choice(this, "Check Inventory State");

        const success = new sfn.Succeed(this, "Success");
        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator :: Graph

        const graphDefinition = parallelQueries
            .branch(taskStartInventoryQuery.next(taskGetInventoryResults))
            .branch(taskStartPartitionedQuery.next(taskGetPartitionedResults))
            .next(checkInventory);

        checkInventory
            .when(isInventoryEmpty, failOnEmptyInventory)
            .when(notConsistentPartitionedTable, failOnInventoryMismatch)
            .otherwise(checkPartitionStatus);

        checkPartitionStatus
            .when(equalsPartitionedCountInventory, taskStartMaxPartitionQuery)
            .otherwise(parallelPartitioning);

        taskUpdateMetricCount.next(taskUpdateMetricSize).next(taskSubmitAnonymousStatistics);

        parallelPartitioning
            .branch(taskGluePartitioningJob)
            .branch(taskUpdateMetricCount)
            .next(taskStartMaxPartitionQuery);

        taskStartMaxPartitionQuery.next(taskGetMaxPartitionResult).next(taskRequestArchives);

        taskRequestArchives.next(
            new sfn.Choice(this, "Is Complete?")
                .when(isComplete, success)
                .otherwise(taskWaitX.next(taskRequestArchives))
        ); // loop

        // -------------------------------------------------------------------------------------------
        // Stage Two Orchestrator
        const stageTwoOrchestratorLogGroup = logs.LogGroup.fromLogGroupName(
            this,
            "Orchestrator",
            `/aws/vendedlogs/states/${Aws.STACK_NAME}-stageTwoOrchestrator`
        );

        // Stage Two Orchestrator :: IAM
        const stageTwoOrchestratorRole = new iam.Role(this, "OrchestratorRole", {
            assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
        });

        props.stagingBucket.grantReadWrite(stageTwoOrchestratorRole);
        props.requestArchives.grantInvoke(stageTwoOrchestratorRole);
        props.sendAnonymousStats.grantInvoke(stageTwoOrchestratorRole);
        props.dynamoDataCatalog.metricTable.grantWriteData(stageTwoOrchestratorRole);
        stageTwoOrchestratorLogGroup.grantWrite(stageTwoOrchestratorRole);

        stageTwoOrchestratorRole.addToPrincipalPolicy(
            iamSec.IamPermissions.athena([
                props.glueDataCatalog.inventoryDatabase.catalogArn,
                props.glueDataCatalog.inventoryDatabase.databaseArn,
                `arn:${Aws.PARTITION}:athena:*:${Aws.ACCOUNT_ID}:workgroup/${props.glueDataCatalog.athenaWorkgroup.name}`,
                props.glueDataCatalog.inventoryTable.tableArn,
                props.glueDataCatalog.partitionedInventoryTable.tableArn,
            ])
        );

        stageTwoOrchestratorRole.addToPrincipalPolicy(
            new iam.PolicyStatement({
                sid: "allowGlueJobRun",
                effect: iam.Effect.ALLOW,
                actions: ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"],
                resources: [`arn:${Aws.PARTITION}:glue:${Aws.REGION}:${Aws.ACCOUNT_ID}:job/${props.glueJobName}`],
            })
        );

        stageTwoOrchestratorRole.addToPrincipalPolicy(
            new iam.PolicyStatement({
                sid: "allowLogDelivery",
                effect: iam.Effect.ALLOW,
                actions: [
                    "logs:CreateLogDelivery",
                    "logs:GetLogDelivery",
                    "logs:UpdateLogDelivery",
                    "logs:DeleteLogDelivery",
                    "logs:ListLogDeliveries",
                    "logs:PutResourcePolicy",
                    "logs:DescribeResourcePolicies",
                    "logs:DescribeLogGroups",
                ],
                resources: ["*"],
            })
        );

        const defaultOrchetratorPolicy = stageTwoOrchestratorRole.node.findChild("DefaultPolicy").node
            .defaultChild as CfnResource;
        defaultOrchetratorPolicy.addMetadata("cfn_nag", {
            rules_to_suppress: [
                {
                    id: "W12",
                    reason: "[*] Access granted as per documentation: https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html",
                },
                {
                    id: "W76",
                    reason: "SPCM complexity greater then 25 is appropriate for the logic implemented",
                },
            ],
        });

        // Stage Two Orchestrator :: StepFunction
        this.stateMachine = new sfn.StateMachine(this, "StageTwoOrchestrator", {
            stateMachineName: `${Aws.STACK_NAME}-stageTwoOrchestrator`,
            stateMachineType: sfn.StateMachineType.STANDARD,
            definitionBody: sfn.DefinitionBody.fromChainable(graphDefinition),
            role: stageTwoOrchestratorRole.withoutPolicyUpdates(),
            logs: {
                destination: stageTwoOrchestratorLogGroup,
                level: sfn.LogLevel.ALL,
            },
        });
        (this.stateMachine.node.defaultChild as sfn.CfnStateMachine).overrideLogicalId(`stageTwoOrchestrator`);
        this.stateMachine.node.addDependency(stageTwoOrchestratorRole);
    }
}
