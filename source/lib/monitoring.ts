// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

import { Construct } from "constructs";
import { CfnDeletionPolicy, Duration, Aws } from "aws-cdk-lib";
import { aws_dynamodb as dynamo } from "aws-cdk-lib";
import { aws_lambda as lambda } from "aws-cdk-lib";
import { aws_iam as iam } from "aws-cdk-lib";
import { aws_sns as sns } from "aws-cdk-lib";
import { aws_cloudwatch as cloudwatch } from "aws-cdk-lib";
import { aws_events as events } from "aws-cdk-lib";
import { aws_logs as logs } from "aws-cdk-lib";
import { aws_events_targets as targets } from "aws-cdk-lib";
import { aws_lambda_event_sources as eventsource } from "aws-cdk-lib";
import * as iamSec from "./iam-permissions";
import * as path from "path";
import * as fs from "fs";
import { CfnNagSuppressor } from "./cfn-nag-suppressor";

export interface MonitoringProps {
    readonly statusTable: dynamo.ITable;
    readonly metricTable: dynamo.ITable;
    readonly archiveNotificationTopic: sns.ITopic;
}

export class Monitoring extends Construct {
    public readonly dashboardName: string;

    constructor(scope: Construct, id: string, props: MonitoringProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Calculate Metrics
        const calculateMetricsRole = new iam.Role(this, "CalculateMetricsRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        // Declaring the policy explicitly to minimize permissions and reduce cfn_nag warnings
        const calculateMetricsRolePolicy = new iam.Policy(this, "CalculateMetricsRolePolicy", {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-calculateMetrics`),
                new iam.PolicyStatement({
                    resources: [`${props.statusTable.tableArn}/stream/*`],
                    actions: [
                        "dynamodb:ListStreams",
                        "dynamodb:DescribeStream",
                        "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator",
                        "dynamodb:ListShards",
                    ],
                }),
            ],
        });
        props.metricTable.grantReadWriteData(calculateMetricsRole);
        calculateMetricsRolePolicy.attachToRole(calculateMetricsRole);

        const statusTableEventStream = new eventsource.DynamoEventSource(props.statusTable, {
            startingPosition: lambda.StartingPosition.TRIM_HORIZON,
            parallelizationFactor: 1,
            maxBatchingWindow: Duration.seconds(30),
            batchSize: 1000,
        });

        const calculateMetrics = new lambda.Function(this, "CalculateMetrics", {
            functionName: `${Aws.STACK_NAME}-calculateMetrics`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/calculateMetrics")),
            role: calculateMetricsRole.withoutPolicyUpdates(),
            environment: {
                METRICS_TABLE: props.metricTable.tableName,
            },
            events: [statusTableEventStream],
        });
        calculateMetrics.node.addDependency(calculateMetricsRolePolicy);
        CfnNagSuppressor.addLambdaSuppression(calculateMetrics);

        // -------------------------------------------------------------------------------------------
        // Post Metrics
        const postMetricsRole = new iam.Role(this, "PostMetricsRole", {
            assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
        });

        // Declaring the policy granting access to the stream explicitly to minimize permissions
        const postMetricsRolePolicy = new iam.Policy(this, "PostMetricsRolePolicy", {
            statements: [
                new iam.PolicyStatement({
                    resources: [props.metricTable.tableArn],
                    actions: ["dynamodb:Query"],
                }),
                new iam.PolicyStatement({
                    sid: "permitPostMetrics",
                    effect: iam.Effect.ALLOW,
                    actions: ["cloudwatch:PutMetricData"],
                    resources: ["*"],
                    conditions: {
                        StringEquals: {
                            "cloudwatch:namespace": "AmazonS3GlacierReFreezer",
                        },
                    },
                }),
                iamSec.IamPermissions.lambdaLogGroup(`${Aws.STACK_NAME}-postMetrics`),
            ],
        });
        postMetricsRolePolicy.attachToRole(postMetricsRole);
        CfnNagSuppressor.addSuppression(
            postMetricsRolePolicy,
            "W12",
            "CloudWatch does not support metric ARNs. Using Namespace condition"
        );

        const postMetrics = new lambda.Function(this, "PostMetrics", {
            functionName: `${Aws.STACK_NAME}-postMetrics`,
            runtime: lambda.Runtime.NODEJS_16_X,
            handler: "index.handler",
            code: lambda.Code.fromAsset(path.join(__dirname, "../lambda/postMetrics")),
            role: postMetricsRole,
            environment: {
                METRICS_TABLE: props.metricTable.tableName,
                STATUS_TABLE: props.statusTable.tableName,
                STACK_NAME: Aws.STACK_NAME,
                ARCHIVE_NOTIFICATIONS_TOPIC: props.archiveNotificationTopic.topicName,
            },
        });
        postMetrics.node.addDependency(postMetricsRolePolicy);
        CfnNagSuppressor.addLambdaSuppression(postMetrics);

        const postMetricSchedule = new events.Rule(this, "PostMetricSchedule", {
            schedule: events.Schedule.rate(Duration.minutes(1)),
        });
        postMetricSchedule.addTarget(new targets.LambdaFunction(postMetrics));

        // -------------------------------------------------------------------------------------------
        // Dashboard

        const total = Monitoring.createRefreezerMetric("ArchiveCountTotal", "Total Archives");
        const requested = Monitoring.createRefreezerMetric("ArchiveCountRequested", "Requested from Glacier");
        const staged = Monitoring.createRefreezerMetric("ArchiveCountStaged", "Staged");
        const validated = Monitoring.createRefreezerMetric("ArchiveCountValidated", "Hashes Validated");
        const copied = Monitoring.createRefreezerMetric("ArchiveCountCompleted", "Copied to Destination");

        this.dashboardName = `${Aws.STACK_NAME}-Amazon-S3-Glacier-ReFreezer`;
        const dashboard = new cloudwatch.Dashboard(this, "glacier-refreezer-dashboard", {
            dashboardName: this.dashboardName,
        });

        // single value
        const singleValueWidget = new cloudwatch.SingleValueWidget({
            width: 24,
            height: 3,
            title: `Amazon S3 Glacier Re:Freezer Progress Metrics : ${Aws.STACK_NAME}`,
            metrics: [total, requested, staged, validated, copied],
        });

        // progress line
        const graphWidget = new cloudwatch.GraphWidget({
            title: "Timeline",
            width: 24,
            height: 6,
            view: cloudwatch.GraphWidgetView.TIME_SERIES,
            left: [total, requested, staged, validated, copied],
        });

        // Log Groups and Log Widget
        // Pre-creating all log groups explicitly to allow Log Insights search
        // Collecting logGroupNames to include into the dashboard
        const logGroupNames: string[] = [
            Monitoring.createStackLogGroup(this, "/aws/vendedlogs/states", "stageTwoOrchestrator"),
        ];

        const directoryPath = path.join(__dirname, "../lambda");
        fs.readdirSync(directoryPath).map((entry) => {
            if (fs.lstatSync(directoryPath + "/" + entry).isDirectory()) {
                logGroupNames.push(Monitoring.createStackLogGroup(this, "/aws/lambda", entry));
            }
        });

        const logWidget = new cloudwatch.LogQueryWidget({
            width: 24,
            height: 6,
            title: "Errors",
            logGroupNames,
            view: cloudwatch.LogQueryVisualizationType.TABLE,
            queryLines: [
                "fields @timestamp, @message",
                "filter @message like /error/ or @message like /Error/ or @message like /ERROR/",
                "filter @message not like /ThrottlingException/",
                "filter @message not like /Idle connections will be closed/",
                "sort by @timestamp desc",
            ],
        });

        // Oldest Retrieved but not copied to Staging Bucket Archive
        const sqsOldestMessageWidget = new cloudwatch.GraphWidget({
            title: "Oldest SQS Message",
            width: 24,
            height: 6,
            view: cloudwatch.GraphWidgetView.TIME_SERIES,
            left: [
                Monitoring.createSqsMetric(`${Aws.STACK_NAME}-archive-notification-queue`),
                Monitoring.createSqsMetric(`${Aws.STACK_NAME}-chunk-copy-queue`),
            ],
        });

        dashboard.addWidgets(singleValueWidget);
        dashboard.addWidgets(graphWidget);
        dashboard.addWidgets(logWidget);
        dashboard.addWidgets(sqsOldestMessageWidget);
    }

    private static createSqsMetric(queueName: string) {
        return new cloudwatch.Metric({
            unit: cloudwatch.Unit.NONE,
            metricName: "ApproximateAgeOfOldestMessage",
            namespace: "AWS/SQS",
            dimensionsMap: {
                QueueName: queueName,
            },
        });
    }

    private static createRefreezerMetric(metricName: string, metricLabel: string) {
        return new cloudwatch.Metric({
            unit: cloudwatch.Unit.COUNT,
            metricName,
            namespace: "AmazonS3GlacierReFreezer",
            label: metricLabel,
            dimensionsMap: {
                CloudFormationStack: Aws.STACK_NAME,
            },
            account: Aws.ACCOUNT_ID,
            statistic: cloudwatch.Statistic.MAXIMUM,
            period: Duration.seconds(300),
        });
    }

    private static createStackLogGroup(construct: Construct, prefix: string, name: string) {
        const logGroupName = `${prefix}/${Aws.STACK_NAME}-${name}`;
        const logGroup = new logs.CfnLogGroup(construct, `${name}LogGroup`, {
            logGroupName,
            retentionInDays: 90,
        });
        logGroup.addOverride("DeletionPolicy", CfnDeletionPolicy.RETAIN);

        logGroup.cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress: [
                    {
                        id: "W84",
                        reason: "The solution is a temporary, one off deployment. No sensitive or PII data logged.",
                    },
                ],
            },
        };

        return logGroupName;
    }
}
