/*********************************************************************************************************************
 *  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           *
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
import * as cloudwatch from '@aws-cdk/aws-cloudwatch';
import * as lambda from '@aws-cdk/aws-lambda';
import * as events from "@aws-cdk/aws-events";
import * as logs from "@aws-cdk/aws-logs";
import * as targets from "@aws-cdk/aws-events-targets";
import * as eventsource from '@aws-cdk/aws-lambda-event-sources';
import * as iam from '@aws-cdk/aws-iam';
import * as sns from '@aws-cdk/aws-sns';
import * as iamSec from './iam-permissions';
import * as path from 'path';
import * as fs from 'fs';
import * as dynamo from "@aws-cdk/aws-dynamodb";
import {CfnNagSuppressor} from "./cfn-nag-suppressor";

export interface MonitoringProps {
    readonly statusTable: dynamo.ITable,
    readonly metricTable: dynamo.ITable,
    readonly archiveNotificationTopic: sns.ITopic
}

export class Monitoring extends cdk.Construct {

    public readonly dashboardName: string;

    constructor(scope: cdk.Construct, id: string, props: MonitoringProps) {
        super(scope, id);

        // -------------------------------------------------------------------------------------------
        // Calculate Metrics
        const calculateMetricsRole = new iam.Role(this, 'CalculateMetricsRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        // Declaring the policy explicitly to minimize permissions and reduce cfn_nag warnings
        const calculateMetricsRolePolicy = new iam.Policy(this, 'CalculateMetricsRolePolicy', {
            statements: [
                iamSec.IamPermissions.lambdaLogGroup(`${cdk.Aws.STACK_NAME}-calculateMetrics`),
                new iam.PolicyStatement({
                    resources: [
                        `${props.statusTable.tableArn}/stream/*`
                    ],
                    actions: [
                        'dynamodb:ListStreams',
                        'dynamodb:DescribeStream',
                        'dynamodb:GetRecords',
                        'dynamodb:GetShardIterator',
                        'dynamodb:ListShards'
                    ]
                })
            ]
        });
        props.metricTable.grantReadWriteData(calculateMetricsRole);
        calculateMetricsRolePolicy.attachToRole(calculateMetricsRole);

        const statusTableEventStream = new eventsource.DynamoEventSource(props.statusTable, {
            startingPosition: lambda.StartingPosition.TRIM_HORIZON,
            parallelizationFactor: 1,
            maxBatchingWindow: cdk.Duration.seconds(30),
            batchSize: 1000
        });

        const calculateMetrics = new lambda.Function(this, 'CalculateMetrics', {
            functionName: `${cdk.Aws.STACK_NAME}-calculateMetrics`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/calculateMetrics')),
            role: calculateMetricsRole.withoutPolicyUpdates(),
            environment:
                {
                    METRICS_TABLE: props.metricTable.tableName
                },
            events: [statusTableEventStream]
        });
        calculateMetrics.node.addDependency(calculateMetricsRolePolicy);
        CfnNagSuppressor.addLambdaSuppression(calculateMetrics);

        // -------------------------------------------------------------------------------------------
        // Post Metrics
        const postMetricsRole = new iam.Role(this, 'PostMetricsRole', {
            assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com')
        });

        // Declaring the policy granting access to the stream explicitly to minimize permissions
        const postMetricsRolePolicy = new iam.Policy(this, 'PostMetricsRolePolicy', {
            statements: [
                new iam.PolicyStatement({
                    resources: [props.metricTable.tableArn],
                    actions: ['dynamodb:Query']
                }),
                new iam.PolicyStatement({
                    sid: 'permitPostMetrics',
                    effect: iam.Effect.ALLOW,
                    actions: ['cloudwatch:PutMetricData'],
                    resources: ['*'],
                    conditions: {
                        StringEquals: {
                            'cloudwatch:namespace': 'AmazonS3GlacierReFreezer'
                        }
                    }
                }),
                iamSec.IamPermissions.lambdaLogGroup(`${cdk.Aws.STACK_NAME}-postMetrics`)
            ]
        });
        postMetricsRolePolicy.attachToRole(postMetricsRole);
        CfnNagSuppressor.addSuppression(postMetricsRolePolicy, 'W12', 'CloudWatch does not support metric ARNs. Using Namespace condition');

        const postMetrics = new lambda.Function(this, 'PostMetrics', {
            functionName: `${cdk.Aws.STACK_NAME}-postMetrics`,
            runtime: lambda.Runtime.NODEJS_14_X,
            handler: 'index.handler',
            code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/postMetrics')),
            role: postMetricsRole,
            environment:
                {
                    METRICS_TABLE: props.metricTable.tableName,
                    STATUS_TABLE: props.statusTable.tableName,
                    STACK_NAME: cdk.Aws.STACK_NAME,
                    ARCHIVE_NOTIFICATIONS_TOPIC: props.archiveNotificationTopic.topicName
                }
        });
        postMetrics.node.addDependency(postMetricsRolePolicy);
        CfnNagSuppressor.addLambdaSuppression(postMetrics);

        const postMetricSchedule = new events.Rule(this, 'PostMetricSchedule', {
            schedule: {
                expressionString: 'rate(1 minute)'
            }
        });
        postMetricSchedule.addTarget(new targets.LambdaFunction(postMetrics));

        // -------------------------------------------------------------------------------------------
        // Dashboard

        const total = Monitoring.createRefreezerMetric('ArchiveCountTotal', 'Total Archives');
        const requested = Monitoring.createRefreezerMetric('ArchiveCountRequested', 'Request from Glacier');
        const staged = Monitoring.createRefreezerMetric('ArchiveCountStaged', 'Staged');
        const validated = Monitoring.createRefreezerMetric('ArchiveCountValidated', 'Hashes Validated');
        const copied = Monitoring.createRefreezerMetric('ArchiveCountCompleted', 'Copied to Destination');

        this.dashboardName = `${cdk.Aws.STACK_NAME}-Amazon-S3-Glacier-ReFreezer`;
        const dashboard = new cloudwatch.Dashboard(this, 'glacier-refreezer-dashboard',
            {
                dashboardName: this.dashboardName,
            });

        // single value
        const singleValueWidget = new cloudwatch.SingleValueWidget({
            width: 24,
            height: 3,
            title: `Amazon S3 Glacier Re:Freezer Progress Metrics : ${cdk.Aws.STACK_NAME}`,
            metrics: [
                total,
                requested,
                staged,
                validated,
                copied
            ]
        });

        // progress line
        const graphWidget = new cloudwatch.GraphWidget({
            title: 'Timeline',
            width: 24,
            height: 6,
            view: cloudwatch.GraphWidgetView.TIME_SERIES,
            left: [
                total,
                requested,
                staged,
                validated,
                copied
            ]
        });

        // Log Groups and Log Widget
        // Pre-creating all log groups explicitly to allow Log Insights search
        // Collecting logGroupNames to include into the dashboard
        const logGroupNames: string[] = [
            Monitoring.createStackLogGroup(this, '/aws/vendedlogs/states', 'stageTwoOrchestrator')
        ];

        const directoryPath = path.join(__dirname, '../lambda');
        fs.readdirSync(directoryPath).map(entry => {
            if (fs.lstatSync(directoryPath + '/' + entry).isDirectory()) {
                logGroupNames.push(Monitoring.createStackLogGroup(this, '/aws/lambda', entry))
            }
        });

        const logWidget = new cloudwatch.LogQueryWidget({
            width: 24,
            height: 6,
            title: 'Errors',
            logGroupNames,
            view: cloudwatch.LogQueryVisualizationType.TABLE,
            queryLines: [
                'fields @timestamp, @message ',
                'filter @message like /error/ or @message like /Error/ or @message like /ERROR/',
                'sort by @timestamp desc'
            ]
        });

        // Oldest Retrieved but not copied to Staging Bucket Archive
        const sqsOldestMessageWidget = new cloudwatch.GraphWidget({
            title: 'Oldest SQS Message',
            width: 24,
            height: 6,
            view: cloudwatch.GraphWidgetView.TIME_SERIES,
            left: [
                Monitoring.createSqsMetric(`${cdk.Aws.STACK_NAME}-archive-notification-queue`),
                Monitoring.createSqsMetric(`${cdk.Aws.STACK_NAME}-chunk-copy-queue`)
            ]
        });

        dashboard.addWidgets(singleValueWidget);
        dashboard.addWidgets(graphWidget);
        dashboard.addWidgets(logWidget);
        dashboard.addWidgets(sqsOldestMessageWidget);
    }

    private static createSqsMetric(queueName: string) {
        return new cloudwatch.Metric({
            unit: cloudwatch.Unit.NONE,
            metricName: 'ApproximateAgeOfOldestMessage',
            namespace: 'AWS/SQS',
            dimensions: {
                'QueueName': queueName
            }
        });
    }

    private static createRefreezerMetric(metricName: string, metricLabel: string) {
        return new cloudwatch.Metric({
            unit: cloudwatch.Unit.COUNT,
            metricName,
            namespace: 'AmazonS3GlacierReFreezer',
            label: metricLabel,
            dimensions: {
                'CloudFormationStack': cdk.Aws.STACK_NAME
            },
            account: cdk.Aws.ACCOUNT_ID,
            statistic: cloudwatch.Statistic.MAXIMUM,
            period: cdk.Duration.seconds(300)
        });
    }

    private static createStackLogGroup(construct: cdk.Construct, prefix: string, name: string) {
        const logGroupName = `${prefix}/${cdk.Aws.STACK_NAME}-${name}`;
        const logGroup = new logs.CfnLogGroup(construct, `${name}LogGroup`, {
            logGroupName,
            retentionInDays: 90
        });
        logGroup.addOverride('DeletionPolicy',cdk.CfnDeletionPolicy.RETAIN);

        logGroup.cfnOptions.metadata = {
            cfn_nag: {
                rules_to_suppress:
                    [{
                        id: 'W84',
                        reason: 'The solution is a temporary, one off deployment. No sensitive or PII data logged.'
                    }]
            }
        };

        return logGroupName;
    }
}
