// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

/**
 * @author Solution Builders
 */

"use strict";

import { StackProps } from "aws-cdk-lib";

export interface SolutionStackProps extends StackProps {
    readonly solutionId: string;
    readonly description: string;
    readonly solutionName: string;
}
