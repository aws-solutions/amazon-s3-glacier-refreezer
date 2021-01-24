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

import {AthenaGetQueryResults} from "@aws-cdk/aws-stepfunctions-tasks";
import {Construct} from "@aws-cdk/core";
import {AthenaGetQueryResultsProps} from "@aws-cdk/aws-stepfunctions-tasks/lib/athena/get-query-results";

export interface AthenaGetQueryResultPropsSelector extends AthenaGetQueryResultsProps{
    readonly resultSelector?: object
}

/**
 * Custom Task so we can use ResultSelector
 * See https://github.com/aws/aws-cdk/issues/9904
 */
export class AthenaGetQueryResultsSelector extends AthenaGetQueryResults {
    private readonly resultSelector?: object

    constructor(scope: Construct, id: string, props: AthenaGetQueryResultPropsSelector) {
        super(scope, id, props);

        this.resultSelector = props.resultSelector
    }

    public toStateJson(): object {
        const stateJson: any = super.toStateJson();
        if (this.resultSelector !== undefined) {
            stateJson.ResultSelector = this.resultSelector
        }
        return stateJson
    }
}
