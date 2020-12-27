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