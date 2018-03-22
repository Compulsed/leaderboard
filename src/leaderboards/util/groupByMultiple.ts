import * as _ from 'lodash';

export const groupByMultiple = <T>(inputCollection: Array<T>, operator: (input: T) => Array<string> | string) => {
    const keyGroups = _(inputCollection)
        .map(operator)
        .map(_.castArray)
        .map(results => results.map(_.toString))
        .value();

    const grouping = _.zipWith(
        keyGroups,
        inputCollection,
        (keyGroup, input) => [keyGroup, input] as [string[], T]
    );

    const mergeWithCustomizer = (objValue, srcValue) => {
        if (_.isArray(objValue)) {
          return objValue.concat(srcValue);
        }
    }

    return _.reduce(
        grouping,
        (acc, [keyGroup, input]) => 
            _.mergeWith(acc, _.mapValues(_.keyBy(keyGroup), () => [input]), mergeWithCustomizer),
        {} as { [s: string]: Array<T>; }
    );
}