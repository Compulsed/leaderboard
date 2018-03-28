import * as _ from 'lodash';

import { supportedIntervals } from '../model';

import { TimeFacet, GenericStringFacet } from '../model';

const facetFactoryMethod = (facetKey, facetValue) => {
    const isKeyAnInterval = facetKey =>
        _.includes(supportedIntervals, facetKey)

    if (isKeyAnInterval(facetKey)) {
        return new TimeFacet(facetKey, facetValue);
    }

    return new GenericStringFacet(facetKey, facetValue);
}

export default facetFactoryMethod;