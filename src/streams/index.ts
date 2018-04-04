import * as BbPromise from 'bluebird';
import * as _ from 'lodash';
import * as Rx from 'rxjs';

let counter = 0;

const getScore = () => 
    BbPromise.delay(1000)
        .then(() => ({ index: ++counter }));

const write = score => BbPromise.delay(1000)
    .then(() => console.log(`Score Written: ${score}`));

const observable = Rx.Observable.create(observer => {
    getScore().then(observer.next)
});

observable.subscribe(
    value => console.log(value),
    err => {},
    () => console.log('this is the end')
);