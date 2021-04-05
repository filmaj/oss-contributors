const AWS = require('aws-sdk-mock');
const dynamoScan = require('../src/dynamo-scan');
const defaultResults = {Count: 0, Items: []};
const scanParams = {source: 'sometable', index: 'someindex'};
const querySpy = jasmine.createSpy();
const writeSpy = jasmine.createSpy();
let queryResults = {Count: 0, Items: []};
let scanResults = {Count: 0, Items: []};

/*
 * TODO: things to test:
 * - today something something (same meta): updates all records
 */
describe('dynamo-queries', () => {
    beforeEach(() => {
        querySpy.calls.reset();
        writeSpy.calls.reset();
        queryResults = defaultResults;
        scanResults = defaultResults;
        AWS.mock('DynamoDB.DocumentClient', 'query', querySpy.and.callFake((params, cb) => {
            if (params.IndexName) {
                if (querySpy.calls.count() === 1) cb(null, scanResults);
                else cb(null, defaultResults);
            } else cb(null, queryResults);
        }));
        AWS.mock('DynamoDB.DocumentClient', 'batchWrite', writeSpy.and.callFake((params, cb) => cb(null)));
    });
    describe('write ops', () => {
        it('three affiliation records, two most recent are duplicates and equal raw/match, meta matches too, one should update and one should delete plus meta should update', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: 'Sauce Labs', raw: 'Sauce Labs'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2007', match: null, raw: null});
            queryResults.Items.push({username: 'filmaj', startdate: '2013', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Count = 3;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(2);
            expect(deletes.length).toEqual(1);
            const affPut = puts[0].PutRequest.Item;
            const metaPut = puts[1].PutRequest.Item;
            const del = deletes[0].DeleteRequest.Key;
            expect(affPut.username).toEqual('filmaj');
            expect(affPut.startdate).toEqual('2013');
            expect(affPut.match).toBeNull();
            expect(affPut.raw).toEqual('Sauce Labs');
            expect(metaPut.username).toEqual('filmaj');
            expect(metaPut.startdate).toEqual('#META');
            expect(metaPut.match).toBeNull();
            expect(metaPut.raw).toEqual('Sauce Labs');
            expect(del.username).toEqual('filmaj');
            expect(del.startdate).toEqual('2014');
        });
        it('one affiliation record, meta has false positive match, both should update', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: 'Sauce Labs', raw: 'Sauce Labs'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Count = 1;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(2);
            expect(deletes.length).toEqual(0);
            const affPut = puts[0].PutRequest.Item;
            const metaPut = puts[1].PutRequest.Item;
            expect(affPut.username).toEqual('filmaj');
            expect(affPut.startdate).toEqual('2014');
            expect(affPut.match).toBeNull();
            expect(affPut.raw).toEqual('Sauce Labs');
            expect(metaPut.username).toEqual('filmaj');
            expect(metaPut.startdate).toEqual('#META');
            expect(metaPut.match).toBeNull();
            expect(metaPut.raw).toEqual('Sauce Labs');
        });
        it('three affiliation records, first two should update, no deletions', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: 'Adobe', raw: '@adobe'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2009', match: 'Nitobi', raw: 'Nitobi'});
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Items.push({username: 'filmaj', startdate: '2017', match: 'Adobe', raw: '@adobe'});
            queryResults.Count = 3;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(2);
            expect(deletes.length).toEqual(0);
            const nitobiPut = puts[0].PutRequest.Item;
            const saucePut = puts[1].PutRequest.Item;
            expect(nitobiPut.username).toEqual('filmaj');
            expect(nitobiPut.startdate).toEqual('2009');
            expect(nitobiPut.match).toBeNull();
            expect(nitobiPut.raw).toEqual('Nitobi');
            expect(saucePut.username).toEqual('filmaj');
            expect(saucePut.startdate).toEqual('2014');
            expect(saucePut.match).toBeNull();
            expect(saucePut.raw).toEqual('Sauce Labs');
        });
        it('two affiliation records, only first should update', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: null, raw: null}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Items.push({username: 'filmaj', startdate: '2015', match: null, raw: null});
            queryResults.Count = 2;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(1);
            expect(deletes.length).toEqual(0);
            const put = puts[0].PutRequest.Item;
            expect(put.username).toEqual('filmaj');
            expect(put.startdate).toEqual('2014');
            expect(put.match).toBeNull();
            expect(put.raw).toEqual('Sauce Labs');
        });
        it('two affiliation records, first should update, last should delete', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: null, raw: 'Sauce Labs'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Items.push({username: 'filmaj', startdate: '2015', match: null, raw: 'Sauce Labs'});
            queryResults.Count = 2;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(1);
            expect(deletes.length).toEqual(1);
            const put = puts[0].PutRequest.Item;
            const del = deletes[0].DeleteRequest.Key;
            expect(put.username).toEqual('filmaj');
            expect(put.startdate).toEqual('2014');
            expect(put.match).toBeNull();
            expect(put.raw).toEqual('Sauce Labs');
            expect(del.username).toEqual('filmaj');
            expect(del.startdate).toEqual('2015');
        });
        it('three affiliation record, middle should update, last should delete', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: null, raw: 'Sauce Labs'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2007', match: null, raw: null});
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Items.push({username: 'filmaj', startdate: '2015', match: null, raw: 'Sauce Labs'});
            queryResults.Count = 3;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(1);
            expect(deletes.length).toEqual(1);
            const put = puts[0].PutRequest.Item;
            const del = deletes[0].DeleteRequest.Key;
            expect(put.username).toEqual('filmaj');
            expect(put.startdate).toEqual('2014');
            expect(put.match).toBeNull();
            expect(put.raw).toEqual('Sauce Labs');
            expect(del.username).toEqual('filmaj');
            expect(del.startdate).toEqual('2015');
        });
        it('three affiliation records, only middle should update', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: 'Adobe', raw: '@adobe'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2007', match: null, raw: null});
            queryResults.Items.push({username: 'filmaj', startdate: '2014', match: 'Sauce Labs', raw: 'Sauce Labs'});
            queryResults.Items.push({username: 'filmaj', startdate: '2017', match: 'Adobe', raw: '@adobe'});
            queryResults.Count = 3;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(1);
            expect(deletes.length).toEqual(0);
            const put = puts[0].PutRequest.Item;
            expect(put.username).toEqual('filmaj');
            expect(put.startdate).toEqual('2014');
            expect(put.match).toBeNull();
            expect(put.raw).toEqual('Sauce Labs');
        });
        it('two affiliation records with fields that should be nulled', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: null, raw: 'nil'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2007', match: 'nil', raw: 'nil'});
            queryResults.Items.push({username: 'filmaj', startdate: '2020', match: null, raw: 'nil'});
            queryResults.Count = 2;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            const writes = writeSpy.calls.mostRecent().args[0].RequestItems.sometable;
            const deletes = writes.filter(r => r.DeleteRequest);
            const puts = writes.filter(r => r.PutRequest);
            expect(puts.length).toEqual(1);
            expect(deletes.length).toEqual(1);
            const put = puts[0].PutRequest.Item;
            expect(put.username).toEqual('filmaj');
            expect(put.startdate).toEqual('2007');
            expect(put.match).toBeNull();
            const del = deletes[0].DeleteRequest.Key;
            expect(del.username).toEqual('filmaj');
            expect(del.startdate).toEqual('2020');
        });
    });
    describe('no ops', () => {
        it('single affiliation record, no matches should no op', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: null, raw: 'Sauce Labs'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2015', match: null, raw: 'Sauce Labs'});
            queryResults.Count = 2;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            expect(writeSpy.calls.mostRecent().args[0].RequestItems.sometable.length).toEqual(0);
        });
        it('two affiliation records, no matches should no op', async () => {
            scanResults = {
                Count: 1,
                Items: [{username: 'filmaj', startdate: '#META', match: null, raw: 'Nitobi'}]
            };
            queryResults = JSON.parse(JSON.stringify(scanResults));
            queryResults.Items.push({username: 'filmaj', startdate: '2007', match: null, raw: null});
            queryResults.Items.push({username: 'filmaj', startdate: '2008', match: null, raw: 'Nitobi'});
            queryResults.Count = 3;
            await dynamoScan(scanParams);
            await dynamoScan.flush();
            expect(writeSpy.calls.mostRecent().args[0].RequestItems.sometable.length).toEqual(0);
        });
    });
});
