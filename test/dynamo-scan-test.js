const AWS = require('aws-sdk-mock');
const dynamoScan = require('../src/dynamo-scan');
const defaultResults = {Count: 0, Items: []};
const scanParams = {source: 'sometable'};
const scanSpy = jasmine.createSpy();
const querySpy = jasmine.createSpy();
const writeSpy = jasmine.createSpy();
let scanResults = {Count: 0, Items: []};
let queryResults = {Count: 0, Items: []};

/*
 * TODO: things to test:
 * - if re-running, should handle newly-modified records
 */
describe('dynamo-scan', () => {
    beforeEach(() => {
        scanSpy.calls.reset();
        querySpy.calls.reset();
        writeSpy.calls.reset();
        scanResults = defaultResults;
        queryResults = defaultResults;
        let scanCalled = 0;
        AWS.mock('DynamoDB.DocumentClient', 'scan', scanSpy.and.callFake((params, cb) => {
            scanCalled++;
            if (scanCalled === 1) cb(null, scanResults);
            else cb(null, defaultResults);
        }));
        AWS.mock('DynamoDB.DocumentClient', 'query', querySpy.and.callFake((params, cb) => cb(null, queryResults)));
        AWS.mock('DynamoDB.DocumentClient', 'batchWrite', writeSpy.and.callFake((params, cb) => cb(null)));
    });
    it('updates a meta record if it exists for a user', async () => {
        scanResults = {
            Count: 2,
            Items: [
                {username: 'filmaj', startdate: '2020', polishedcompany: 'Freelance', rawcompany: ''},
                {username: 'filmaj', startdate: '#META', inferredAffiliation: 'Freelance', rawCompanyField: 'Freelance', lastUpdated: '2021'}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '#META'}));
    });
    it('creates a meta record if it does not exist for a user', async () => {
        scanResults = {
            Count: 1,
            Items: [{username: 'filmaj', startdate: '2020', polishedcompany: 'Freelance', rawcompany: ''}]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '#META'}));
    });
    it('renames the company properties on records', async () => {
        scanResults = {
            Count: 2,
            Items: [
                {username: 'filmaj', startdate: '2020', polishedcompany: 'Freelance', rawcompany: ''},
                {username: 'filmaj', startdate: '#META', inferredAffiliation: 'Freelance', rawCompanyField: 'Freelance', lastUpdated: '2021'}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '#META', raw: 'Freelance', match: 'Freelance', updated: '2020'}));
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', raw: 'Freelance', match: 'Freelance'}));
    });
    it('consolidates identical consecutive company records', async () => {
        scanResults = {
            Count: 4,
            Items: [
                {username: 'filmaj', startdate: '2020', polishedcompany: 'Freelance', rawcompany: ''},
                {username: 'filmaj', startdate: '2021', polishedcompany: 'Freelance', rawcompany: ''},
                {username: 'filmaj', startdate: '2019', polishedcompany: 'Adobe', rawcompany: ''},
                {username: 'filmaj', startdate: '2018', polishedcompany: 'Adobe', rawcompany: ''}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        const deletes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.DeleteRequest).map(w => w.DeleteRequest.Key);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', raw: 'Freelance', match: 'Freelance'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021', raw: 'Freelance', match: 'Freelance'}));
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2018', raw: 'Adobe', match: 'Adobe'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019', raw: 'Adobe', match: 'Adobe'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019'}));
    });
    it('consolidates identical consecutive company records, raw fields only', async () => {
        scanResults = {
            Count: 4,
            Items: [
                {username: 'filmaj', startdate: '2020', rawcompany: 'Freelance', polishedcompany: ''},
                {username: 'filmaj', startdate: '2021', rawcompany: 'Freelance', polishedcompany: ''},
                {username: 'filmaj', startdate: '2019', rawcompany: 'Adobe', polishedcompany: ''},
                {username: 'filmaj', startdate: '2018', rawcompany: 'Adobe', polishedcompany: ''}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        const deletes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.DeleteRequest).map(w => w.DeleteRequest.Key);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', raw: 'Freelance', match: 'Freelance'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021', raw: 'Freelance', match: 'Freelance'}));
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2018', raw: 'Adobe', match: 'Adobe'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019', raw: 'Adobe', match: 'Adobe'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019'}));
    });
    it('single affiliation with polished but no raw field should write to both new fields', async () => {
        scanResults = {
            Count: 1,
            Items: [{username: 'filmaj', startdate: '2020', polishedcompany: 'Freelance', rawcompany: ''}]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', match: 'Freelance', raw: 'Freelance'}));
    });
    it('single affiliation with raw but no polished field should write to both new fields', async () => {
        scanResults = {
            Count: 1,
            Items: [{username: 'filmaj', startdate: '2020', rawcompany: 'Freelance', polishedcompany: ''}]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', match: 'Freelance', raw: 'Freelance'}));
    });
    it('single affiliation with polished field but differing raw field should write to both new fields', async () => {
        scanResults = {
            Count: 1,
            Items: [{username: 'filmaj', startdate: '2020', rawcompany: '@Adobe', polishedcompany: 'Adobe'}]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', match: 'Adobe', raw: '@Adobe'}));
    });
    it('new affiliation is empty, but old one was not (raw only)', async () => {
        scanResults = {
            Count: 2,
            Items: [
                {username: 'filmaj', startdate: '2020', rawcompany: 'Adobe', polishedcompany: ''},
                {username: 'filmaj', startdate: '2021', rawcompany: '', polishedcompany: ''}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', raw: 'Adobe', match: 'Adobe'}));
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021', raw: null, match: null}));
    });
    it('new affiliation is empty, but old one was not (polished only)', async () => {
        scanResults = {
            Count: 2,
            Items: [
                {username: 'filmaj', startdate: '2020', polishedcompany: 'Adobe', rawcompany: ''},
                {username: 'filmaj', startdate: '2021', polishedcompany: '', rawcompany: ''}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', raw: 'Adobe', match: 'Adobe'}));
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021', raw: null, match: null}));
    });
    it('consolidates identical consecutive company records, even if some of them are empty', async () => {
        scanResults = {
            Count: 4,
            Items: [
                {username: 'filmaj', startdate: '2020', polishedcompany: '', rawcompany: ''},
                {username: 'filmaj', startdate: '2021', polishedcompany: '', rawcompany: ''},
                {username: 'filmaj', startdate: '2019', polishedcompany: 'Adobe', rawcompany: ''},
                {username: 'filmaj', startdate: '2018', polishedcompany: 'Adobe', rawcompany: ''}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        const deletes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.DeleteRequest).map(w => w.DeleteRequest.Key);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020', raw: null, match: null}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021', raw: null, match: null}));
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2018', raw: 'Adobe', match: 'Adobe'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019', raw: 'Adobe', match: 'Adobe'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019'}));
    });
    it('handles the super fucked up case that i introduced with my smartness', async () => {
        scanResults = {
            Count: 3,
            Items: [
                {username: 'filmaj', startdate: '2021', polishedcompany: 'Adobe', rawcompany: 'Adobe'},
                {username: 'filmaj', startdate: '2020', polishedcompany: null, rawcompany: 'Adobe'},
                {username: 'filmaj', startdate: '2019', polishedcompany: 'Adobe', rawcompany: ''}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        const writes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.PutRequest).map(w => w.PutRequest.Item);
        const deletes = writeSpy.calls.mostRecent().args[0].RequestItems[scanParams.source].filter(w => w.DeleteRequest).map(w => w.DeleteRequest.Key);
        expect(writes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2019', raw: 'Adobe', match: 'Adobe'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020'}));
        expect(writes).not.toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2021'}));
        expect(deletes).toContain(jasmine.objectContaining({username: 'filmaj', startdate: '2020'}));
    });
    it('should not write records in new format in case i need to rerun the script', async () => {
        scanResults = {
            Count: 3,
            Items: [
                {username: 'filmaj', startdate: '2021', match: 'Adobe', raw: 'Adobe'},
                {username: 'filmaj', startdate: '2015', match: 'Sauce Labs', raw: 'Sauce Labs'},
                {username: 'filmaj', startdate: '#META', match: 'Adobe', raw: 'Adobe'}
            ]
        };
        queryResults = scanResults;
        await dynamoScan(scanParams);
        expect(writeSpy).not.toHaveBeenCalled();
    });
});
