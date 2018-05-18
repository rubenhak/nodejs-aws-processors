const _ = require('the-lodash');
const Promise = require('the-promise');

class KinesisConsumer
{
    constructor(kinesis) {
        this._kinesis = kinesis;
    }

    shouldParseJson(value) {
        this._shouldParseJson = value;
        return this;
    }

    streamName(value) {
        this._streamName = value;
        return this;
    }

    handler(value) {
        this._dataHandler = value;
        return this;
    }

    process() {
        return Promise.resolve()
            .then(() => this._selectShard())
            .then(() => this._setupInitialIterator())
            .then(() => this._processShard());

    }

    _selectShard() {
        return this._kinesis.describeStream({StreamName: this._streamName}).promise()
            .then(streamData => {
                this._shardId = streamData.StreamDescription.Shards[0].ShardId;
            });
    }

    _setupInitialIterator() {
        var params = {
            ShardId: this._shardId,
            StreamName: this._streamName,
            ShardIteratorType: 'TRIM_HORIZON'
        };
        return this._kinesis.getShardIterator(params).promise()
            .then(shardIteratorData => {
                this._shardIterator = shardIteratorData.ShardIterator
            });
    }

    _processShard() {
        return this._kinesis.getRecords({ShardIterator: this._shardIterator}).promise()
            .then(data => {
                return Promise.serial(data.Records, x => this._processRecord(x))
                    .then(() => Promise.timeout(1000))
                    .then(() => { this._shardIterator = data.NextShardIterator; })
            })
            .then(() => this._processShard());
    }

    _processRecord(record)
    {
        var data = record.Data;
        if (this._shouldParseJson) {
            data = JSON.parse(data);
        }
        return Promise.resolve(this._dataHandler(data));
    }
}

module.exports = KinesisConsumer;
