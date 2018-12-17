'use strict';

const fs = require('fs-extra');
const stream = require('stream');
const csv = require('csv');
const util = require('util');

const GenericWriteStream = function (settings) {
    let theThis; if (!((theThis = this) instanceof GenericWriteStream)) { return new GenericWriteStream(settings); }
    let recordsToAdd, addedCount, lastErr;

    const addRecords = (cb) => { addedCount += recordsToAdd.length; settings.addSettings.addRecords(recordsToAdd, (err, result) => { cb(err, addedCount); }); };

    const _write = function (chunk, encoding, callback) {
        let doCall = true, err, preparedRecord;
        try { preparedRecord = settings.addSettings.prepareRecord(chunk); }
        catch (e) {
            preparedRecord = undefined; lastErr = err = e;
            theThis.emit('error', err);
        }
        if (preparedRecord) {
            recordsToAdd.push(preparedRecord);
            if (recordsToAdd.length >= settings.addSettings.chunkSize) {
                doCall = false;
                addRecords(err => {
                    recordsToAdd = []; if (!err) { callback(null); } else {
                        theThis.emit('error', err);
                        callback(lastErr = err);
                    }
                });
            }
        }
        if (doCall) { callback(err); }
        return doCall && !err;
    };

    const initialize = () => {
        addedCount = 0;
        recordsToAdd = [];
        stream.Writable.call(theThis, settings.streamSettings);
        theThis._write = _write;
        theThis.on('finish', () => {
            if (!lastErr) {
                if (recordsToAdd.length > 0) { addRecords(settings.onFinish); }
                else { settings.onFinish(undefined, addedCount); }
            }
            else {
                settings.onFinish(lastErr, 0);
            }
        });
    };

    initialize();
};
util.inherits(GenericWriteStream, stream.Writable);

const csvParseWrite = (settings, cb) => {
    let hasErrors = false, isFinished = false;
    const rs = fs.createReadStream(settings.fileName);
    const parser = csv.parse({ columns: true, relax: true, skip_lines_with_empty_values: true, trim: true, skip_empty_lines: true, relax_column_count: true });
    const onFinish = (err, result) => { if (!isFinished) { isFinished = true; cb(err, result); } /*else { console.log('finished ignored'); }*/ };
    const writer = GenericWriteStream({ addSettings: settings, streamSettings: { objectMode: true }, onFinish: onFinish });

    const onError = (source, err) => {
        if (!hasErrors) {
            //console.log('on ' + source + ' error: ' + err.message);
            hasErrors = true;
            writer.destroy();
            parser.destroy();
            rs.destroy();
            onFinish(err);
        }
    };

    writer.on('error', err => { onError('writer', err); });
    parser.on('error', err => { onError('parser', err); });
    rs.on('error', err => { onError('rs', err); });

    rs.pipe(parser).pipe(writer);
};

module.exports = {
    csvParseWrite: csvParseWrite
};
