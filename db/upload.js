'use strict';

const path = require('path');
const Promise = require("bluebird");
const fs = require('fs-extra');
const csv = require('csv');
const extractZip = require('extract-zip');
const fetch = require('node-fetch');
var parse_url = require('url').parse;

const gtfsImport = require('../db/gtfsimport');

const uploadsFolderName = 'uploads';
const unzippedFolderName = 'unzipped';

const getZipFileName = agencyId => { return path.resolve(uploadsFolderName, agencyId + '.zip'); };
const getUnzipDir = agencyId => { return path.resolve(unzippedFolderName + '/' + agencyId); };

const emptyDir = dirName => { fs.emptyDir(dirName).then(() => { console.log('emptied ' + dirName); }).catch((err) => { console.log(err.message); }); };
const emptyDirSync = dirName => { try { fs.emptyDirSync(dirName); } catch (e) { console.log('error emptying ' + dirName + ' ' + e); } };
const createDirIfNotExists = dirName => { try { if (!fs.existsSync(dirName)) { fs.mkdirSync(dirName); } } catch (e) { console.log('error creating ' + dirName + ' ' + e); } }; 
const removeLocalFile = fileName => { try { fs.removeSync(fileName); } catch (e) { console.log('error removing ' + fileName + ' ' + e); } };

const getErrorStatus = message => { return { ok: false, message: message ? message : "failed" }; };
const getInternalErrorStatus = (err) => { if (err) { console.log(err.message); } return getErrorStatus("unexpected internal server error"); };
const getBadURLErrorStatus = () => { return getErrorStatus("unable to access GTFS content at the provided url"); };
const getOKStatus = message => { return { ok: true, message: message ? message : 'done' }; };

let agency;
const getAgency = () => { if (agency === undefined) { agency = require('../db/agency'); } return agency; };
const getAgencyConcurrency = () => { return getAgency().getAgencyConcurrency(); };

const prepareLocalZipFileForImport = async agencyId => {
    return new Promise((resolve, reject) => {
        try {
            let zipFileName = getZipFileName(agencyId);
            let unzipDir = getUnzipDir(agencyId);
            createDirIfNotExists(unzipDir);
            emptyDirSync(unzipDir);
            extractZip(zipFileName, { dir: unzipDir }, err => {
                removeLocalFile(zipFileName);
                if (err) {
                    console.log(err.message);
                    resolve(getErrorStatus('invalid .zip file'));
                }
                else { resolve(getOKStatus()); }
            });
        }
        catch (err) { resolve(getInternalErrorStatus(err)); }
    });
};

const onImportResults = (unzipDir, agencyId, importResults) => {
    emptyDir(unzipDir);
    let agencyConcurrency = getAgencyConcurrency();
    agencyConcurrency.EndAgencyBlock(agencyId);
    if (importResults.messages.length <= 100) { console.log(importResults); }
    console.log('import: ' + importResults.success);
};

const importLocalFile = async (userObj, theFile, agencyId) => {
    return new Promise((resolve, reject) => {
        let agencyConcurrency = getAgencyConcurrency();
        if (agencyConcurrency.BlockAgency(agencyId)) {
            let zipFileName = getZipFileName(agencyId);
            theFile.mv(zipFileName, function (err) {
                if (err) {
                    agencyConcurrency.EndAgencyBlock(agencyId);
                    resolve(getInternalErrorStatus(err));
                }
                else {
                    prepareLocalZipFileForImport(
                        agencyId
                    ).then(results => {
                        if (results.ok) {
                            let unzipDir = getUnzipDir(agencyId);
                            gtfsImport.GTFSImport({ unzipDir: unzipDir, gtfsAgencyId: '' + agencyId, userObj: userObj }).Import(
                            ).then((results) => {
                                onImportResults(unzipDir, agencyId, results);
                            });
                            resolve(getOKStatus("upload has started"));
                        }
                        else {
                            agencyConcurrency.EndAgencyBlock(agencyId);
                            resolve(results);
                        }
                    });
                }
            });
        }
        else {
            resolve(getAgency().getConcurrencyErrorStatus());
        }
    });
};

const importRemoteFile = async (userObj, theURL, agencyId) => {
    return new Promise((resolve, reject) => {
        let agencyConcurrency = getAgencyConcurrency();
        if (agencyConcurrency.BlockAgency(agencyId)) {
            let parsedURL = parse_url(theURL);
            let protocol = parsedURL.protocol;
            let hostName = parsedURL.hostname;
            if (!!protocol && !!hostName) {
                getAgency().notifyAgencyProgress(agencyId, { order: 0, message: "Fetch operation has started" });
                return new fetch(
                    theURL
                ).then(result => {
                    let zipFileName = getZipFileName(agencyId);
                    let dest = fs.createWriteStream(zipFileName);
                    let stream = result.body.pipe(dest);
                    stream.on('error', (err) => {
                        agencyConcurrency.EndAgencyBlock(agencyId);
                        getAgency().notifyAgencyProgress(agencyId, { order: 0, message: "internal server error reading file stream" });
                    });
                    stream.on('finish', () => {
                        prepareLocalZipFileForImport(
                            agencyId
                        ).then(results => {
                            if (results.ok) {
                                let unzipDir = getUnzipDir(agencyId);
                                gtfsImport.GTFSImport({ unzipDir: unzipDir, gtfsAgencyId: '' + agencyId, userObj: userObj }).Import(
                                ).then((results) => {
                                    onImportResults(unzipDir, agencyId, results);
                                });
                            }
                            else {
                                //console.log('importRemoteFile: ' + results.message);
                                agencyConcurrency.EndAgencyBlock(agencyId);
                                getAgency().notifyAgencyProgress(agencyId, { order: 0, message: results.message });
                            }
                        });
                    });
                    resolve(getOKStatus("fetch from '" + theURL + "'has started"));
                }).catch(err => {
                    agencyConcurrency.EndAgencyBlock(agencyId);
                    resolve(getBadURLErrorStatus());
                });
            }
            else {
                agencyConcurrency.EndAgencyBlock(agencyId);
                resolve(getBadURLErrorStatus());
            }
        }
        else {
            resolve(getAgency().getConcurrencyErrorStatus());
        }
    });
};

const publishCurrentDataSet = async (userObj, agencyId) => {
    let agencyConcurrency = getAgencyConcurrency();
    if (agencyConcurrency.BlockAgency(agencyId)) {
        gtfsImport.GTFSPublish({ gtfsAgencyId: '' + agencyId, userObj: userObj }).Publish(
        ).then(results => {
            agencyConcurrency.EndAgencyBlock(agencyId);
            //console.log('publish: ' + results);
        });
        return getOKStatus("publish operation has started");
    }
    else {
        return getConcurrencyErrorStatus();
    }
};

module.exports = {
    publishCurrentDataSet: publishCurrentDataSet,
    importRemoteFile: importRemoteFile,
    importLocalFile: importLocalFile
};
