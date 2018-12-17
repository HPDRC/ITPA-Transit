'use strict';

const db = require('../db/db');
const Promise = require('bluebird');
const sessions = require('../sessions/sessions');
const schemas = require('../db/schemas');
const geom = require('../lib/geom');

let user;
const getUser = () => { if (user === undefined) { user = require('../db/user'); } return user; };

let gtfsImport;
const getGTFSImport = () => { if (gtfsImport === undefined) { gtfsImport = require('../db/gtfsimport'); } return gtfsImport; };

const forceCreate = false;
const agencyTableSpecs = schemas.getAgencyTableSpecs(forceCreate);
const userAgencyTableSpecs = schemas.getUserAgencyTableSpecs(forceCreate);

const getCount = async (whereStr) => { return db.getTableCount(agencyTableSpecs.tableName, whereStr); };

const init = async () => {
    return db.checkCreateTable(agencyTableSpecs)
        .then(() => {
            return db.checkCreateTable(userAgencyTableSpecs);
        }).then(() => {
            return getGTFSImport().init();
        }).then(() => {
            return agencyPublications.Update();
        }).then(results => {
            return getCount();
        });
};

const getUnknownAgencyStatus = () => { return { ok: false, message: "unknown agency" }; };

const getSuccessResult = result => { return { ok: true, message: "success", result: result }; };


const AgencyConcurrency = function (settings) {
    let theThis; if (!((theThis = this) instanceof AgencyConcurrency)) { return new AgencyConcurrency(settings); }
    let agenciesBlocked;

    this.IsAgencyBlocked = prefix => { return agenciesBlocked[prefix] !== undefined; };
    this.BlockAgency = prefix => { let willStart = !theThis.IsAgencyBlocked(prefix); if (willStart) { agenciesBlocked[prefix] = true; } return willStart; };
    this.EndAgencyBlock = prefix => { let willEnd = theThis.IsAgencyBlocked(prefix); if (willEnd) { delete agenciesBlocked[prefix]; } else { console.log('ending non existing import'); } return willEnd; };

    const initialize = () => { agenciesBlocked = {}; };

    initialize();
};

const agencyConcurrency = AgencyConcurrency({});
const getAgencyConcurrency = () => { return agencyConcurrency; };

const AgencyPublications = function (settings) {
    let theThis; if (!((theThis = this) instanceof AgencyPublications)) { return new AgencyPublications(settings); }
    let allAgencyIds;
    let latestPublishedAgencyInfos;
    let agenciesPublishedDates;
    let latestPublishedInfosByAgencyId;
    let agenciesPublishedDatesByAgencyId;

    this.GetAllAgencyIds = () => { return allAgencyIds; };
    this.GetLatestPublishedAgencyInfos = () => { return latestPublishedAgencyInfos; };
    this.GetAgenciesPublishedDates = () => { return agenciesPublishedDates; };

    this.GetLatestPublisheInfoByAgencyId = agencyId => { return latestPublishedInfosByAgencyId['' + agencyId]; };

    this.GetLatestPublishDateByAgencyId = agencyId => {
        let agencyPublishedDates = theThis.GetAgencyPublishedDatesByAgencyId(agencyId);
        return agencyPublishedDates ? agencyPublishedDates[agencyPublishedDates.length - 1].published_date : undefined;
    };

    this.GetAgencyPublishedDatesByAgencyId = agencyId => { return agenciesPublishedDatesByAgencyId['' + agencyId]; };

    this.GetAgencyPublishedDateForOnDay = (agencyId, onDay) => {
        let theDate;
        let onDays = theThis.GetAgencyPublishedDatesByAgencyId(agencyId);
        if (onDays) {
            let nDays = onDays.length;
            if (nDays) {
                if (onDay >= onDays[nDays - 1].published_date) { theDate = onDays[nDays - 1].published_date; }
                else if (onDay <= onDays[0].published_date) { theDate = onDays[0].published_date; }
                else {
                    let index = geom.binarySearchForExactOrPrevEntryIndex(onDays.map(t => t.published_date), onDay, (a, b) => {
                        return a.getTime() - b.getTime();
                    });
                    theDate = onDays[index].published_date;
                }
            }
        }
        return theDate;
    };

    this.GetAgencyPublishedDateForOnDayOrLatestIfOnDayIsNil = (agencyId, onDay) => {
        return onDay ? theThis.GetAgencyPublishedDateForOnDay(agencyId, onDay) : theThis.GetLatestPublishDateByAgencyId(agencyId);
    };

    this.Update = async () => {
        return getAllAgencyIds(
        ).then(results => {
            allAgencyIds = results.map(t => t.id);
            return getGTFSImport().getLatestPublishedAgencyInfos(
                allAgencyIds
            );
        }).then(results => {
            latestPublishedAgencyInfos = results;
            latestPublishedInfosByAgencyId = latestPublishedAgencyInfos.reduce((prev, cur) => {
                let id = cur['agency_id'];
                prev['' + id] = cur;
                return prev;
            }, {});
            return getGTFSImport().getAgenciesPublishedDates(
                allAgencyIds
            );
        }).then(results => {
            agenciesPublishedDates = results;
            agenciesPublishedDatesByAgencyId = agenciesPublishedDates.reduce((prev, cur) => {
                let id = cur['agency_id'], key = '' + id;
                let toArray = prev[key];
                if (toArray === undefined) {
                    prev[key] = toArray = [];
                }
                //cur.published_date_date = new Date(cur.published_date);
                toArray.push(cur);
                return prev;
            }, {});
        });
    };

    const initialize = () => { };

    initialize();
};

const agencyPublications = AgencyPublications({});
const getAgencyPublications = () => { return agencyPublications; };

const getAgencyById = async id => {
    return db.select(agencyTableSpecs.tableName, agencyTableSpecs.fieldNames, "`id`='" + id + "'")
        .then(results => { return db.getFirstResult(results); }).catch(err => { console.log('getAgencyById: ' + err.message); return undefined; });
};

const getAgencyByPrefix = async prefix => {
    prefix = consistAgencyPrefix(prefix).prefix;
    return db.select(agencyTableSpecs.tableName, agencyTableSpecs.fieldNames, "`prefix`='" + prefix + "'")
        .then(results => { return db.getFirstResult(results); }).catch(err => { console.log('getAgencyByPrefix: ' + err.message); return undefined; });
};

const getAgencyByIdOrPrefix = async (id, prefix) => { return !id ? getAgencyByPrefix(prefix) : getAgencyById(id); };

const minAgencyPrefixLen = 3;
const maxAgencyPrefixLen = 8;

const consistAgencyPrefix = prefix => {
    prefix = prefix || "";
    prefix = prefix.toUpperCase();
    let message = "";
    let ok = prefix.length >= minAgencyPrefixLen && prefix.length <= maxAgencyPrefixLen;
    if (!ok) {
        message = "prefix length must be from " + minAgencyPrefixLen + " to " + maxAgencyPrefixLen + " characters";
    }
    else {
        if (prefix.indexOf('_') >= 0) {
            ok = false;
            message = "prefix cannot contain '_'";
        }
    }
    return { ok: ok, prefix: prefix, message: message };
};

const getConcurrencyErrorStatus = () => { return { ok: false, message: "this agency has ongoing operations" }; };

const createAgency = async (prefix) => {
    var consistedPrefix = consistAgencyPrefix(prefix);
    if (consistedPrefix.ok) {
        return db.insert(agencyTableSpecs.tableName,
            ["prefix"],
            [[[consistedPrefix.prefix]]]).then(results => {
                if (results.insertId > 0) {
                    return sessions.notifyAgencyCreated({ id: results.insertId, prefix: prefix })
                        .then(count => {
                            //console.log('agency creation notified ' + count + ' sessions');
                            return { ok: true, message: "agency created" };
                        });
                }
                else {
                    return { ok: false, message: "agency already exists" };
                }
            });
    }
    else {
        return consistedPrefix;
    }
};

const deleteAgency = async id => {
    return getAgencyById(id)
        .then(agency => {
            if (agency) {
                if (agencyConcurrency.BlockAgency(agency.id)) {
                    let user = getUser();
                    return user.getUsersInAgencyId(id)
                        .then(usersInAgency => {
                            return db.deleteQuery(agencyTableSpecs.tableName, '`id`="' + id + '"')
                                .then(() => {
                                    getGTFSImport().dropAllAgencyTables('' + id);
                                    agencyConcurrency.EndAgencyBlock(agency.id);
                                    return sessions.notifyAgencyDeleted(usersInAgency, { id: id, prefix: agency.prefix })
                                        .then(count => {
                                            //console.log('agency deletion notified ' + count + ' sessions');
                                            return { ok: true, message: "agency deleted" };
                                        });
                                }).catch((err) => {
                                    agencyConcurrency.EndAgencyBlock(agency.id);
                                    return { ok: false, message: err.message };
                                });
                        });
                }
                else {
                    return getConcurrencyErrorStatus();
                }
            }
            else {
                return { ok: false, message: "agency does not exist" };
            }
        }).then(result => {
            return result;
        });
};

const changeAgency = async (id, prefix) => {
    var consistedPrefix = consistAgencyPrefix(prefix);
    if (consistedPrefix.ok) {
        let agencyTableName = agencyTableSpecs.tableName;
        let sqlStr = 'UPDATE ?? SET ?? = ? WHERE ?? = ?;';
        let values = [
            agencyTableName, "prefix", consistedPrefix.prefix, "id", id
        ];
        return getAgencyById(id)
            .then(agency => {
                if (agency) {
                    let user = getUser();
                    return user.getUsersInAgencyId(id)
                        .then(usersInAgency => {
                            return db.queryPoolValues(
                                sqlStr, values
                            ).then(results => {
                                if (results.changedRows > 0) {
                                    return sessions.notifyAgencyChanged(usersInAgency, { id: id, prefix: prefix })
                                        .then(count => {
                                            //console.log('agency change notified ' + count + ' sessions');
                                            return { ok: true, message: "agency updated" };
                                        });
                                }
                                else { return getUnknownAgencyStatus(); }
                            }).catch(err => {
                                console.log('changeAgency: ' + err.message);
                                let message;
                                if (err.code === "ER_DUP_ENTRY") { message = "prefix already exists"; }
                                else { message = "operation failed"; }
                                return { ok: false, message: message };
                            });
                        });
                }
                else {
                    return getUnknownAgencyStatus();
                }
            });
    }
    else {
        return consistedPrefix;
    }
};

const addToAgencyResults = (agencyResults, agencyInds, agencyFieldName, objectResults, objectFieldName) => {
    let nObjectResults = objectResults ? objectResults.length : 0;
    for (let i = 0; i < nObjectResults; ++i) {
        let objResult = objectResults[i];
        let objAgencyId = objResult[objectFieldName];
        let agencyIndex = agencyInds['' + objAgencyId];
        if (agencyIndex !== undefined) {
            delete objResult[objectFieldName];
            agencyResults[agencyIndex][agencyFieldName] = objResult;
        }
    }
    return agencyResults;
};

const getAgencyIdsAndIndsFromResults = (agencyResults, agencyIdFieldName) => {
    let agencyIds = [], agencyInds = {};
    agencyResults.map((cur, index) => {
        let id = cur[agencyIdFieldName];
        agencyIds.push(id);
        agencyInds['' + id] = index;
        return undefined;
    });
    return { agencyIds: agencyIds, agencyInds: agencyInds };
};

const addAgencyInfos = async agencyResults => {
    let agencyIds = [], agencyInds = {};
    agencyResults = agencyResults.map((cur, index) => {
        let id = cur.id;
        agencyIds.push(id);
        agencyInds['' + id] = index;
        return { id: id, prefix: cur.prefix };
    });
    return getGTFSImport().getCurrentAgencyInfos(
        agencyIds
    ).then(results => {
        addToAgencyResults(agencyResults, agencyInds, "workingSetInfo", results, "agency_id");
        return getAgencyPublications().GetLatestPublishedAgencyInfos();
    }).then(results => {
        addToAgencyResults(agencyResults, agencyInds, "publishedSetInfo", results, "agency_id");
        return agencyResults;
    }).catch(err => {
        console.log('addAgencyInfos: ' + err.message);
        return agencyResults;
    });
};

const getAllAgencyIds = async (whereStr, orderByStr) => {
    if (!orderByStr) { orderByStr = "`id`"; }
    return db.select(
        agencyTableSpecs.tableName, ["id"], whereStr, orderByStr
    );
};

const listAgencies = async (whereStr, orderByStr) => {
    if (!orderByStr) { orderByStr = "`id`"; }
    return db.select(
        agencyTableSpecs.tableName, agencyTableSpecs.fieldNames, whereStr, orderByStr
    ).then(results => {
        return addAgencyInfos(results);
    });
};

let upload;
const getUpload = () => { if (upload === undefined) { upload = require('../db/upload'); } return upload; };

const importLocalFile = async (userObj, theFile, agencyId) => {
    return getAgencyById(agencyId).then(agency => { return agency ? getUpload().importLocalFile(userObj, theFile, agencyId) : getUnknownAgencyStatus(); });
};

const importRemoteFile = async (userObj, theURL, agencyId) => {
    return getAgencyById(agencyId).then(agency => { return agency ? getUpload().importRemoteFile(userObj, theURL, agencyId) : getUnknownAgencyStatus(); });
};

const publishAgency = async (userObj, agencyId) => {
    return getAgencyById(agencyId).then(agency => { return agency ? getUpload().publishCurrentDataSet(userObj, agencyId) : getUnknownAgencyStatus(); });
};

const notifyAgencyProgress = async (id, progressObj) => {
    return getAgencyById(
        id
    ).then(agency => {
        if (agency) {
            let user = getUser();
            return user.getUsersInAgencyId(id)
                .then(usersInAgency => {
                    progressObj = Object.assign(progressObj, { id: id });
                    return sessions.notifyAgencyProgress(usersInAgency, progressObj)
                        .then(count => {
                            //console.log('agency change notified ' + count + ' sessions');
                            return count;
                        });
                });
        }
        else { return 0; }
    });
};

const notifyAgencyChanged = async (id) => {
    return getAgencyById(
        id
    ).then(agency => {
        if (agency) {
            let user = getUser();
            return user.getUsersInAgencyId(id)
                .then(usersInAgency => {
                    let agencyObj = { id: id, prefix: agency.prefix };
                    return sessions.notifyAgencyChanged(usersInAgency, agencyObj)
                        .then(count => {
                            //console.log('agency change notified ' + count + ' sessions');
                            return count;
                        });
                });
        }
        else { return 0; }
    });
};

let agencyMessageNumber = 0;

const sendMessage = async (id, messageObj) => {
    return getAgencyById(
        id
    ).then(agency => {
        if (agency) {
            let user = getUser();
            return user.getUsersInAgencyId(id)
                .then(usersInAgency => {
                    messageObj = Object.assign(messageObj, { id: id, order: ++agencyMessageNumber });
                    return sessions.sendAgencyMessage(usersInAgency, messageObj)
                        .then(count => {
                            //console.log('agency change notified ' + count + ' sessions');
                            return { ok: true, message: "agency message sent" };
                        });
                });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignAgencies = async (userId, agencyId, agencyPrefix, requestedAgencyId, requestedAgencyIdInAgency) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentAgencies(
                agency.id,
                requestedAgencyId,
                requestedAgencyIdInAgency
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignRoutes = async (
    userId, agencyId, agencyPrefix, subAgencyId, subAgencyIdInAgency, requestedRouteId, requestedRouteIdInAgency,
    includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentRoutes(
                agency.id, subAgencyId, subAgencyIdInAgency, requestedRouteId, requestedRouteIdInAgency, includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds,
                decodeData, returnGeoJSON
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignShapes = async (userId, agencyId, agencyPrefix, requestedShapeId, requestedShapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentShapes(
                agency.id, requestedShapeId, requestedShapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignCalendar = async (userId, agencyId, agencyPrefix, requestedServiceId, requestedServiceIdInAgency, onDate) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentCalendar(
                agency.id, requestedServiceId, requestedServiceIdInAgency, onDate
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignCalendarDates = async (userId, agencyId, agencyPrefix, requestedServiceId, requestedServiceIdInAgency, onDate) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentCalendarDates(
                agency.id, requestedServiceId, requestedServiceIdInAgency, onDate
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignServices = async (userId, agencyId, agencyPrefix, requestedServiceId, requestedServiceIdInAgency, onDate) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentServices(
                agency.id, requestedServiceId, requestedServiceIdInAgency, onDate
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignTrips = async (userId, agencyId, agencyPrefix, requestedTripId, requestedTripIdInAgency, requestedRouteId, requestedRouteIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, requestedStopSequenceId,
    stopIds, stopIdsInAgency,
    onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
    includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentTrips(
                agency.id, requestedTripId, requestedTripIdInAgency, requestedRouteId, requestedRouteIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, requestedStopSequenceId,
                stopIds, stopIdsInAgency,
                onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
                includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignStops = async (userId, agencyId, agencyPrefix, requestedStopId, requestedStopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentStops(
                agency.id, requestedStopId, requestedStopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getDesignStopSequences = async (userId, agencyId, agencyPrefix, requestedStopSequenceId, requestedRouteId, requestedRouteIdInAgency, routeDirectionId, requestedStopId, requestedStopIdInAgency, decodeData) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            return getGTFSImport().getCurrentStopSequences(
                agency.id, requestedStopSequenceId, requestedRouteId, requestedRouteIdInAgency, routeDirectionId, requestedStopId, requestedStopIdInAgency, decodeData
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else {
            return getUnknownAgencyStatus();
        }
    });
};

const getTransitAgencyOnDate = async (agencyId, agencyPrefix, onTransitDate) => {
    return getAgencyByIdOrPrefix(
        agencyId, agencyPrefix
    ).then(agency => {
        if (agency) {
            onTransitDate = getAgencyPublications().GetAgencyPublishedDateForOnDayOrLatestIfOnDayIsNil(agency.id, onTransitDate);
            if (onTransitDate) {
                return { ok: true, agency: agency, onTransitDate: onTransitDate };
            } else { return getUnknownAgencyStatus(); }
        }
        else { return getUnknownAgencyStatus(); }
    });
};

const getTransitAgencies = async (userId, agencyId, agencyPrefix, onTransitDate, requestedAgencyId, requestedAgencyIdInAgency) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedAgencies(
                result.agency.id, result.onTransitDate, requestedAgencyId, requestedAgencyIdInAgency
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};


const getTransitCalendar = async (userId, agencyId, agencyPrefix, onTransitDate, requestedServiceId, requestedServiceIdInAgency, onDate) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedCalendar(
                result.agency.id, result.onTransitDate, requestedServiceId, requestedServiceIdInAgency, onDate
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitCalendarDates = async (userId, agencyId, agencyPrefix, onTransitDate, requestedServiceId, requestedServiceIdInAgency, onDate) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedCalendarDates(
                result.agency.id, result.onTransitDate, requestedServiceId, requestedServiceIdInAgency, onDate
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitServices = async (userId, agencyId, agencyPrefix, onTransitDate, requestedServiceId, requestedServiceIdInAgency, onDate) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedServices(
                result.agency.id, result.onTransitDate, requestedServiceId, requestedServiceIdInAgency, onDate
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitRoutes = async (
    userId, agencyId, agencyPrefix, onTransitDate, subAgencyId, subAgencyIdInAgency, requestedRouteId, requestedRouteIdInAgency,
    includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds, decodeData, returnGeoJSON) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedRoutes(
                result.agency.id, result.onTransitDate, subAgencyId, subAgencyIdInAgency, requestedRouteId, requestedRouteIdInAgency, includeDirections, includeDirectionShape, includeRouteShape, includeServiceIds, includeStopSequenceIds,
                decodeData, returnGeoJSON
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitShapes = async (userId, agencyId, agencyPrefix, onTransitDate, requestedShapeId, requestedShapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedShapes(
                result.agency.id, result.onTransitDate, requestedShapeId, requestedShapeIdInAgency, routeId, routeIdInAgency, routeDirectionId, includeOriginal, excludeSimplified, decodeData, returnGeoJSON
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitStops = async (userId, agencyId, agencyPrefix, onTransitDate, requestedStopId, requestedStopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedStops(
                result.agency.id, result.onTransitDate, requestedStopId, requestedStopIdInAgency, stopSequenceId, centerLon, centerLat, radiusInMeters
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitStopSequences = async (userId, agencyId, agencyPrefix,
    onTransitDate,
    requestedStopSequenceId, requestedRouteId, requestedRouteIdInAgency, routeDirectionId, requestedStopId, requestedStopIdInAgency, decodeData) => {
    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedStopSequences(
                result.agency.id, result.onTransitDate, requestedStopSequenceId, requestedRouteId, requestedRouteIdInAgency, routeDirectionId, requestedStopId, requestedStopIdInAgency, decodeData
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

const getTransitTrips = async (userId, agencyId, agencyPrefix, onTransitDate, requestedTripId, requestedTripIdInAgency,
    requestedRouteId, requestedRouteIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, requestedStopSequenceId,
    stopIds, stopIdsInAgency,
    onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
    includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON) => {

    return getTransitAgencyOnDate(
        agencyId, agencyPrefix, onTransitDate
    ).then(result => {
        if (result.ok) {
            return getGTFSImport().getPublishedTrips(
                result.agency.id, result.onTransitDate, requestedTripId, requestedTripIdInAgency, requestedRouteId, requestedRouteIdInAgency, routeTypeList, routeDirectionId, serviceIds, serviceIdsInAgency, requestedStopSequenceId,
                stopIds, stopIdsInAgency,
                onDate, minStartHMS, maxStartHMS, minEndHMS, maxEndHMS,
                includeStopSequences, includeStopTimes, includeStopDistances, includeStops, includeRoutes, includeShapes, includeOriginal, excludeSimplified, decodeData, returnGeoJSON
            ).then(result => {
                return getSuccessResult(result);
            });
        }
        else { return result; }
    });
};

module.exports = {
    init: init,
    notifyAgencyProgress: notifyAgencyProgress,
    publishAgency: publishAgency,
    importRemoteFile: importRemoteFile,
    importLocalFile: importLocalFile,
    sendMessage: sendMessage,
    getAgencyConcurrency: getAgencyConcurrency,
    getAgencyPublications: getAgencyPublications,
    getConcurrencyErrorStatus: getConcurrencyErrorStatus,
    getAgencyById: getAgencyById,
    getAgencyByPrefix: getAgencyByPrefix,
    createAgency: createAgency,
    changeAgency: changeAgency,
    notifyAgencyChanged: notifyAgencyChanged,
    addAgencyInfos: addAgencyInfos,
    deleteAgency: deleteAgency,
    listAgencies: listAgencies,
    getAgencyIdsAndIndsFromResults: getAgencyIdsAndIndsFromResults,

    getTransitStops: getTransitStops,
    getTransitStopSequences: getTransitStopSequences,
    getTransitTrips: getTransitTrips,
    getTransitServices: getTransitServices,
    getTransitCalendarDates: getTransitCalendarDates,
    getTransitCalendar: getTransitCalendar,
    getTransitShapes: getTransitShapes,
    getTransitRoutes: getTransitRoutes,
    getTransitAgencies: getTransitAgencies,

    getDesignStopSequences: getDesignStopSequences,
    getDesignTrips: getDesignTrips,
    getDesignServices: getDesignServices,
    getDesignCalendarDates: getDesignCalendarDates,
    getDesignCalendar: getDesignCalendar,
    getDesignShapes: getDesignShapes,
    getDesignStops: getDesignStops,
    getDesignRoutes: getDesignRoutes,
    getDesignAgencies: getDesignAgencies
};
