'use strict';

const bcrypt = require('bcrypt');
const db = require('../db/db');
const sessions = require('../sessions/sessions');
const Promise = require('bluebird');
const schemas = require('../db/schemas');

const adminUserType = 1;
const regularUserType = 2;

const adminUserId = 1;
const adminInitialEmail = "admin@admin.admin";
const adminInitialPassword = "1@Admin.";

const saltRounds = 10;

const changesCannotBeImplementedAsRequested = 'changes cannot not be implemented as requested';

const forceCreate = false;
const usersTableSpecs = schemas.getUserTableSpecs(regularUserType, forceCreate);

const isUserAdmin = user => { return !!user && user.type === adminUserType; };
const getUserType = isAdmin => { return isAdmin ? adminUserType : regularUserType; };

const encryptPassword = async password => { return bcrypt.hash(password, saltRounds); };
const comparePassword = async (reqPassword, dbPassword) => { return bcrypt.compare(reqPassword, dbPassword); };

const getInvalidUserStatus = () => { return { ok: false, message: "invalid user" }; };

let agency;
const getAgency = () => { if (agency === undefined) { agency = require('../db/agency'); } return agency; };

let gtfsImport;
const getGTFSImport = () => { if (gtfsImport === undefined) { gtfsImport = require('../db/gtfsimport'); } return gtfsImport; };

const auth = async (req, email, password) => {
    let invCred = "invalid credentials";
    let invCredResult = { ok: false, message: invCred };
    return new Promise((resolve, reject) => {
        let promiseReq = req;
        if (!!email && email.length > 0) {
            if (!!password && password.length > 0) {
                getUserByEmail(email)
                    .then((user) => {
                        if (user) {
                            comparePassword(password, user.password).
                                then((result) => {
                                    if (result) {
                                        /*promiseReq.session.regenerate(err => {
                                            let sessionUser = {};
                                            sessionUser.id = user.id;
                                            sessionUser.email = user.email;
                                            sessionUser.isAdmin = isUserAdmin(user);
                                            promiseReq.session.user = sessionUser;
                                            if (err) { console.log('session regenerate: ' + err); }
                                            let message = "authenticated";
                                            if (process.env.NODE_ENV == 'development') { message = email + " " + message; }
                                            resolve({ ok: true, message: message });
                                        });*/
                                        let sessionUser = {};
                                        sessionUser.id = user.id;
                                        sessionUser.email = user.email;
                                        sessionUser.isAdmin = isUserAdmin(user);
                                        promiseReq.session.user = sessionUser;
                                        promiseReq.session.save();
                                        let message = "authenticated";
                                        if (process.env.NODE_ENV === 'development') { message = email + " " + message; }
                                        //sessions.refreshSession(promiseReq.session);
                                        resolve({ ok: true, message: message });
                                    }
                                    else {
                                        sessions.destroySession(promiseReq.session);
                                        resolve({ ok: result, message: invCred });
                                    }
                                }).catch((err) => { console.log(err.message); resolve({ ok: false, message: 'internal error' }); });
                        }
                        else {
                            sessions.destroySession(promiseReq.session);
                            resolve(invCredResult);
                        }
                    }).catch((err) => {
                        console.log('auth unexpected error: ' + err.message);
                        sessions.destroySession(promiseReq.session);
                        resolve({ ok: false, message: 'internal error' });
                    });
            } else {
                sessions.destroySession(promiseReq.session);
                resolve(invCredResult);
            }
        } else {
            sessions.destroySession(promiseReq.session);
            resolve(invCredResult);
        }
    });
};

const createUserAll = async (email, password, type, emailConfirmed) => {
    return encryptPassword(password)
        .then((encryptedPassword) => {
            return db.insert(usersTableSpecs.tableName,
                ["email", "password", "type", "is_email_confirmed"],
                [[[email, encryptedPassword, type, emailConfirmed]]]);
        });
};

const createUser = async (email, password, isAdmin) => {
    return getUserByEmail(email)
        .then(user => {
            if (user) { return { ok: false, message: "user already exists" }; }
            else {
                return createUserAll(email, password, getUserType(isAdmin), true)
                    .then(() => {
                        return sessions.notifyUserCreated()
                            .then(count => {
                                //console.log('user creation notified ' + count + ' sessions');
                                return { ok: true, message: "user created" };
                            });
                    })
                    .catch(err => {
                        console.log('createUser: ' + err.message);
                        return { ok: false, message: "failed to create user" };
                    });
            }
        });
};

const modifyUser = async (userId, email, password, isAdmin) => {
    return getUserById(userId)
        .then(user => {
            if (user) {
                if (email) {
                    if (email !== user.email) {
                        let result = db.validateEmail(email);
                        if (!result.ok) { return result; }
                    }
                    else { email = undefined; }
                }
                if (password) {
                    let result = db.validatePassword(password);
                    if (!result.ok) { return result; }
                }

                if (user.id === adminUserId) { isAdmin = undefined; }

                if (isAdmin !== undefined) {
                    if ((isAdmin = !!isAdmin) === isUserAdmin(user)) {
                        isAdmin = undefined;
                    }
                }
                if (email === undefined && password === undefined && isAdmin === undefined) {
                    return { ok: false, message: "unchanged" };
                }
                else {
                    if (password !== undefined) {
                        return encryptPassword(password)
                            .then(encryptedPassword => {
                                return setUserEmailPasswordAdmin(user.id, email, encryptedPassword, isAdmin);
                            });
                    }
                    else {
                        return setUserEmailPasswordAdmin(user.id, email, undefined, isAdmin);
                    }
                }
            }
            else {
                return getInvalidUserStatus();
            }
        });
};

const deleteUserWithId = async userId => {
    return getUserById(userId)
        .then(user => {
            if (user) {
                if (user.id === adminUserId) {
                    return { ok: false, message: "user cannot be deleted" };
                }
                else {
                    return listUserUsers(
                        userId
                    ).then(usersInUserAgencies => {
                        return db.deleteQuery(usersTableSpecs.tableName, '`id`="' + user.id + '"')
                            .then(() => {
                                return sessions.delSessionsByUserId(user.id);
                            }).then(() => {
                                sessions.notifyUserDeleted(
                                    usersInUserAgencies, { id: userId }
                                ).then(notifyCount => {
                                    return { ok: true, message: "user deleted" };
                                });
                            }).catch((err) => {
                                return { ok: false, message: err.message };
                            });
                    });
                }
            }
            else {
                return { ok: false, message: "user does not exist" };
            }
        }).then(result => {
            return result;
        });
};

const setUserEmailPasswordAdmin = async (userId, email, encryptedPassword, isAdmin) => {
    let update = {}, where = {};
    if (email !== undefined) {
        update.email = email;
    }
    if (encryptedPassword !== undefined) {
        update.password = encryptedPassword;
    }
    if (isAdmin !== undefined) {
        update.type = getUserType(isAdmin);
    }
    where.id = userId;
    let values = [update, where];
    return db.update(
        usersTableSpecs.tableName, values, true
    ).then(() => {
        if (email !== undefined) {
            if (isAdmin !== undefined) { return sessions.changeAdminAndEmail(userId, isAdmin, email); }
            else { return sessions.changeEmail(userId, email); }
        }
        else if (isAdmin !== undefined) { return sessions.changeAdmin(userId, isAdmin); }
        else { return 0; }
    }).then((countChanged) => {
        return listUserUsers(
            userId
        ).then(usersInUserAgencies => {
            sessions.notifyUserChanged(
                usersInUserAgencies, { id: userId }
            ).then(notifyCount => {
                let sessionSessions = countChanged !== 1 ? ' sessions' : ' session';
                return { ok: true, message: 'changed ' + countChanged + sessionSessions };
            });
        });
    }).catch(err => {
        let isDuplicateError = err.code === "ER_DUP_ENTRY";
        let message = isDuplicateError ? "email already exists" : changesCannotBeImplementedAsRequested;
        if (!isDuplicateError) { console.log('modifyUser: ' + err.message); }
        return { ok: false, message: message };
    });
};

const getUserById = async userId => {
    return db.select(usersTableSpecs.tableName, usersTableSpecs.fieldNames, "`id`='" + userId + "'")
        .then(results => { return db.getFirstResult(results); });
};
const getUserByEmail = async userEmail => {
    return db.select(usersTableSpecs.tableName, usersTableSpecs.fieldNames, "`email`='" + userEmail + "'")
        .then(results => { return db.getFirstResult(results); });
};

const createUserWithId = async (id, email, password, type, emailConfirmed) => {
    return encryptPassword(password)
        .then((encryptedPassword) => {
            return db.insert(usersTableSpecs.tableName,
                ["id", "email", "password", "type", "is_email_confirmed"],
                [[[id, email, encryptedPassword, type, emailConfirmed]]]);
        });
};

const checkCreateAdminUser = async () => {
    return getUserById(adminUserId)
        .then((user) => {
            if (user === undefined) {
                return createUserWithId(adminUserId, adminInitialEmail, adminInitialPassword, adminUserType, true)
                    .then(insertResults => {
                        if (insertResults.insertId !== adminUserId) {
                            throw new Error('invalid admin user id');
                        }
                        return true;
                        //return setUserEmailConfirmed(insertResults.insertId, true);
                    });
            }
        });
};

const getUserCount = async (whereStr) => { return db.getTableCount(usersTableSpecs.tableName, whereStr); };

const init = async () => {
    return db.checkCreateTable(usersTableSpecs)
        .then(() => {
            return getUserCount();
        })
        .then(userCount => {
            return userCount > 0 ? true : checkCreateAdminUser();
        });
};

const associateUserWithAgency = async (userId, agencyId) => {
    return getUserById(
        userId
    ).then(user => {
        if (user) {
            let userAgencyTableName = schemas.getUserAgencyTableName();
            let sqlStr = 'INSERT INTO ?? (??, ??) VALUES (?, ?);';
            let values = [userAgencyTableName, "user_id", "agency_id", user.id, agencyId];
            return db.queryPoolValues(sqlStr, values)
                .then(results => {
                    return getUsersInAgencyId(agencyId)
                        .then(usersInAgency => {
                            return sessions.notifyUserAddedIntoAgency(usersInAgency, { userId: userId, agencyId: agencyId })
                                .then(count => {
                                    //console.log('user added to agency notified ' + count + ' sessions');
                                    return { ok: true, message: "agency and user are associated" };
                                });
                        });
                }).catch(err => {
                    let message;
                    if (err.code === "ER_DUP_ENTRY") { message = "agency and user are already associated"; }
                    else if (err.code === "ER_NO_REFERENCED_ROW" || err.code === "ER_NO_REFERENCED_ROW_2") { message = "agency does not exist"; }
                    else { message = changesCannotBeImplementedAsRequested; }
                    return { ok: false, message: message };
                });
        }
        else { return { ok: false, message: "user does not exist" }; }
    }).then(result => { return result; });
};

const dissociateUserFromAgency = async (userId, agencyId) => {
    return getUserById(
        userId
    ).then(user => {
        if (user) {
            getUsersInAgencyId(agencyId)
                .then(usersInAgency => {
                    let userAgencyTableName = schemas.getUserAgencyTableName();
                    let sqlStr = 'DELETE FROM ?? WHERE ?? =  ? AND ?? = ?;';
                    let values = [userAgencyTableName, "user_id", user.id, "agency_id", agencyId];
                    return db.queryPoolValues(sqlStr, values)
                        .then(results => {
                            return sessions.notifyUserRemovedFromAgency(usersInAgency, { userId: userId, agencyId: agencyId })
                                .then(count => {
                                    //console.log('user removed from agency notified ' + count + ' sessions');
                                    return { ok: true, message: "agency and user are dissociated" };
                                });
                        }).catch(err => {
                            console.log('dissociateUserFromAgency: ' + err.message);
                            return { ok: false, message: changesCannotBeImplementedAsRequested };
                        });
                });
        }
        else { return { ok: false, message: "user does not exist" }; }
    }).then(result => { return result; });
};

const mapUserResultsToJSONOutput = results => { return results.map(t => { return { id: t.id, email: t.email, isAdmin: isUserAdmin(t) }; }); };

const mapUserWithAgencies = results => {
    let newResults = [];
    results.reduce((soFar, t) => {
        let existing;
        if (!(existing = soFar['' + t.id])) {
            newResults.push(existing = soFar['' + t.id] = { id: t.id, email: t.email, isAdmin: isUserAdmin(t), agencies: [] });
        }
        if (t.agency_id) {
            existing.agencies.push({ id: t.agency_id, prefix: t.prefix });
        }
        return soFar;
    }, {});
    return newResults;
};

const listUserUsers = async (userId) => {
    let userTableName = schemas.getUserTableName();
    let userAgencyTableName = schemas.getUserAgencyTableName();
    let agenciesTableName = schemas.getAgenciesTableName();
    let sqlStr = 'SELECT ??.??, ??.??, ??.??, ??.??, ??.?? FROM ??, ??, ?? WHERE ';
    sqlStr += '??.?? IN (SELECT ??.?? FROM ?? WHERE ??.?? = ?) AND ';
    sqlStr += '??.?? = ??.?? AND ??.?? = ??.?? ORDER BY ??.?? ASC, ??.?? ASC;';
    let values = [
        userTableName, "id", userTableName, "email", userTableName, "type", userAgencyTableName, "agency_id", agenciesTableName, "prefix",
        userTableName, agenciesTableName, userAgencyTableName,

        userAgencyTableName, "agency_id",

        userAgencyTableName, "agency_id",
        userAgencyTableName,

        userAgencyTableName, "user_id",
        userId,

        userTableName, "id", userAgencyTableName, "user_id",
        agenciesTableName, "id", userAgencyTableName, "agency_id",

        userTableName, "email", userAgencyTableName, "agency_id"
    ];
    return db.queryPoolValues(
        sqlStr, values
    ).then(results => {
        return mapUserWithAgencies(results);
    }).catch(err => {
        console.log('listUserUsers: ' + err.message); return [];
    });
};

const listUsersWithAgencies = async () => {
    let userTableName = schemas.getUserTableName();
    let userAgencyTableName = schemas.getUserAgencyTableName();
    let agenciesTableName = schemas.getAgenciesTableName();
    let sqlStr = 'SELECT ??.??, ??.??, ??.??, ??.??, ??.?? FROM ??, ??, ?? WHERE ??.?? = ??.?? AND ??.?? = ??.?? ORDER BY ??.?? ASC, ??.?? ASC;';
    let values = [
        userTableName, "id", userTableName, "email", userTableName, "type", userAgencyTableName, "agency_id", agenciesTableName, "prefix",
        userTableName, agenciesTableName, userAgencyTableName,

        userTableName, "id", userAgencyTableName, "user_id",
        agenciesTableName, "id", userAgencyTableName, "agency_id",

        userTableName, "email", userAgencyTableName, "agency_id"
    ];
    return db.queryPoolValues(
        sqlStr, values
    ).then(results => {
        return mapUserWithAgencies(results);
    }).catch(err => {
        console.log('listUses: ' + err.message); return [];
    });
};

const listAllUsers = async () => {
    let userTableName = schemas.getUserTableName();
    let userAgencyTableName = schemas.getUserAgencyTableName();
    let agenciesTableName = schemas.getAgenciesTableName();
    let sqlStr = 'SELECT ??.??, ??.??, ??.??, ??.??, ??.?? FROM ??, ??, ?? WHERE ??.?? = ??.?? AND ??.?? = ??.?? ORDER BY ??.?? ASC, ??.?? ASC';
    let sqlStr2 = 'SELECT ??.??, ??.??, ??.??, null as agency_id, null as prefix FROM ?? LEFT JOIN ?? ON ??.?? = ??.?? WHERE ??.?? is null ORDER BY ??.?? ASC, ??.?? ASC';
    sqlStr = '(' + sqlStr + ') union (' + sqlStr2 + ');';
    let values = [
        userTableName, "id", userTableName, "email", userTableName, "type", userAgencyTableName, "agency_id", agenciesTableName, "prefix",
        userTableName, agenciesTableName, userAgencyTableName,

        userTableName, "id", userAgencyTableName, "user_id",
        agenciesTableName, "id", userAgencyTableName, "agency_id",

        userTableName, "email", userAgencyTableName, "agency_id",

        userTableName, "id", userTableName, "email", userTableName, "type", 
        userTableName, userAgencyTableName,

        userTableName, "id", userAgencyTableName, "user_id",

        userAgencyTableName, "user_id",

        userTableName, "email", userAgencyTableName, "agency_id"
    ];
    return db.queryPoolValues(
        sqlStr, values
    ).then(results => {
        return mapUserWithAgencies(results);
    }).catch(err => {
        console.log('listUses: ' + err.message); return [];
    });
};

const getUsersInAgencyId = async agencyId => {
    //select users.id from users inner join user_agency on (user_agency.agency_id = 2) where users.id = user_agency.user_id;

    let userTableName = schemas.getUserTableName();
    let userAgencyTableName = schemas.getUserAgencyTableName();
    let sqlStr = 'SELECT ??.??, ??.??, ??.?? FROM ?? INNER JOIN ?? ON (??.?? = ?) WHERE ??.?? = ??.??';
    let values = [
        userTableName, "id", userTableName, "email", userTableName, "type",
        userTableName,
        userAgencyTableName,
        userAgencyTableName, "agency_id", agencyId,
        userTableName, "id", userAgencyTableName, "user_id"
    ];
    return db.queryPoolValues(
        sqlStr, values
    ).then(results => {
        return mapUserResultsToJSONOutput(results);
    }).catch(err => {
        console.log('getUsersInAgencyId: ' + err.message); return [];
    });
};

const getAgenciesInUserId = async userId => {
    let agenciesTableName = schemas.getAgenciesTableName();
    let userAgencyTableName = schemas.getUserAgencyTableName();
    let sqlStr = 'SELECT ??.??, ??.?? FROM ?? INNER JOIN ?? ON (??.?? = ?) WHERE ??.?? = ??.?? ORDER BY ??.??';
    let values = [
        agenciesTableName, "id", agenciesTableName, "prefix",
        agenciesTableName,
        userAgencyTableName,
        userAgencyTableName, "user_id", userId,
        agenciesTableName, "id", userAgencyTableName, "agency_id",
        agenciesTableName, "id"
    ];
    return db.queryPoolValues(
        sqlStr, values
    ).then(results => {
        return getAgency().addAgencyInfos(results);
    }).catch(err => {
        console.log('getAgenciesInUserId: ' + err.message); return [];
    });
};

const isUserInAgency = async (userId, agencyId) => {
    let userAgencyTableName = schemas.getUserAgencyTableName();
    let sqlStr = 'SELECT ??.?? FROM ?? WHERE ??.?? = ? AND ??.?? = ? LIMIT 1';
    let values = [
        userAgencyTableName, "user_id",
        userAgencyTableName,
        userAgencyTableName, "user_id",
        userId,
        userAgencyTableName, "agency_id",
        agencyId
    ];
    return db.queryPoolValues(sqlStr, values).then(results => {
        return results.length > 0;
    }).catch(err => {
        return false;
    });
};

const isUserInAgencyByPrefix = async (userId, agencyPrefix) => {
    return getAgency().getAgencyByPrefix(
        agencyPrefix
    ).then(agencyObj => { return agencyObj ? isUserInAgency(userId, agencyObj.id) : false; });
};

let userMessageNumber = 0;

const sendMessage = async (toUserId, messageObj) => {
    return getUserById(
        messageObj.user.id
    ).then(fromUser => {
        if (fromUser) {
            return getUserById(
                toUserId
            ).then(toUser => {
                if (toUser) {
                    messageObj = Object.assign(messageObj, { order: ++userMessageNumber });
                    return sessions.sendUserMessage(toUserId, messageObj)
                        .then(count => {
                            return { ok: true, message: "user message sent" };
                        });
                }
                else {
                    return getInvalidUserStatus();
                }
            });
        }
        else {
            return getInvalidUserStatus();
        }
    });
};

module.exports = {
    init: init,
    auth: auth,
    sendMessage: sendMessage,
    getUsersInAgencyId: getUsersInAgencyId,
    getUserById: getUserById,
    getUserByEmail: getUserByEmail,
    isUserInAgency: isUserInAgency,
    isUserInAgencyByPrefix: isUserInAgencyByPrefix,
    isUserAdmin: isUserAdmin,
    getUserCount: getUserCount,
    createUser: createUser,
    modifyUser: modifyUser,
    deleteUserWithId: deleteUserWithId,
    listAllUsers: listAllUsers,
    associateUserWithAgency: associateUserWithAgency,
    dissociateUserFromAgency: dissociateUserFromAgency,
    listUserUsers: listUserUsers,
    getAgenciesInUserId: getAgenciesInUserId
};

