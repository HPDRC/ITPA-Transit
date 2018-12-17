'use strict';

const session = require('express-session');
const mysqlSession = require('express-mysql-session');
const Promise = require('bluebird');
const notify = require('../notify/notify');

let appSessionStore, appSession;

let user;
const getUser = () => { if (user === undefined) { user = require('../db/user'); } return user; };

const getAppSession = () => { return appSession; };

const init = settings => {
    let app = settings.app;
    appSessionStore = new mysqlSession(Object.assign(settings.dbConnSettings, { createDatabaseTable: true, database: settings.database }));
    appSession = session({
        store: appSessionStore,
        secret: 'whatchamacallit',
        reverse: false,
        resave: false,
        saveUninitialized: false,
        name: "net.stats",
        cookie: {
            secure: false,
            expires: false,
            path: '/',
            httpOnly: true
        }
    });
    app.use(appSession);
};

const endUnauthorizedRequest = res => { res.sendStatus(204); };

const processAuthRequiredRequest = (req, res, next) => { if (req.session.user) { next(); } else { endUnauthorizedRequest(res); } };
const processAdminAuthRequiredRequest = (req, res, next) => { if (req.session.user && req.session.user.isAdmin) { next(); } else { endUnauthorizedRequest(res); } };
const processTableAuthRequiredRequest = (req, res, next) => {
    let endRequest = true;
    if (req.session.user) {
        let user = getUser();
        if (req.session.user.isAdmin) { endRequest = false; next(); }
        else if (req.body.id || req.query.id) {
            let idUse = req.body.id ? req.body.id : req.query.id;
            endRequest = false;
            user.isUserInAgency(req.session.user.id, idUse).then(isUserInAgency => { if (isUserInAgency) { next(); } else { endUnauthorizedRequest(res); } });
        }
        else if (req.body.prefix || req.query.prefix) {
            let prefixUse = req.body.prefix ? req.body.prefix : req.query.prefix;
            endRequest = false;
            user.isUserInAgencyByPrefix(req.session.user.id, prefixUse).then(isUserInAgency => { if (isUserInAgency) { next(); } else { endUnauthorizedRequest(res); } });
        }
    }
    if (endRequest) { endUnauthorizedRequest(res); }
};

const processGuestRequest = (req, res, next) => {
    req.session = req.session || {};
    req.session.user = { id: 0, email: "guest", isAdmin: false };
    next();
};

const setSessionRoutes = (app) => {
    app.use('/admin', processAdminAuthRequiredRequest);
    app.use('/user', processAuthRequiredRequest);
    app.use('/agency', processAuthRequiredRequest);
    app.use('/design', processAuthRequiredRequest);
    app.use('/transit', processGuestRequest);
};

const setPostBodySessionRoutes = (app) => {
    app.use('/agency', processTableAuthRequiredRequest);
    app.use('/design', processTableAuthRequiredRequest);
};

const setSessionData = async (sessionId, data) => {
    return new Promise((resolve, reject) => { appSessionStore.set(sessionId, data, err => { if (err) {console.log('setSessionData: ' + err.message); } notify.refreshSessionIdSockets(sessionId); resolve(); }); });
};

const changeSessions = async changedSessions => {
    changedSessions = changedSessions || [];
    return Promise.map(changedSessions, t => { return setSessionData(t.session_id, t.data); }).then(() => { return changedSessions.length; }).
        catch((err) => { if (err) { console.log('changeSessions: ' + err.message); } return 0; });
};

const changeEmail = async (userId, newEmail) => { return getSessionsByUserId(userId).then(matchSessions => { return changeSessions(matchSessions.map(t => { t.data.user.email = newEmail; return t; })); }); };

const changeAdmin = async (userId, newAdmin) => {
    newAdmin = !!newAdmin;
    return getSessionsByUserId(userId).then(matchSessions => { return changeSessions(matchSessions.map(t => { t.data.user.isAdmin = newAdmin; return t; })); });
};

const changeAdminAndEmail = async (userId, newEmail, newAdmin) => {
    newAdmin = !!newAdmin;
    return getSessionsByUserId(userId).then(matchSessions => { return changeSessions(matchSessions.map(t => { t.data.user.isAdmin = newAdmin; t.data.user.email = newEmail; return t; })); });
};

const getSessionsByUserId = async userId => { return getAllUserSessions().then(allSessions => { return allSessions.filter(t => { return t.data.user.id === userId; }); }); };

const getIsAdminSessionIds = async (isAdmin, additionalUserIds) => {
    isAdmin = !!isAdmin;
    let auidMap = additionalUserIds !== undefined ? additionalUserIds.reduce((previousValue, currentValue) => {
        let key = '' + currentValue.id;
        previousValue[key] = key;
        return previousValue;
    }, {}) : {};
    return getAllUserSessions().then(allSessions => {
        return allSessions.filter(t => {
            let key = '' + t.data.user.id;
            let isInList = auidMap[key] !== undefined;
            let isInFilter = isInList;
            if (!isInFilter) { if (t.data.user.isAdmin === isAdmin) { isInFilter = true; } }
            return isInFilter;
        });
    });
};

const getAllUserSessions = async () => {
    return new Promise((resolve, reject) => {
        appSessionStore.all((err, allSessions) => {
            if (err) { reject(err); } else { resolve(allSessions.filter(t => { t.data = JSON.parse(t.data); return t.data.user !== undefined; })); }
        });
    });
};

const listSessions = async () => { return getAllUserSessions( ).then(allSessions => { return allSessions.map(t => { return { sessionId: t.session_id, email: t.data.user.email, isAdmin: t.data.user.isAdmin }; }); }); };

const delSessionById = async sessionId => {
    return new Promise((resolve, reject) => {
        notify.disconnectSessionIdSockets(sessionId);
        appSessionStore.destroy(sessionId, err => { if (err) { console.log('delSession: ' + err.message); } resolve(); });
    });
};

const delSessionsByIds = async delSessionIds => {
    delSessionIds = delSessionIds || [];
    return Promise.map(delSessionIds, t => { return delSessionById(t); }).then(() => { return delSessionIds.length; }).
        catch((err) => { if (err) { console.log('delSessionsByIds: ' + err.message); } return 0; });
};

function makeSessionChangeStatus(countChanged, verb) {
    let sessionSessions = countChanged !== 1 ? ' sessions' : ' session';
    verb = verb || "changed";
    return { ok: true, message: verb + ' ' + countChanged + sessionSessions };
}

const delSessionsByUserId = async userId => {
    return getSessionsByUserId(userId).then(matchSessions => {
        return delSessionsByIds(matchSessions.map(t => { return t.session_id; }, []))
            .then(countChanged => {
                return makeSessionChangeStatus(countChanged, "deleted");
            });
    });
};

const destroySession = (session) => { if (session) { notify.disconnectSessionIdSockets(session.id); session.destroy(); } };

const refreshSession = session => { if (session) { notify.refreshSessionIdSockets(session.id); } };

const getSessionById = async sessionId => {
    return new Promise((resolve, reject) => {
        appSessionStore.get(sessionId, (err, session) => {
            if (err) { session = undefined; console.log('getSessionById: ' + err.message); }
            resolve(session);
        });
    });
};

const notifyAgencyCreated = async agencyObj => {
    return getIsAdminSessionIds(true).
        then(sessions => {
            for (let i in sessions) {
                let session = sessions[i];
                notify.sendEventToSessionIdSockets(session.session_id, 'ev_agency_added', agencyObj);
            }
            return sessions.length;
        }).catch(err => {
            return 0;
        });
};

const commonNotifyAgencyVerb = async (usersInAgency, notifyObj, notifyVerb) => {
    return getIsAdminSessionIds(true, usersInAgency).
        then(sessions => {
            for (let i in sessions) {
                let session = sessions[i];
                notify.sendEventToSessionIdSockets(session.session_id, notifyVerb, notifyObj);
            }
            //console.log('notified ' + sessions.length + ' sessions');
            return sessions.length;
        }).catch(err => {
            return 0;
        });
};

const notifyAgencyChanged = async (usersInAgency, agencyObj) => {
    return commonNotifyAgencyVerb(usersInAgency, agencyObj, 'ev_agency_changed');
};

const notifyAgencyDeleted = async (usersInAgency, agencyObj) => {
    return commonNotifyAgencyVerb(usersInAgency, agencyObj, 'ev_agency_deleted');
};

const notifyUserAddedIntoAgency = async (usersInAgency, userAgencyObj) => {
    return commonNotifyAgencyVerb(usersInAgency, userAgencyObj, 'ev_user_into_agency');
};

const notifyUserRemovedFromAgency = async (usersInAgency, userAgencyObj) => {
    return commonNotifyAgencyVerb(usersInAgency, userAgencyObj, 'ev_user_outof_agency');
};

const notifyAgencyProgress = async (usersInAgency, progressObj) => {
    return commonNotifyAgencyVerb(usersInAgency, progressObj, 'ev_agency_progress');
};

const notifyUserCreated = async userObj => {
    return getIsAdminSessionIds(true).
        then(sessions => {
            for (let i in sessions) {
                let session = sessions[i];
                notify.sendEventToSessionIdSockets(session.session_id, 'ev_user_added', userObj);
            }
            return sessions.length;
        }).catch(err => {
            return 0;
        });
};

const notifyUserChanged = async (usersInAgency, userObj) => {
    return commonNotifyAgencyVerb(usersInAgency, userObj, 'ev_user_changed');
};

const notifyUserDeleted = async (usersInAgency, userObj) => {
    return commonNotifyAgencyVerb(usersInAgency, userObj, 'ev_user_deleted');
};

const sendAgencyMessage = async (usersInAgency, messageObj) => {
    return commonNotifyAgencyVerb(usersInAgency, messageObj, 'ev_agency_message');
};

const sendUserMessage = async (userId, messageObj) => {
    return getSessionsByUserId(
        userId
    ).then(sessions => {
        for (let i in sessions) {
            let session = sessions[i];
            notify.sendEventToSessionIdSockets(session.session_id, 'ev_user_message', messageObj);
        }
        return sessions.length;
    }).catch(err => {
        return 0;
    });
};

module.exports = {
    init: init,
    sendAgencyMessage: sendAgencyMessage,
    sendUserMessage: sendUserMessage,
    notifyAgencyProgress: notifyAgencyProgress,
    notifyAgencyCreated: notifyAgencyCreated,
    notifyAgencyChanged: notifyAgencyChanged,
    notifyAgencyDeleted: notifyAgencyDeleted,
    notifyUserAddedIntoAgency: notifyUserAddedIntoAgency,
    notifyUserRemovedFromAgency: notifyUserRemovedFromAgency,
    notifyUserCreated: notifyUserCreated,
    notifyUserChanged: notifyUserChanged,
    notifyUserDeleted: notifyUserDeleted,
    getAppSession: getAppSession,
    setSessionRoutes: setSessionRoutes,
    setPostBodySessionRoutes: setPostBodySessionRoutes,
    getSessionById: getSessionById,
    getIsAdminSessionIds: getIsAdminSessionIds,
    changeEmail: changeEmail,
    changeAdmin: changeAdmin,
    changeAdminAndEmail: changeAdminAndEmail,
    refreshSession: refreshSession,
    destroySession: destroySession,
    delSessionsByUserId: delSessionsByUserId,
    delSessionById: delSessionById,
    listSessions: listSessions
};
