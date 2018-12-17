'use strict';

let theio;
let sessions;

let socketsBySessionId = {};

const getSocketsBySessionId = sessionId => {
    return socketsBySessionId[sessionId] ? socketsBySessionId[sessionId].sockets : undefined;
};

const associate = (socket, session) => {
    if (!!socket && !!session) {
        let socketId = socket.id, sessionId = session.id;
        if (socketsBySessionId[sessionId] === undefined) { socketsBySessionId[sessionId] = { count: 0, sessionId: sessionId, sockets: {} }; }
        socketsBySessionId[sessionId].sockets[socketId] = socket;
        ++socketsBySessionId[sessionId].count;
    }
};

const desassociateBySocketId = (socketId, sessionId) => {
    let sockets = getSocketsBySessionId(sessionId);
    if (sockets !== undefined && sockets[socketId] !== undefined) {
        delete socketsBySessionId[sessionId].sockets[socketId];
        if (--socketsBySessionId[sessionId].count <= 0) {
            console.log('deleting');
            delete socketsBySessionId[sessionId];
        }
    }
};

const desassociateBySocket = (socket, sessionId) => {
    if (socket) {
        return desassociateBySocketId(socket.id, sessionId);
    }
};

const desassociateBySessionId = sessionId => {
    if (getSocketsBySessionId(sessionId)) { delete socketsBySessionId[sessionId]; }
};

const getList = () => {
    let list = [], index = 0;
    for (let i in socketsBySessionId) {
        let sb = socketsBySessionId[i], sockets = sb.sockets;
        let count = 0;
        for (let j in sockets) { ++count; }
        list.push({ index: ++index, count: count });
    }
    return list;
};

const onDisconnect = socket => {
    let session = socket.handshake.session;
    desassociateBySocket(socket, session.id);
    sessions.getSessionById(session.id)
        .then(session => {
            let userEmail = session ? session.user.email : 'unknown';
            console.log('io dis: ' + userEmail);
        });
};

const onMessageOut = (socket, msg) => {
    let session = socket.handshake.session, user = session.user;
    sessions.getSessionById(session.id)
        .then(session => {
            let userEmail = session ? session.user.email : 'unknown';
            console.log('message: ' + msg);
            theio.emit('ev_messagein', userEmail, msg);
        });
};

const init = settings => {
    let app = settings.app;

    sessions = settings.sessions;

    theio = require('socket.io')(settings.server);

    const sharedSession = require('express-socket.io-session');

    theio.use(sharedSession(settings.session));

    theio.on('connection', (socket) => {
        if (socket.handshake.session && socket.handshake.session.user) {
            let session = socket.handshake.session, user = session.user;
            associate(socket, session);

            console.log('io conn: ' + user.email);

            socket.on('disconnect', function () { onDisconnect(socket); });

            socket.on('ev_messageout', function (msg) { onMessageOut(socket, msg); });
            socket.on('ev_getlist', function () { socket.emit('ev_setlist', getList()); });

            socket.emit('ev_connected');
        }
        else {
            console.log('user rejected');
            socket.disconnect();
        }
    });

    theio.set('authorization', function (handshakeData, callback) {
        callback(null, true);
    });
};

const emitToSessionIdSockets = (sessionId, event, thenDisconnect) => {
    let sockets = getSocketsBySessionId(sessionId);
    if (sockets) {
        for (let i in sockets) {
            let socket = sockets[i];
            socket.emit(event);
            if (thenDisconnect) {
                desassociateBySocket(socket, sessionId);
                socket.disconnect();
            }
        }
    }
};

const disconnectSessionIdSockets = sessionId => { return emitToSessionIdSockets(sessionId, 'ev_disconnect', true); };

const refreshSessionIdSockets = sessionId => { return emitToSessionIdSockets(sessionId, 'ev_refresh', false); };

const sendEventToSessionIdSockets = (sessionId, eventName, eventObject) => {
    let sockets = getSocketsBySessionId(sessionId);
    let count = 0;
    if (sockets) {
        let nowDate = new Date();
        eventObject = Object.assign(eventObject, { timestamp: nowDate });
        for (let i in sockets) {
            let socket = sockets[i];
            socket.emit('ev_notification', eventName, eventObject);
            ++count;
        }
    }
    //console.log('notified ' + count + ' sockets');
};

const io = () => { return theio; };

module.exports = {
    init: init,
    io: io,
    sendEventToSessionIdSockets: sendEventToSessionIdSockets,
    refreshSessionIdSockets: refreshSessionIdSockets,
    disconnectSessionIdSockets: disconnectSessionIdSockets
};
