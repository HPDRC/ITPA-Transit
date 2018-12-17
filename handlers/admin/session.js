'use strict';

var sessions = require('../../sessions/sessions');

module.exports = {
    get: async (req, res, next) => {
        return sessions.listSessions(
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    delete: async (req, res, next) => {
        let okResult = { ok: true, message: "OK" };
        if (req.body.userId) {
            return sessions.delSessionsByUserId(
                req.body.userId
            ).then((result) => {
                res.status(200).json(result);
            }).catch((err) => {
                res.status(400).send(err.message);
            });
        }
        else if (req.body.sessionId) {
            return sessions.delSessionById(
                req.body.sessionId
            ).then((result) => {
                res.status(200).json(okResult);
            }).catch((err) => {
                res.status(400).send(err.message);
            });
        }
        else {
            res.status(200).json(okResult);
        }
    }
};
