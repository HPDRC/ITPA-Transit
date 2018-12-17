'use strict';

const user = require('../db/user');
const sessions = require('../sessions/sessions');

module.exports = {
    post: async (req, res, next) => {
        return user.auth(
            req,
            req.body.email,
            req.body.password
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    delete: async (req, res, next) => {
        if (req.session.user) {
            var result = { ok: true, message: "logged out" };
            if (process.env.NODE_ENV === 'development') { result.message = req.session.user.email + " " + result.message; }
            //let email = req.session.user.email;
            let userId = req.session.user.id;
            sessions.destroySession(req.session);
            if (req.query.allSessions) {
                sessions.delSessionsByUserId(userId)
                    .then(() => {
                        res.status(200).json(result);
                    }).catch((err) => {
                        res.status(400).send(err.message);
                    });
            }
            else {
                res.status(200).json(result);
            }
        }
        else {
            res.sendStatus(200);
        }
    }
};
