'use strict';

const user = require('../db/user');

module.exports = {
    get: async (req, res, next) => {
        if (req.session.user) {
            var result = { id: req.session.user.id, email: req.session.user.email, isAdmin: req.session.user.isAdmin };
            res.status(200).json(result);
        }
        else {
            res.sendStatus(204);
        }
    },
    put: async (req, res, next) => {
        if (req.session.user) {
            return user.modifyUser(
                req.session.user.id,
                req.body.email,
                req.body.password,
                undefined
            ).then((result) => {
                res.status(200).json(result);
            }).catch((err) => {
                res.status(400).send(err.message);
            });
        }
        else {
            res.sendStatus(204);
        }
    }
};
