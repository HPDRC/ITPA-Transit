'use strict';

var user = require('../../db/user');

module.exports = {
    get: async (req, res, next) => {
        if (req.session.user) {
            return user.getAgenciesInUserId(
                req.session.user.id
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
