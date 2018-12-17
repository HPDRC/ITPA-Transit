'use strict';

var user = require('../../../db/user');

module.exports = {
    post: async (req, res, next) => {
        return user.associateUserWithAgency(
            req.body.userId,
            req.body.agencyId
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    },
    delete: async (req, res, next) => {
        return user.dissociateUserFromAgency(
            req.body.userId,
            req.body.agencyId
        ).then((result) => {
            res.status(200).json(result);
        }).catch((err) => {
            res.status(400).send(err.message);
        });
    }
};
