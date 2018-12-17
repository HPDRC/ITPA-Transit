'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        if (req.session.user) {
            return agency.getDesignAgencies(
                req.session.user.id,
                req.body.id,
                req.body.prefix,
                req.body.subAgencyIds,
                req.body.subAgencyIdsInAgency
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

