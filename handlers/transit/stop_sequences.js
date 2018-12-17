'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        if (req.session.user) {
            return agency.getTransitStopSequences(
                req.session.user.id,
                req.body.id,
                req.body.prefix,
                req.body.onTransitDate,
                req.body.stopSequenceIds,
                req.body.routeId,
                req.body.routeIdInAgency,
                req.body.routeDirectionId,
                req.body.stopId,
                req.body.stopIdInAgency,
                req.body.decodeData
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
