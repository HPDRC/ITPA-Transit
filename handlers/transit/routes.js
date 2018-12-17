'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        if (req.session.user) {
            return agency.getTransitRoutes(
                req.session.user.id,
                req.body.id,
                req.body.prefix,
                req.body.onTransitDate,
                req.body.subAgencyId,
                req.body.subAgencyIdInAgency,
                req.body.routeIds,
                req.body.routeIdsInAgency,
                req.body.includeDirections,
                req.body.includeDirectionShape,
                req.body.includeRouteShape,
                req.body.includeServiceIds,
                req.body.includeStopSequenceIds,
                req.body.decodeData,
                req.body.returnGeoJSON
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

