'use strict';

var agency = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        if (req.session.user) {
            return agency.getDesignTrips(
                req.session.user.id,
                req.body.id,
                req.body.prefix,
                req.body.tripIds,
                req.body.tripIdsInAgency,
                req.body.routeId,
                req.body.routeIdInAgency,
                req.body.routeTypeList,
                req.body.routeDirectionId,
                req.body.serviceIds,
                req.body.serviceIdsInAgency,
                req.body.stopSequenceId,
                req.body.stopIds,
                req.body.stopIdsInAgency,
                req.body.onDate,
                req.body.minStartHMS,
                req.body.maxStartHMS,
                req.body.minEndHMS,
                req.body.maxEndHMS,
                req.body.includeStopSequences,
                req.body.includeStopTimes,
                req.body.includeStopDistances,
                req.body.includeStops,
                req.body.includeRoutes,
                req.body.includeShapes,
                req.body.includeOriginal,
                req.body.excludeSimplified,
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

