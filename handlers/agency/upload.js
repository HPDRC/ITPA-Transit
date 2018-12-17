'use strict';

const dataProvider = require('../../db/agency');

module.exports = {
    post: async (req, res, next) => {
        let sessionUser = req.session.user;
        if (sessionUser) {
            if (!req.files || !req.files.file || !req.files.file.mv) { return res.status(400).send('missing file'); }
            let user = { id: sessionUser.id, email: sessionUser.email, isAdmin: sessionUser.isAdmin };
            dataProvider.importLocalFile(
                user, req.files.file, req.body.id
            ).then(result => {
                return res.status(200).json(result);
            }).catch(err => {
                console.log(err.message);
                return res.status(200).json({ ok: false, message: "internal error" });
            });
        }
        else {
            res.sendStatus(204);
        }
    }
};
