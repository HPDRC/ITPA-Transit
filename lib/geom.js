'use strict';

const REarth = 6378137;
const toRad = Math.PI / 180;

const haversineDistance = (c1, c2) => {
    let lat1 = toRad * c1[1];
    let lat2 = toRad * c2[1];
    let deltaLatBy2 = (lat2 - lat1) / 2;
    let deltaLonBy2 = (toRad * (c2[0] - c1[0])) / 2;
    let a = Math.sin(deltaLatBy2) * Math.sin(deltaLatBy2) +
        Math.sin(deltaLonBy2) * Math.sin(deltaLonBy2) *
        Math.cos(lat1) * Math.cos(lat2);
    return 2 * REarth * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
};

const PolyCode = function (settings) {
    let theThis; if (!((theThis = this) instanceof PolyCode)) { return new PolyCode(settings); }
    let defaultPrecision = 5;

    this.EncodeLineString = (coordinates, precision) => {
        if (!coordinates.length) { return ''; }
        let factor = Math.pow(10, clipPrecision(precision)),
            output = write(coordinates[0][0], 0, factor) + write(coordinates[0][1], 0, factor);
        for (let i = 1; i < coordinates.length; i++) {
            let a = coordinates[i], b = coordinates[i - 1];
            output += write(a[0], b[0], factor);
            output += write(a[1], b[1], factor);
        }
        return output;
    };

    this.DecodeLineString = (str, precision) => {
        let index = 0, lat = 0, lon = 0, coordinates = [], factor = Math.pow(10, clipPrecision(precision)), length = str.length, newValIndex;
        while (index < length) {
            newValIndex = read(str, index); lon += newValIndex.value_change; index = newValIndex.index;
            newValIndex = read(str, index); lat += newValIndex.value_change; index = newValIndex.index;
            coordinates.push([lon / factor, lat / factor]);
        }
        return coordinates;
    };

    this.EncodeValues = (values, precision) => {
        let len = !!values ? values.length : 0;
        if (!len) { return ''; }
        let prev = values[0];
        let factor = Math.pow(10, clipPrecision(precision)), output = write(prev, 0, factor);
        for (let i = 1; i < len; i++) { let next = values[i]; output += write(next, prev, factor); prev = next; }
        return output;
    };

    this.DecodeValues = (str, precision) => {
        let values = [], len = str.length;
        if (len > 0) {
            let factor = Math.pow(10, clipPrecision(precision)), index = 0, value = 0;
            while (index < len) {
                let newValIndex = read(str, index); value += newValIndex.value_change; index = newValIndex.index;
                let actualValue = (factor != 1) ? value / factor : value; values.push(actualValue);
            }
        }
        return values;
    };

    const clipPrecision = precision => { return precision === undefined ? defaultPrecision : (precision == 0 ? precision : (precision > 7 ? 7 : (precision < 5 ? 5 : precision))); }
    const py2_round = value => { return Math.floor(Math.abs(value) + 0.5) * Math.sign(value); }

    const write = (current, previous, factor) => {
        current = py2_round(current * factor);
        previous = py2_round(previous * factor);
        let delta = (current - previous) << 1;
        if (delta < 0) { delta = ~delta; }
        let output = '';
        while (delta >= 0x20) { output += String.fromCharCode((0x20 | (delta & 0x1f)) + 63); delta >>>= 5; }
        output += String.fromCharCode(delta + 63);
        return output;
    };

    const read = (encodedStr, index) => {
        let byteVal = undefined, result = 0, shift = 0, comp = false;
        while (byteVal === undefined || byteVal >= 0x20) { byteVal = encodedStr.charCodeAt(index) - 63; ++index; result |= (byteVal & 0x1f) << shift; shift += 5; comp = result & 1; }
        let value_change = !!comp ? ~(result >>> 1) : (result >>> 1);
        return { index: index, value_change: value_change };
    };

    const initialize = () => { }

    initialize();
};

const createPolyCode = () => { return new PolyCode(); }

const degrees2Meters = coords => {
    let lon = coords[0], lat = coords[1]
    let x = lon * 20037508.34 / 180;
    let y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180);
    y = y * 20037508.34 / 180;
    return [x, y]
};

const squaredDistance = (x1, y1, x2, y2) => { let dx = x2 - x1, dy = y2 - y1; return dx * dx + dy * dy; };

const squaredSegmentDistance = (x, y, x1, y1, x2, y2) => {
    let dx = x2 - x1, dy = y2 - y1;
    if (dx !== 0 || dy !== 0) {
        let t = ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy);
        if (t > 1) { x1 = x2; y1 = y2; } else if (t > 0) { x1 += dx * t; y1 += dy * t; }
    }
    return squaredDistance(x, y, x1, y1);
};

const simplifyLS = (lsCoords, simplifyTolerance) => {
    simplifyTolerance = simplifyTolerance !== undefined ? simplifyTolerance : 4;
    let simplifiedlsCoords = [];
    let start = 0, end = lsCoords.length, n = end - start;
    let squaredTolerance = simplifyTolerance * simplifyTolerance;
    let indices = [];
    if (n < 3) {
        for (; start < end; ++start) {
            simplifiedlsCoords.push(lsCoords[start]);
            indices.push(start);
        }
    }
    else {
        let markers = new Array(n);
        let stack = [start, end - 1], index = 0, i;
        let meterLSCoords = [];
        for (let i = 0; i < n; ++i) { meterLSCoords.push(degrees2Meters(lsCoords[i])); }
        markers[0] = markers[n - 1] = 1;
        while (stack.length > 0) {
            let last = stack.pop(), first = stack.pop();
            let inMeters = meterLSCoords[first];
            let x1 = inMeters[0], y1 = inMeters[1];
            inMeters = meterLSCoords[last];
            let x2 = inMeters[0], y2 = inMeters[1];
            let maxSquaredDistance = 0;
            for (i = first + 1; i < last; i += 1) {
                let inMeters = meterLSCoords[i];
                let x = inMeters[0], y = inMeters[1];
                let squaredDistance = squaredSegmentDistance(x, y, x1, y1, x2, y2);
                if (squaredDistance > maxSquaredDistance) { index = i; maxSquaredDistance = squaredDistance; }
            }
            if (maxSquaredDistance > squaredTolerance) {
                markers[index - start] = 1;
                if (first + 1 < index) { stack.push(first, index); }
                if (index + 1 < last) { stack.push(index, last); }
            }
        }
        for (i = 0; i < n; ++i) {
            if (markers[i]) {
                indices.push(i);
                simplifiedlsCoords.push(lsCoords[start + i]);
            }
        }
    }
    return { coords: simplifiedlsCoords, indices: indices };
};

const isLSClockwise = function(lsCoords) {
    let isClockwise = false;
    let lsLen = !!lsCoords ? lsCoords.length : 0;
    if (lsLen > 2) {
        let last = lsLen - 1, sum = 0, firstCoord = lsCoords[0], prevCoord = firstCoord;
        for (let i = 1; i < last; ++i) {
            let thisCoord = lsCoords[i];
            sum += (thisCoord[0] - prevCoord[0]) * (thisCoord[1] + prevCoord[1])
            prevCoord = thisCoord;
        }
        prevCoord = lsCoords[last];
        sum += (firstCoord[0] - prevCoord[0]) * (firstCoord[1] + prevCoord[1])
        isClockwise = sum >= 0;
        //isClockwise = sum <= 0;
    }
    return isClockwise;
};

const isValidNumber = n => { return !isNaN(n) && isFinite(n); };
const isNonNegative = n => { return isValidNumber(n) && n >= 0; };
const isMinOrSec = n => { return isValidNumber(n) && n >= 0 && n < 60; };

const getHMS = (hmsStr) => {
    let hms = 0, validFormat = false;
    try {
        let a = hmsStr.split(':');
        hms = (+a[0]) * 60 * 60 + (+a[1]) * 60 + (+a[2]);
        validFormat = true;
    }
    catch (e) { hms = 0; validFormat = false; }
    return { hms: hms, valid: validFormat };
};

const calcLSDistances = lsCoords => {
    let lsLen = !!lsCoords ? lsCoords.length : 0;
    let dists;
    if (lsLen > 0) {
        let totalDist = 0;
        let firstPoint = lsCoords[0], prevPoint = firstPoint;
        dists = new Array(lsLen);
        dists[0] = 0;
        for (let i = 1; i < lsLen; ++i) {
            let thisPoint = lsCoords[i];
            let thisDist = haversineDistance(thisPoint, prevPoint);
            prevPoint = thisPoint;
            totalDist += thisDist;
            dists[i] = totalDist;
        }
    }
    else { dists = []; }
    return dists;
};

const MergeMLS = function(settings) {
    let theThis; if (!((theThis = this) instanceof MergeMLS)) { return new MergeMLS(settings); }
    let doLogErrors, myName, coordMap, nCoordsMap, nCoords, segMap, nSegsMap, nSegs, newMLS, segsProcess, nSegsProcess;

    this.Merge = function(mlsGeom/*, simplifyTolerance*/) {
        reset();
        let mlsCoords = mlsGeom.coordinates ? mlsGeom.coordinates : mlsGeom, nLSs = mlsCoords.length;
        for (let iLS = 0; iLS < nLSs; ++iLS) { addLSCoordsToMap(mlsCoords[iLS]); }
        for (let iLS = 0; iLS < nLSs; ++iLS) { addLSSegsToMap(mlsCoords[iLS]); }
        while (createNewLS()) { }
        //if (simplifyTolerance != undefined) { newMLS.coordinates = tf.map.SimplifyMLSCoords(newMLS.coordinates, simplifyTolerance); }
        return newMLS;
    };

    const logError = (errorStr) => { if (doLogErrors) { console.log(myName + ': ' + errorStr); } };

    const addNewLS = () => { let newLS = []; newMLS.coordinates.push(newLS); return newLS; };

    const getCoordsAreDifferent = (coord1, coord2) => { return coord1[0] != coord2[0] || coord1[1] != coord2[1]; };

    const makeCoordKey = (coord) => { let lon = coord[0], lat = coord[1]; let key = '' + lon + '|' + lat; return key; };

    const makeSegKey = (coord1, coord2) => {
        let lon1 = coord1[0], lat1 = coord1[1], lon2 = coord2[0], lat2 = coord2[1], minLon, maxLon, minLat, maxLat;
        if (lon1 < lon2) { minLon = lon1; maxLon = lon2; } else { minLon = lon2; maxLon = lon1; }
        if (lat1 < lat2) { minLat = lat1; maxLat = lat2; } else { minLat = lat2; maxLat = lat1; }
        return makeCoordKey([minLon, minLat]) + '-' + makeCoordKey([maxLon, maxLat]);
    };

    const reset = () => { segMap = {}; coordMap = {}; segsProcess = {}; nCoordsMap = nCoords = nSegsMap = nSegs = nSegsProcess = 0; newMLS = { type: 'multilinestring', coordinates: [] } };

    const addCoordToMap = (coord) => {
        let coordKey = makeCoordKey(coord), coordEntry = coordMap[coordKey];
        ++nCoords;
        if (coordEntry == undefined) { ++nCoordsMap; coordEntry = coordMap[coordKey] = { count: 1, coordKey: coordKey, coord: coord, nSegs: 0, segMap: {} }; }
        else { ++coordEntry.count; }
    };

    const addSegToCoord = (coordKey, segKey) => {
        let coordEntry = coordMap[coordKey];
        if (!!coordEntry) {
            if (coordEntry.segMap[segKey] == undefined) { ++coordEntry.nSegs; coordEntry.segMap[segKey] = segKey; }
            else { logError('adding segment to coordinate more than once'); }
        } else { logError('adding segment to unmapped coordinate'); }
    };

    const delSegFromCoordObj = (coordObj, segKey) => {
        if (!!coordObj) {
            if (coordObj.segMap[segKey] != undefined) { delete coordObj.segMap[segKey]; --coordObj.nSegs; }
            else { logError('deleting non existing segment from coord'); }
        }
    };

    const addSegToMap = (coord1, coord2) => {
        ++nSegs;
        if (getCoordsAreDifferent(coord1, coord2)) {
            let segKey = makeSegKey(coord1, coord2), segEntry = segMap[segKey];
            if (segEntry == undefined) {
                let coord1K = makeCoordKey(coord1), coord2K = makeCoordKey(coord2);
                ++nSegsMap;
                segEntry = segMap[segKey] = { count: 1, segKey: segKey, coord1: coord1, coord2: coord2, coord1K: coord1K, coord2K: coord2K };
                addSegToCoord(coord1K, segKey);
                addSegToCoord(coord2K, segKey);
                segsProcess[segKey] = segEntry;
            }
            else { ++segEntry.count; }
        } else { logError('mls contains ls with zero length segment'); }
    };

    const addLSCoordsToMap = (lsCoords) => { for (let i in lsCoords) { addCoordToMap(lsCoords[i]); } };
    const addLSSegsToMap = (lsCoords) => { let nSegs = lsCoords.length - 1; for (let i = 0; i < nSegs; ++i) { addSegToMap(lsCoords[i], lsCoords[1 + i]); } nSegsProcess = nSegsMap; };

    const getSegObjWithMaxCountFromMap = (theMap) => { let maxCount = 0, maxSegObj; for (let i in theMap) { let s = theMap[i]; if (s.count > maxCount) { maxCount = s.count; maxSegObj = s; } } return maxSegObj; };
    const getProcessSegObjWithMaxCount = () => { return getSegObjWithMaxCountFromMap(segsProcess); };

    const delSegToProcess = (segKey) => { if (segsProcess[segKey] != undefined) { delete segsProcess[segKey]; --nSegsProcess; } else { logError('deleting invalid seg to process'); } };

    const getNextSegObjToProcess = () => { let nextSegObj = getProcessSegObjWithMaxCount(); if (!!nextSegObj) { delSegToProcess(nextSegObj.segKey); } return nextSegObj; };

    const getOtherCoordObj = (segObj, coordObj) => {
        let otherCoordObj;
        if (!!segObj && !!coordObj) {
            let coordKey = coordObj.coordKey;
            if (segObj.coord1K == coordKey) { otherCoordObj = coordMap[segObj.coord2K]; }
            else if (segObj.coord2K == coordKey) { otherCoordObj = coordMap[segObj.coord1K]; }
        }
        return otherCoordObj;
    };

    const removeFirstSegKeyFromCoordObj = (coordObj) => {
        let firstSegKey, nSegs = coordObj.nSegs;
        if (nSegs > 0) {
            for (let i in coordObj.segMap) { firstSegKey = coordObj.segMap[i]; delete coordObj.segMap[i]; break; }
            --coordObj.nSegs;
        }
        return firstSegKey;
    };

    const continueLS = (theLS, addToEndBool) => {
        let continued = false, coordInLSIndex, functionAddToLS;

        if (addToEndBool) { coordInLSIndex = theLS.length - 1; functionAddToLS = theLS.push; }
        else { coordInLSIndex = 0; functionAddToLS = theLS.unshift; }

        let coordInLS = theLS[coordInLSIndex], coordInLSKey = makeCoordKey(coordInLS), coordInLSObj = coordMap[coordInLSKey];

        if (!!coordInLSObj) {
            if (coordInLSObj.nSegs > 0) {
                let nextSegKey = removeFirstSegKeyFromCoordObj(coordInLSObj), nextSeg = segsProcess[nextSegKey];
                if (!!nextSeg) {
                    let otherCoordObj = getOtherCoordObj(nextSeg, coordInLSObj);
                    if (!!otherCoordObj) {
                        delSegToProcess(nextSegKey);
                        delSegFromCoordObj(otherCoordObj, nextSegKey);
                        functionAddToLS.call(theLS, otherCoordObj.coord);
                        continued = true;
                    } else { logError('cannot find other coord on seg to process'); }
                } else { logError('cannot find coord seg to process'); }
            }
        } else { logError('cannot find ls coord object to add to'); }
        return continued;
    };

    const createNewLS = () => {
        let created = false;
        if (nSegsProcess > 0) {
            let nextSegObj = getNextSegObjToProcess();
            if (!!nextSegObj) {
                let segKey = nextSegObj.segKey;
                let coord1K = nextSegObj.coord1K, coord2K = nextSegObj.coord2K;
                let coord1Obj = coordMap[coord1K], coord2Obj = coordMap[coord2K];
                if (!!coord1Obj && !!coord2Obj) {
                    let newLS = addNewLS();
                    delSegFromCoordObj(coord1Obj, segKey);
                    delSegFromCoordObj(coord2Obj, segKey);
                    newLS.push(coord1Obj.coord);
                    newLS.push(coord2Obj.coord);
                    while (continueLS(newLS, true)) { }
                    while (continueLS(newLS, false)) { }
                    created = true;
                } else { logError('segment with missing coordinates'); }
            } else { logError('cannot find the next seg obj to process'); }
        }
        return created;
    };

    const initialize = () => { myName = "MergeMLS"; settings = settings || {}; doLogErrors = !!settings.logErrors; }

    initialize();
};

const updateMapExtent = (extent, coord) => {
    try {
        if (coord) {
            if (!extent) { extent = [coord[0], coord[1], coord[0], coord[1]]; }
            else {
                if (extent[0] > coord[0]) { extent[0] = coord[0]; } else if (extent[2] < coord[0]) { extent[2] = coord[0]; }
                if (extent[1] > coord[1]) { extent[1] = coord[1]; } else if (extent[3] < coord[1]) { extent[3] = coord[1]; }
            }
        }
    } catch(e) {}
    return extent;
};

const hitTestSegment = (segStart, segEnd, coordinates) => {
    let distance, closestPoint, isStart = false, isEnd = false, isSeg = false, proj;
    let startLon = segStart[0], startLat = segStart[1];
    let endLon = segEnd[0], endLat = segEnd[1];

    if (startLon == endLon && startLat == endLat) { closestPoint = [startLon, startLat]; isEnd = true; proj = 1; }
    else {
        let coordsLon = coordinates[0], coordsLat = coordinates[1];
        let lonCoordsToStart = coordsLon - startLon, latCoordsToStart = coordsLat - startLat;
        let lonEndToStart = endLon - startLon, latEndToStart = endLat - startLat;
        let distEndStart = lonEndToStart * lonEndToStart + latEndToStart * latEndToStart;

        proj = (lonCoordsToStart * lonEndToStart + latCoordsToStart * latEndToStart) / distEndStart;

        if (proj < 0) { closestPoint = [startLon, startLat]; isStart = true; proj = 0; }
        else if (proj > 1) { closestPoint = [endLon, endLat]; isEnd = true; proj = 1; }
        else {
            closestPoint = [startLon + lonEndToStart * proj, startLat + latEndToStart * proj];
            isSeg = true;
        }
    }
    distance = haversineDistance(coordinates, closestPoint);
    return { distance: distance, closestPoint: closestPoint, isStart: isStart, isEnd: isEnd, isSeg: isSeg, proj: proj };
};

const hitTestLS = (lsCoords, coords, startSegIndex, startMinProj, endSegIndex, acceptDistance) => {
    let minDistance = -1, minDistanceIndex = -1, closestPoint, proj, nSegs = lsCoords.length - 1;
    let lsCoordsUse;

    if (nSegs == 0) { lsCoordsUse = [lsCoords[0], lsCoords[0]]; nSegs = 1; } else { lsCoordsUse = lsCoords; }
    if (startSegIndex == undefined) { startSegIndex = 0; }
    if (startMinProj == undefined) { startMinProj = 0; }

    for (let i = startSegIndex; i < nSegs; ++i) {
        let startSeg = lsCoordsUse[i];
        let endSeg = lsCoordsUse[1 + i];
        let hitSeg = hitTestSegment(startSeg, endSeg, coords);
        if (hitSeg.distance != undefined && hitSeg.proj >= startMinProj && (minDistance == -1 || hitSeg.distance < minDistance)) {
            minDistance = hitSeg.distance;
            closestPoint = !!hitSeg.closestPoint ? hitSeg.closestPoint.slice(0) : undefined;
            minDistanceIndex = i;
            proj = hitSeg.proj;
            if (acceptDistance !== undefined && minDistance <= acceptDistance) { break; }
            if (endSegIndex != undefined && i >= endSegIndex) { break; }
        }
        startMinProj = 0;
    }
    return { minDistance: minDistance, minDistanceIndex: minDistanceIndex, closestPoint: closestPoint, proj: proj };
};

const calcPointDistances = (lsCoords, lsDistances, ptsCoords) => {
    let distances, results, nFailed = 0;
    let nPts = ptsCoords ? (ptsCoords.length > 0 ? ptsCoords.length : 0) : 0;

    if (nPts > 0) {
        let prevDist = 0;
        let prevProj = 0, prevSeg = 0;

        let distPrevToClosest = 0;
        let distClosestInShape = 0;
        let distPtFromShape = 0;
        let closestPoint;
        let prevDistanceInShape = 0;

        results = new Array(nPts);
        distances = new Array(nPts);

        let acceptDistance = 30;
        let endSegIndex = 0;

        for (let iPt = 0; iPt < nPts; ++iPt) {
            let ptCoords = ptsCoords[iPt];
            let hitTest = hitTestLS(lsCoords, ptCoords, prevSeg, prevProj, endSegIndex, acceptDistance);
            endSegIndex = undefined;
            if (hitTest.minDistanceIndex >= 0 && hitTest.closestPoint) {
                closestPoint = hitTest.closestPoint;
                prevProj = hitTest.proj;
                let prevLSCoord = lsCoords[prevSeg = hitTest.minDistanceIndex];
                distPrevToClosest = haversineDistance(prevLSCoord, closestPoint);
                distClosestInShape = lsDistances[prevSeg] + distPrevToClosest;
                distPtFromShape = haversineDistance(closestPoint, ptCoords);
                if (distClosestInShape < prevDistanceInShape) {
                    ++nFailed;
                    //console.log('distance error case 1');
                }
                prevDistanceInShape = distClosestInShape;
            }
            else {
                ++nFailed;
                //console.log('hit test failed case 1');
            }
            let thisDistance = {
                distInShape: distClosestInShape,
                distFromShape: distPtFromShape,
                closestShapeCoord: closestPoint,
                seg: prevSeg,
                proj: prevProj
            };
            results[iPt] = thisDistance;
            distances[iPt] = thisDistance.distInShape;
        }
    }
    else {
        results = distances = [];
    }
    return { distances: distances, results: results, nFailed: nFailed };
};

const bitString32BitEncode = bitString => {
    let i, l = bitString.length, encodedBitString = l.toString() + '|';
    for (i = 0; i < l; i += 10) { encodedBitString += parseInt((bitString + '000000000').substring(i, i + 10), 2).toString(32); }
    return encodedBitString;
};

const bitString32BitDecode = encodedBitString => {
    const right = (string, count) => { return string.substring(string.length <= count ? 0 : string.length - count); }
    let arr = encodedBitString.split('|'), l = parseInt(arr[0], 10), chars = arr[1], i, limit = chars.length, decodedStr = '';
    for (i = 0; i < limit; i += 2) { decodedStr += right('000000000' + parseInt(chars.substring(i, i + 2), 32).toString(2), 10); }
    return decodedStr.substring(0, l);
};

const binarySearch = function (theArray, theKey, compareFunction) {
    if (theArray && theArray.length && compareFunction) {
        var m = 0, n = theArray.length - 1;
        while (m <= n) {
            var k = (n + m) >>> 1, cmp = compareFunction(theKey, theArray[k]);
            if (cmp > 0) { m = k + 1; } else if (cmp < 0) { n = k - 1; } else { return k; }
        }
        return -m - 1;
    }
    return undefined;
};

const binarySearchForExactOrPrevEntryIndex = function (theArray, theKey, compareFunction) {
    var result = binarySearch(theArray, theKey, compareFunction);
    if (result != undefined) {
        if (result < 0) {
            result = -(result + 1) - 1;
            if (result < 0) { result = undefined; }
        }
    }
    return result;
};

const binarySearchForInsertNewDataIndex = function (theArray, theKey, compareFunction) {
    var result = binarySearch(theArray, theKey, compareFunction);
    if (result != undefined) { if (result < 0) { result = -(result + 1); } }
    return result;
};

module.exports = {
    getHMS: getHMS,
    updateMapExtent: updateMapExtent,
    hitTestLS: hitTestLS,
    hitTestSegment: hitTestSegment,
    calcPointDistances: calcPointDistances,
    calcLSDistances: calcLSDistances,
    isLSClockwise: isLSClockwise,
    simplifyLS: simplifyLS,
    MergeMLS: MergeMLS,
    bitString32BitEncode: bitString32BitEncode,
    bitString32BitDecode: bitString32BitDecode,
    binarySearch: binarySearch,
    binarySearchForExactOrPrevEntryIndex: binarySearchForExactOrPrevEntryIndex,
    binarySearchForInsertNewDataIndex: binarySearchForInsertNewDataIndex,
    PolyCode: PolyCode
};



/*
MDT
'shapes id 150444 contains 4 sequence errors',
'shapes id 150445 contains 4 sequence errors',
'shapes id 150680 contains 93 sequence errors',
'shapes id 150681 contains 93 sequence errors',
'shapes id 150691 contains 4 sequence errors',
'shapes id 150760 contains 33 sequence errors',
'shapes id 150759 contains 39 sequence errors',
'shapes id 150767 contains 14 sequence errors',
'shapes id 144530 contains 10 sequence errors',
'shapes id 145026 contains 8 sequence errors',
'shapes id 145029 contains 1 sequence errors',
'shapes id 146588 contains 3 sequence errors',
'shapes id 146594 contains 2 sequence errors',
*/
