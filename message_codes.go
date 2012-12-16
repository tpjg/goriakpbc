package riak

import (
    "code.google.com/p/goprotobuf/proto"
)

var messageCodes = map[string]byte{
    "RpbErrorResp":         0,
    "RpbPingReq":           1,
    "RpbPingResp":          2,
    "RpbGetClientIdReq":    3,
    "RpbGetClientIdResp":   4,
    "RpbSetClientIdReq":    5,
    "RpbSetClientIdResp":   6,
    "RpbGetServerInfoReq":  7,
    "RpbGetServerInfoResp": 8,
    "RpbGetReq":            9,
    "RpbGetResp":           10,
    "RpbPutReq":            11,
    "RpbPutResp":           12,
    "RpbDelReq":            13,
    "RpbDelResp":           14,
    "RpbListBucketsReq":    15,
    "RpbListBucketsResp":   16,
    "RpbListKeysReq":       17,
    "RpbListKeysResp":      18,
    "RpbGetBucketReq":      19,
    "RpbGetBucketResp":     20,
    "RpbSetBucketReq":      21,
    "RpbSetBucketResp":     22,
    "RpbMapRedReq":         23,
    "RpbMapRedResp":        24,
    "RpbIndexReq":          25,
    "RpbIndexResp":         26,
    "RpbSearchQueryReq":    27,
    "RbpSearchQueryResp":   28,
}

var codeToMessage = map[byte]string{
    0:  "RpbErrorResp",
    1:  "RpbPingReq",
    2:  "RpbPingResp",
    3:  "RpbGetClientIdReq",
    4:  "RpbGetClientIdResp",
    5:  "RpbSetClientIdReq",
    6:  "RpbSetClientIdResp",
    7:  "RpbGetServerInfoReq",
    8:  "RpbGetServerInfoResp",
    9:  "RpbGetReq",
    10: "RpbGetResp",
    11: "RpbPutReq",
    12: "RpbPutResp",
    13: "RpbDelReq",
    14: "RpbDelResp",
    15: "RpbListBucketsReq",
    16: "RpbListBucketsResp",
    17: "RpbListKeysReq",
    18: "RpbListKeysResp",
    19: "RpbGetBucketReq",
    20: "RpbGetBucketResp",
    21: "RpbSetBucketReq",
    22: "RpbSetBucketResp",
    23: "RpbMapRedReq",
    24: "RpbMapRedResp",
}

func NewMessage(code byte) proto.Message {
    switch codeToMessage[code] {
    case "RpbListKeysResp":
        return new(RpbListKeysResp)
    case "RpbMapRedResp":
        return new(RpbMapRedResp)
    }
    return nil
}
