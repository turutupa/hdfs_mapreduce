package common

// client requests type
const LS = "LS"
const GET = "GET"
const PUT = "PUT"
const RM = "RM"
const COMPUTE = "COMPUTE"
const CLUSTER_STATS = "CLUSTER-STATS"

// modify this for bigger chunks
const CHUNK_SIZE int64 = 1 << 18 // 1MB
//const CHUNK_SIZE int64 = (1 << 20) * 100 // 1MB * 100 = 100MB

const COMPUTE_ENGINE = "COMPUTE_ENGINE"
