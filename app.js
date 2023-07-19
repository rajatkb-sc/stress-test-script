const autocannon = require("autocannon")
const aggregateResult = autocannon.aggregateResult


const fs = require('fs');
const readline = require('readline');

const FILE_PATH = 'output.json';

const FRS_URL = "http://feed-relevance-service.sharechat.internal"



// Overrides
const RANKER_TEST_VARIANT_TF = "control"
const RANKER_TEST_VARIANT_VF = "control"
const RANKER_TEST_VARIANT_VSF = "control"
const LIMIT = 10

// Batsh size for reading of the disk at once -- Higher the better
const BATCH_SIZE = 10

const WORKER = 32

const RATE_OF_REQUEST = 11000

const TEST_DURATION = 5 * 60

const CONNECTIONS = 1000

const PIPELINING = 1000000

const LANGUAGES = [
    "Bengali",
    "Hindi",
    "Gujarati",
    "Kannada",
    "Malayalam",
    "Marathi",
    "Odia",
    "Punjabi",
    "Tamil",
    "Telugu",
]

async function* loadJSONL(filePath) {

    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    for await (const line of rl) {
        const jsonObject = JSON.parse(line);
        yield jsonObject
    }
}

async function* batchRequests(batchSize = 100000) {
    let i = 0
    let batchedObjects = []

    for await (const jsonObject of loadJSONL(FILE_PATH)) {

        let endpoint = ""

        switch (jsonObject.feedType) {
            case "video-feed": {
                endpoint = "v2/videoFeed"
                break
            }
            case "trending": {
                endpoint = "v2/trendingFeed"
                break
            }
            case "video-suggestion": {
                endpoint = "videoSuggestionFeed"
                break
            }
        }

        if (endpoint == "") {
            continue
        }

        const urls = LANGUAGES.map(lang => {
            return `/${lang}/${endpoint}`
        })

       
        let payload

        const REQUEST_ID = "Request-Id"
        const SESSION_ID = "session-id"

        jsonObject.payload.limit = LIMIT
        jsonObject.payload["syntheticLoad"] = true

        switch (jsonObject.feedType) {
            case "video-feed" : {
                jsonObject.payload.experiment.variant = RANKER_TEST_VARIANT_VF
                jsonObject.payload.experiment.feedRelGlobalFeedExperiment = "control"
                jsonObject.payload.experiment.feedRelGlobalRevenueExperiment = "control"
                payload = {
                    feedType : jsonObject.feedType,
                    payload : jsonObject.payload,
                    headers  : {
                        [REQUEST_ID] : jsonObject.requestId,
                        "SESSION-ID" : jsonObject.sessionId,
                    },
                    
                }
            }
            case "trending" : {
                jsonObject.payload.experiment.variant = RANKER_TEST_VARIANT_TF
                jsonObject.payload.experiment.feedRelGlobalFeedExperiment = "control"
                jsonObject.payload.experiment.feedRelGlobalRevenueExperiment = "control"
                payload = {
                    feedType : jsonObject.feedType,
                    payload : jsonObject.payload,
                    headers : {
                        [REQUEST_ID] : jsonObject.requestId,
                        [SESSION_ID] : jsonObject.sessionId,
                    },
                    
                }
                break
            }
            case "video-suggestion": {
                jsonObject.payload.variant = RANKER_TEST_VARIANT_VSF
                jsonObject.payload.feedRelGlobalFeedExperiment = "control"
                jsonObject.payload.feedRelGlobalRevenueExperiment = "control"
                payload = {
                    feedType : jsonObject.feedType,
                    payload : jsonObject.payload,
                    headers : {
                        [REQUEST_ID] : jsonObject.requestId,
                        [SESSION_ID] : jsonObject.sessionId,
                    }
                    
                }
            }
        }

        urls.forEach((url) => {
            batchedObjects.push({
                ...payload,
                url,
            })
        })

        i++


        if (i === batchSize) {
            yield batchedObjects
            i = 0
            batchedObjects = []
        }
    }


    if (batchedObjects.length !== 0){
        yield batchedObjects
    }

}

// Usage example
(async () => {

    // const results = []

    for await(const obj of batchRequests(BATCH_SIZE)) {

        console.log(`read ${obj.length} records : stressing ${FRS_URL}`)

        const instance = autocannon({
            url: FRS_URL,
            pipelining: PIPELINING,
            method: "POST",
            connectionRate: RATE_OF_REQUEST,
            connections : CONNECTIONS , 
            overallRate: RATE_OF_REQUEST , 
            duration : TEST_DURATION , 
            workers : WORKER,
            requests : obj.map((v => {
                return {
                    body: JSON.stringify(v.payload),
                    headers : v.headers,
                    method : "POST",
                    path : v.url
                }
            })) 
        })

        autocannon.track(instance , {
            progressBarString: true , 
            renderLatencyTable : true , 
            renderProgressBar : true , 
        })

        // results.push(new Promise((resolve ) => {
        //     instance.on("done" , (result) => {
        //        resolve(result)
        //     })
        // }))

        break
    }


})();