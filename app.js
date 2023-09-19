const autocannon = require("autocannon")
const aggregateResult = autocannon.aggregateResult


const fs = require('fs');
const { config } = require("process");
const readline = require('readline');

const FILE_PATH = 'output.json';

const FRS_URL = "http://feed-relevance-service.sharechat.internal"



// Overrides
// const RANKER_TEST_VARIANT_TF = "control"
// const RANKER_TEST_VARIANT_VF = "control"
// const RANKER_TEST_VARIANT_VSF = "control"
const RANKER_GLOBAL_FEED_VARIANT = "variant-gf-13"
const LIMIT = 10



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
                // jsonObject.payload.experiment.variant = RANKER_TEST_VARIANT_VF
                jsonObject.payload.experiment.feedRelGlobalFeedExperiment = RANKER_GLOBAL_FEED_VARIANT
                // jsonObject.payload.experiment.feedRelGlobalRevenueExperiment = "control"
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
                // jsonObject.payload.experiment.variant = RANKER_TEST_VARIANT_TF
                jsonObject.payload.experiment.feedRelGlobalFeedExperiment = RANKER_GLOBAL_FEED_VARIANT
                // jsonObject.payload.experiment.feedRelGlobalRevenueExperiment = "control"
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
                // jsonObject.payload.variant = RANKER_TEST_VARIANT_VSF
                jsonObject.payload.feedRelGlobalFeedExperiment = RANKER_GLOBAL_FEED_VARIANT
                // jsonObject.payload.feedRelGlobalRevenueExperiment = "control"
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



    const configs = [
        {
            BATCH_SIZE: 1000000,
            WORKER: 32,
            RATE_OF_REQUEST: 1000,
            TEST_DURATION: 5 * 60, // warm up
            CONNECTIONS: 1000,
            PIPELINING: 1000,
            STEP_UP : 100,
            MAX_REQ: 12000
        },
        {
            BATCH_SIZE: 10000,
            WORKER: 32,
            RATE_OF_REQUEST: 2000,
            TEST_DURATION: 5 * 60, // warm up
            CONNECTIONS: 1000,
            PIPELINING: 1000,
            STEP_UP : 1000,
            MAX_REQ: 12000
        },
        // smaller batches quicker load 
        {
            BATCH_SIZE: 1000,
            WORKER: 64,
            RATE_OF_REQUEST: 6000,
            TEST_DURATION: 60,
            CONNECTIONS: 1000,
            PIPELINING: 10000,
            STEP_UP : 2000,
            MAX_REQ: 12000
        },
        // smaller batches qicker load
        {
            BATCH_SIZE: 1000,
            WORKER: 64,
            RATE_OF_REQUEST: 8000,
            TEST_DURATION: 60,
            CONNECTIONS: 10000,
            PIPELINING: 1000000,
            STEP_UP: 3000,
            MAX_REQ: 12000,

        },
        {
            BATCH_SIZE: 1000,
            WORKER: 64,
            RATE_OF_REQUEST: 8000,
            TEST_DURATION: 60,
            CONNECTIONS: 10000,
            PIPELINING: 1000000,
            STEP_UP: 3000,
            MAX_REQ: 12000
        },
        {
            BATCH_SIZE: 1000,
            WORKER: 64,
            RATE_OF_REQUEST: 8000,
            TEST_DURATION: 60,
            CONNECTIONS: 10000,
            PIPELINING: 1000000,
            STEP_UP: 3000,
            MAX_REQ: 12000
        },
        {
            BATCH_SIZE: 1000,
            WORKER: 64,
            RATE_OF_REQUEST: 1000,
            TEST_DURATION: 60,
            CONNECTIONS: 10000,
            PIPELINING: 1000000,
            STEP_UP: 3000,
            MAX_REQ: 20000
        },
    ];


    for (const config of configs) {
        let stepUp = config.STEP_UP
        let i = 0;
        for await(const obj of batchRequests(config.BATCH_SIZE)) {
            currentRate = config.RATE_OF_REQUEST + i * stepUp
            if(currentRate >= config.RATE_OF_REQUEST){
                stepUp = 0
            }
            console.log(`read ${obj.length} records : stressing ${FRS_URL}`)
    
            const instance = autocannon({
                url: FRS_URL,
                pipelining: config.PIPELINING,
                method: "POST",
                connectionRate: config.RATE_OF_REQUEST + i * stepUp,
                connections : config.CONNECTIONS , 
                overallRate: currentRate ,
                duration : config.TEST_DURATION , 
                workers : config.WORKER,
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
    
    
            await (async () => {
                return (new Promise((resolve ) => {
                    instance.on("done" , (result) => {
                       resolve(result)
                    })
                }))
            })()
            i++
        }
    }


    


})();