const autocannon = require("autocannon")

const fs = require('fs');
const readline = require('readline');

const FILE_PATH = 'output.json';

const FRS_URL = "http://feed-relevance-service.sharechat.internal"



// Overrides
const RANKER_TEST_VARIANT_TF = "variant-tf-13"
const RANKER_TEST_VARIANT_VF = "variant-vf-13"
const RANKER_TEST_VARIANT_VSF = "variant-vs-13"
const RANKER_GLOBAL_FEED_VARIANT = "variant-gf-15"
const LIMIT = 20




async function* loadJSONL(filePath, startLine = -1) {
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineCount = 0;

    for await (const line of rl) {
        lineCount++;
        if (lineCount < startLine) {
            // Skip lines until the desired starting line is reached
            continue;
        }

        const jsonObject = JSON.parse(line);
        yield jsonObject;
    }
}


async function getLineCount(filePath) {
    return new Promise((resolve, reject) => {
        exec(`wc -l ${filePath}`, (error, stdout, stderr) => {
            if (error) {
                reject(`Error executing wc command: ${error.message}`);
                return;
            }

            if (stderr) {
                reject(`Error from wc command: ${stderr}`);
                return;
            }

            // Parse the output to extract the line count
            const lineCount = parseInt(stdout.trim().split(' ')[0], 10);

            resolve(lineCount);
        });
    });
}



async function* batchRequests(batchSize = 100000 , randomlineStart = -1) {
    console.log("using batch size :" , batchSize , " random line :" , randomlineStart)
    let i = 0
    let batchedObjects = []

    for await (const jsonObject of loadJSONL(FILE_PATH , randomlineStart)) {

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

       
        let payload

        const REQUEST_ID = "Request-Id"
        const SESSION_ID = "session-id"

        jsonObject.payload.limit = LIMIT
        jsonObject.payload["syntheticLoad"] = true

        const lang = jsonObject.language

        switch (jsonObject.feedType) {
            case "video-feed" : {
                jsonObject.payload.experiment.variant = RANKER_TEST_VARIANT_VF
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
                jsonObject.payload.experiment.variant = RANKER_TEST_VARIANT_TF
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
                jsonObject.payload.variant = RANKER_TEST_VARIANT_VSF
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

        payload = {
            ...payload,
            url : `/${lang}/${endpoint}`
        }

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


    const totalLines = getLineCount(FILE_PATH)

    console.log(`reading : ${FILE_PATH} lines total : ${totalLines}`)

    const configs = [
        {
            BATCH_SIZE: 100000,
            RATE_OF_REQUEST: 1000,
            TEST_DURATION:   60, // warm up
            CONNECTIONS: 1000,
            PIPELINING: 10000,
            STEP_UP : 50,
            MAX_RATE_OF_REQUEST: 2000
        },
    ];
   

    for (const config of configs) {

        
        const batchSize = config.BATCH_SIZE
        
        const duration = config.TEST_DURATION

        const stepUp = config.STEP_UP

        const totalDuration = Math.floor(totalLines / batchSize) * duration

        console.log(`Stress test started : estimated duration ${totalDuration}`)

        let i = 0;

        const randomStartLine = Math.floor(Math.random() * totalLines) + 1;

        for await(const obj of batchRequests(config.BATCH_SIZE , randomStartLine)) {
            console.log(`read ${obj.length} records : stressing ${FRS_URL}` , config)
    
            const instance = autocannon({
                url: FRS_URL,
                pipelining: config.PIPELINING,
                method: "POST",
                connectionRate: config.RATE_OF_REQUEST + i * stepUp,
                connections : config.CONNECTIONS , 
                overallRate: config.RATE_OF_REQUEST + i * stepUp, 
                duration : config.TEST_DURATION , 
                workers : 1,
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