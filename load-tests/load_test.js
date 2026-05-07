import http from 'k6/http';
import { check, sleep } from 'k6';
import execution from 'k6/execution';

const taskIndex = parseInt(__ENV.CLOUD_RUN_TASK_INDEX || '0', 10);
const taskCount = parseInt(__ENV.CLOUD_RUN_TASK_COUNT || '1', 10);
const dryRun = __ENV.DRY_RUN === 'true';
const mode = __ENV.MODE || 'run'; // 'setup' or 'run'

const TOTAL_DOCS = dryRun ? 10000 : 100000000;
const DOCS_PER_TASK = TOTAL_DOCS / taskCount;

const qpsPerTask = parseInt(__ENV.QPS_PER_TASK || (dryRun ? '100' : '500'), 10);
const durationSeconds = mode === 'setup' ? (DOCS_PER_TASK / qpsPerTask) : 604800; // 7 days for run

const ratePerMinute = Math.round((qpsPerTask * 60) / 100);
let scenario = {};

if (mode === 'setup') {
    // Flat 10K QPS total across 40 tasks for the Data Loader (250 QPS/task = 15,000 rate/min per task)
    const loaderRate = Math.round((250 * 60) / 100);
    scenario = {
        executor: 'constant-arrival-rate',
        rate: loaderRate,
        timeUnit: '1m',
        duration: `${durationSeconds}s`,
        preAllocatedVUs: 150,
        maxVUs: 1000,
    };
} else {
    // Ramping background live traffic to prevent minute-1 database throttling:
    // Starting at 2K QPS total across 40 tasks (50 QPS/task = 3,000 rate/min per task)
    // Stage 1 (0-5 min)  : Stay at 2K QPS (50 QPS/task)
    // Stage 2 (5-10 min) : Ramp to 4K QPS (100 QPS/task)
    // Stage 3 (10-15 min): Ramp to 8K QPS (200 QPS/task)
    // Stage 4 (15+ min)  : Stay at static 10K QPS (250 QPS/task) target
    const rate50 = Math.round((50 * 60) / 100);
    const rate100 = Math.round((100 * 60) / 100);
    const rate200 = Math.round((200 * 60) / 100);
    const rate250 = Math.round((250 * 60) / 100);

    scenario = {
        executor: 'ramping-arrival-rate',
        startRate: rate50, 
        timeUnit: '1m',
        preAllocatedVUs: 50,
        maxVUs: 500,
        stages: [
            { target: rate50, duration: '300s' },   // 5 mins at 2K QPS
            { target: rate100, duration: '300s' },  // Ramp to 4K QPS in 5 mins
            { target: rate200, duration: '300s' },  // Ramp to 8K QPS in 5 mins
            { target: rate250, duration: '300s' },  // Ramp to static 10K QPS in 5 mins
            { target: rate250, duration: `${durationSeconds}s` } // Stay at 10K QPS until completed
        ],
    };
}

export const options = {
    scenarios: {
        load_test_scenario: scenario
    },
    discardResponseBodies: true,
};

export function setup() {
    if (!__ENV.TARGET_URL) {
        throw new Error("TARGET_URL environment variable is required.");
    }
    return {};
}

const setupStart = taskIndex * DOCS_PER_TASK;
const setupEnd = (taskIndex + 1) * DOCS_PER_TASK - 1;

const insertStart = 100000000 + (taskIndex * 100000000);
let localInsertCounter = 0;

const PRIME = 999983; // Coprime to 1M and 1k

let cachedToken = '';
let tokenFetchTime = 0;

function getAuthToken() {
    const jitter = Math.floor(Math.random() * 300000); // 5 min jitter
    // Refresh after 30 minutes (1800000 ms) instead of 50 minutes
    if (cachedToken && (Date.now() - tokenFetchTime < 1800000 - jitter)) {
        return cachedToken;
    }
    const targetUrl = __ENV.TARGET_URL;
    for (let i = 0; i < 3; i++) {
        try {
            console.log('Attempting to fetch identity token from metadata server...');
            const res = http.get('http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=' + targetUrl, {
                headers: { 'Metadata-Flavor': 'Google' },
                responseType: 'text', // Fixes the null body issue
            });
            console.log(`Metadata server response status: ${res.status}`);
            if (res.status === 200 && res.body) {
                console.log('Successfully acquired identity token. Length: ' + res.body.length + ', Prefix: ' + res.body.substring(0, 10));
                cachedToken = res.body.trim();
                tokenFetchTime = Date.now();
                return cachedToken;
            } else {
                console.log(`Failed to acquire token. Status: ${res.status}, Body: ${res.body}`);
            }
            sleep(Math.pow(2, i));
        } catch (e) {
            console.log('Metadata server error: ' + e.message);
            sleep(Math.pow(2, i));
        }
    }
    if (cachedToken) {
        console.log('Warning: Failed to refresh token, using cached (potentially expired) token.');
    }
    return cachedToken;
}

let currentIdx = 0;

export default function () {
    const baseUrl = __ENV.TARGET_URL;
    if (!baseUrl) {
        throw new Error("TARGET_URL environment variable is required.");
    }

    const headers = { 'Content-Type': 'text/plain' }; // Changed to text/plain for line-separated body
    const token = getAuthToken();
    if (token) {
        headers['Authorization'] = 'Bearer ' + token;
    }

    let body = '';
    const batchSize = 100;

    if (mode === 'setup') {
        // Stop if we have successfully completed all our assigned docs
        if (currentIdx >= DOCS_PER_TASK) {
            console.log(`Task index ${taskIndex} successfully completed all ${DOCS_PER_TASK} inserts. Aborting task.`);
            execution.test.abort(`Task completed all ${DOCS_PER_TASK} inserts successfully!`);
            return;
        }
        
        // Generate batch using currentIdx
        for (let i = 0; i < batchSize; i++) {
            const idx = currentIdx + i;
            if (idx >= DOCS_PER_TASK) {
                break;
            }
            const permutedIdx = (idx * PRIME) % DOCS_PER_TASK;
            const id = formatId(setupStart + permutedIdx);
            body += `${id}=insert\n`;
        }
    } else {
        for (let i = 0; i < batchSize; i++) {
            let op = "insert";
            let id = "";
            const rand = Math.random();
            if (rand < 0.40) {
                op = "insert";
                localInsertCounter++;
                id = formatId(insertStart + localInsertCounter);
            } else if (rand < 0.70) {
                op = "update";
                id = pickRandomId();
            } else {
                op = "delete";
                id = pickRandomId();
            }
            body += `${id}=${op}\n`;
        }
    }

    if (body === '') {
        return;
    }

    const res = http.post(baseUrl, body, { headers: headers });

    if (res.status === 200) {
        if (mode === 'setup') {
            // Only advance pointer on successful commit (HTTP 200)
            currentIdx += batchSize;
            if (currentIdx >= DOCS_PER_TASK) {
                console.log(`Task index ${taskIndex} reached target ${DOCS_PER_TASK} successful inserts. Aborting.`);
                execution.test.abort(`Task completed all ${DOCS_PER_TASK} inserts successfully!`);
            }
        }
    } else {
        console.log(`Request failed with status ${res.status}.`);
        if (mode === 'setup') {
            console.log(`Task index ${taskIndex}: Batch failed. Will retry index range ${currentIdx} to ${currentIdx + batchSize - 1}.`);
            // We do NOT advance currentIdx, so it will retry in the next iteration.
        }
    }
}

function formatId(idx) {
    return 'load-' + String(idx).padStart(10, '0');
}

function pickRandomId() {
    const totalLocalPool = DOCS_PER_TASK + localInsertCounter;
    const randIdx = Math.floor(Math.random() * totalLocalPool);

    if (randIdx < DOCS_PER_TASK) {
        return formatId(setupStart + randIdx);
    } else {
        return formatId(insertStart + (randIdx - DOCS_PER_TASK));
    }
}
