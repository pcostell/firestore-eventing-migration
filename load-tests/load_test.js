import http from 'k6/http';
import { check, sleep } from 'k6';
import execution from 'k6/execution';

const taskIndex = parseInt(__ENV.CLOUD_RUN_TASK_INDEX || '0', 10);
const taskCount = parseInt(__ENV.CLOUD_RUN_TASK_COUNT || '1', 10);
const dryRun = __ENV.DRY_RUN === 'true';
const mode = __ENV.MODE || 'run'; // 'setup' or 'run'

const TOTAL_DOCS = dryRun ? 10000 : 20000000;
const DOCS_PER_TASK = TOTAL_DOCS / taskCount;

const qpsPerTask = parseInt(__ENV.QPS_PER_TASK || (dryRun ? '100' : '500'), 10);
const durationSeconds = mode === 'setup' ? (DOCS_PER_TASK / qpsPerTask) : 604800; // 7 days for run

export const options = {
    scenarios: {
        constant_request_rate: {
            executor: 'constant-arrival-rate',
            rate: qpsPerTask / 100,
            timeUnit: '1s',
            duration: `${durationSeconds}s`,
            preAllocatedVUs: 100,
            maxVUs: 500,
        },
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
        const baseIdx = execution.scenario.iterationInTest * batchSize;
        for (let i = 0; i < batchSize; i++) {
            const idx = baseIdx + i;
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

    if (res.status !== 200) {
        console.log(`Request failed with status ${res.status}`);
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
