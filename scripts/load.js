import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const base = (__ENV.BASE_URL || 'http://127.0.0.1:8080').replace(/\/$/, '');
const token = __ENV.API_TOKEN;
const rate = Number(__ENV.RATE || 110);
const duration = __ENV.DURATION || '10m';
const business = new Counter('business_requests');
const successful = new Counter('business_successes');
const failures = new Rate('business_failures');
const latency = new Trend('business_duration', true);

export const options = {
  scenarios: {
    authenticated_api: {
      executor: 'constant-arrival-rate',
      rate,
      timeUnit: '1s',
      duration,
      preAllocatedVUs: 30,
      maxVUs: 150,
      gracefulStop: '10s',
    },
  },
  thresholds: {
    business_failures: ['rate<0.01'],
    business_duration: ['p(95)<200', 'p(99)<500'],
    dropped_iterations: ['count==0'],
    // A 110/s arrival rate leaves room for setup/teardown in k6 rate accounting.
    // The acceptance threshold applies to successful business responses.
    business_successes: ['rate>=100'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  if (!token) fail('Set API_TOKEN to a valid gateway Bearer token.');
  if (!Number.isInteger(rate) || rate < 1) fail('RATE must be a positive integer.');
  const ready = http.get(`${base}/readyz`, { tags: { phase: 'setup' } });
  if (ready.status !== 200) fail(`Gateway is not ready: HTTP ${ready.status}`);
  const probe = http.get(`${base}/api/v1/applications`, {
    headers: { Authorization: `Bearer ${token}` },
    tags: { phase: 'setup' },
  });
  if (probe.status !== 200) fail(`API authentication failed: HTTP ${probe.status}`);
}

const paths = [
  '/api/v1/clusters',
  '/api/v1/applications',
  '/api/v1/applications/genomics/queues',
  '/api/v1/workflows',
  '/api/v1/jobs',
  '/api/v1/files',
  '/api/v1/runs',
];

export default function () {
  const path = paths[__ITER % paths.length];
  const response = http.get(`${base}${path}`, {
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json' },
    tags: { phase: 'business', endpoint: path },
    timeout: '5s',
  });
  const ok = response.status === 200;
  business.add(1);
  if (ok) successful.add(1);
  failures.add(!ok);
  latency.add(response.timings.duration);
  check(response, { 'business HTTP 200': () => ok });
}
