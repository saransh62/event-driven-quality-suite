// k6 load test for POST /orders.
//   k6 run load-tests/orders-load.js                     (PROFILE=load: ~2.5 min, up to 100 req/s)
//   k6 run -e PROFILE=smoke load-tests/orders-load.js    (CI: ~40 s, up to 20 req/s)
import http from 'k6/http';
import { check } from 'k6';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';

const BASE_URL = __ENV.ORDER_URL || 'http://localhost:8081';
const PROFILE = __ENV.PROFILE || 'load';

const profiles = {
  smoke: [
    { target: 10, duration: '10s' },
    { target: 20, duration: '20s' },
    { target: 0, duration: '10s' },
  ],
  load: [
    { target: 25, duration: '30s' },
    { target: 100, duration: '60s' },
    { target: 100, duration: '45s' },
    { target: 0, duration: '15s' },
  ],
};

export const options = {
  scenarios: {
    create_orders: {
      // Open model: k6 starts N requests/second regardless of how fast the server answers,
      // so a slow server shows up as latency/errors instead of silently lowering the load.
      executor: 'ramping-arrival-rate',
      startRate: 1,
      timeUnit: '1s',
      preAllocatedVUs: 20,
      maxVUs: 200,
      stages: profiles[PROFILE],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<300'], // 95% of requests faster than 300 ms
    http_req_failed: ['rate<0.01'],   // fewer than 1% errors
    checks: ['rate>0.99'],
  },
};

export default function () {
  const payload = JSON.stringify({
    customerEmail: `load-${__VU}-${__ITER}@example.com`,
    product: 'LoadTestItem',
    quantity: 1 + (__ITER % 5),
    amount: 9.99,
  });
  const res = http.post(`${BASE_URL}/orders`, payload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'POST /orders' },
  });
  check(res, {
    'status is 201': (r) => r.status === 201,
    'has order id': (r) => r.status === 201 && !!r.json('id'),
  });
}

export function handleSummary(data) {
  const out = __ENV.SUMMARY_FILE || 'load-tests/results/k6-summary.json';
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: false }),
    [out]: JSON.stringify(data, null, 2),
  };
}
