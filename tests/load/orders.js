import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  vus: 50,
  duration: '1m',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
    checks: ['rate>0.99'],
  },
};

export default function () {
  const response = http.post(
    'http://localhost:8080/orders',
    JSON.stringify({ amount_cents: 2500 }),
    { headers: { 'Content-Type': 'application/json' } }
  );

  check(response, {
    'order created': (r) => r.status === 201,
  });

  sleep(0.1);
}
