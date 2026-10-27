
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

const BASE_URL = 'http://localhost:8080';

const sagaDuration = new Trend('saga_completion_duration', true);
const sagaCompleted = new Rate('saga_completion_rate');
const sagaFailed = new Counter('saga_failed');
const sagaTimedOut = new Counter('saga_timed_out');

export const options = {
  vus: 10,
  duration: '1m',
  thresholds: {
    http_req_failed: ['rate<0.01'],
    checks: ['rate>0.99'],
    saga_completion_rate: ['rate>0.95'],
    saga_completion_duration: ['p(95)<10000'],
  },
};

export default function () {
  const createResponse = http.post(
    `${BASE_URL}/orders`,
    JSON.stringify({ amount_cents: 2500 }),
    {
      headers: { 'Content-Type': 'application/json' },
      tags: { endpoint: 'create_order' },
    }
  );

  const created = check(createResponse, {
    'order created': (r) => r.status === 201,
  });

  if (!created) {
    sagaFailed.add(1);
    sagaCompleted.add(false);
    return;
  }

  let orderId;

  try {
    orderId = createResponse.json('id');
  } catch (_) {
    sagaFailed.add(1);
    sagaCompleted.add(false);
    return;
  }

  if (!orderId) {
    sagaFailed.add(1);
    sagaCompleted.add(false);
    return;
  }

  const start = Date.now();
  const deadline = start + 30000;
  let terminal = false;
  let successful = false;

  while (Date.now() < deadline) {
    const response = http.get(
      `${BASE_URL}/orders/${orderId}`,
      { tags: { endpoint: 'get_order' } }
    );

    if (response.status === 200) {
      const status = response.json('status');

      if (status === 'CONFIRMED') {
        terminal = true;
        successful = true;
        break;
      }

      if (
        status === 'CANCELLED_REFUNDED' ||
        status === 'PAYMENT_FAILED' ||
        status === 'REFUND_FAILED'
      ) {
        terminal = true;
        break;
      }
    }

    sleep(0.5);
  }

  if (terminal) {
    sagaDuration.add(Date.now() - start);
    sagaCompleted.add(successful);

    if (!successful) {
      sagaFailed.add(1);
    }
  } else {
    sagaTimedOut.add(1);
    sagaCompleted.add(false);
  }
}
