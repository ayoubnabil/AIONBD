import assert from "node:assert/strict";
import test from "node:test";

import { AionBDClient, AionBDError } from "../src/index.js";

const originalFetch = global.fetch;

function makeJsonResponse(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json" },
  });
}

test.after(() => {
  global.fetch = originalFetch;
});

test("searchCollectionTopK omits limit when null", async () => {
  const calls = [];
  global.fetch = async (url, init) => {
    calls.push({ url, init });
    return makeJsonResponse({ metric: "dot", hits: [] });
  };

  const client = new AionBDClient("http://unit.test");
  await client.searchCollectionTopK("demo", [1, 2], {
    limit: null,
    metric: "dot",
    mode: "auto",
  });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://unit.test/collections/demo/search/topk");
  const body = JSON.parse(calls[0].init.body);
  assert.deepEqual(body, {
    query: [1, 2],
    metric: "dot",
    mode: "auto",
  });
});

test("listPoints omits limit when null and supports cursor mode", async () => {
  const calls = [];
  global.fetch = async (url, init) => {
    calls.push({ url, init });
    return makeJsonResponse({
      points: [{ id: 1 }, { id: 2 }],
      total: 2,
      next_offset: null,
      next_after_id: null,
    });
  };

  const client = new AionBDClient("http://unit.test");
  await client.listPoints("demo", { limit: null, afterId: 7 });

  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "http://unit.test/collections/demo/points?after_id=7");
});

test("listPoints rejects mixed offset and afterId", async () => {
  const client = new AionBDClient("http://unit.test");

  await assert.rejects(
    () => client.listPoints("demo", { offset: 1, afterId: 2 }),
    /offset must be 0 when afterId is provided/
  );
});

test("client sends auth headers", async () => {
  const calls = [];
  global.fetch = async (url, init) => {
    calls.push({ url, init });
    return makeJsonResponse({ status: "ok" });
  };

  const client = new AionBDClient("http://unit.test", {
    apiKey: "key-a",
    bearerToken: "token-a",
  });

  await client.live();

  const headers = calls[0].init.headers;
  assert.equal(headers["x-api-key"], "key-a");
  assert.equal(headers.Authorization, "Bearer token-a");
});

test("metricsPrometheus returns raw text", async () => {
  global.fetch = async () =>
    new Response("aionbd_collections 3\n", {
      status: 200,
      headers: { "content-type": "text/plain" },
    });

  const client = new AionBDClient("http://unit.test");
  const payload = await client.metricsPrometheus();
  assert.equal(payload, "aionbd_collections 3\n");
});

test("HTTP error is surfaced as AionBDError", async () => {
  global.fetch = async () =>
    new Response('{"error":"boom"}', {
      status: 400,
      headers: { "content-type": "application/json" },
    });

  const client = new AionBDClient("http://unit.test");

  await assert.rejects(
    () => client.live(),
    (error) => {
      assert.ok(error instanceof AionBDError);
      assert.equal(error.status, 400);
      assert.match(error.message, /HTTP 400/);
      return true;
    }
  );
});

test("invalid JSON response is surfaced as AionBDError", async () => {
  global.fetch = async () =>
    new Response("not-json", {
      status: 200,
      headers: { "content-type": "application/json" },
    });

  const client = new AionBDClient("http://unit.test");

  await assert.rejects(
    () => client.live(),
    (error) => {
      assert.ok(error instanceof AionBDError);
      assert.match(error.message, /invalid JSON response/);
      return true;
    }
  );
});
