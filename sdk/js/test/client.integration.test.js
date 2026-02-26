import assert from "node:assert/strict";
import { once } from "node:events";
import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import net from "node:net";
import test from "node:test";

import { AionBDClient } from "../src/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "../../..");

let serverProcess;
let baseUrl;
let client;
const serverLogs = [];

function captureLine(prefix, chunk) {
  const text = chunk.toString("utf8");
  for (const line of text.split(/\r?\n/)) {
    if (line.length > 0) {
      serverLogs.push(`${prefix}: ${line}`);
      if (serverLogs.length > 400) {
        serverLogs.shift();
      }
    }
  }
}

async function getFreePort() {
  return new Promise((resolvePort, reject) => {
    const server = net.createServer();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("failed to acquire test port"));
        return;
      }
      const port = address.port;
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolvePort(port);
      });
    });
  });
}

async function waitForServerReady(url, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (serverProcess.exitCode !== null) {
      throw new Error(
        `aionbd-server exited early with code ${serverProcess.exitCode}\n${serverLogs.join("\n")}`,
      );
    }
    try {
      const response = await fetch(`${url}/live`);
      if (response.ok) {
        return;
      }
    } catch {
      // retry
    }
    await delay(250);
  }
  throw new Error(
    `timeout waiting for aionbd-server on ${url}\n${serverLogs.join("\n")}`,
  );
}

async function stopServer() {
  if (!serverProcess || serverProcess.exitCode !== null) {
    return;
  }

  serverProcess.kill("SIGTERM");
  const exited = Promise.race([
    once(serverProcess, "exit"),
    delay(5000).then(async () => {
      if (serverProcess.exitCode === null) {
        serverProcess.kill("SIGKILL");
        await once(serverProcess, "exit");
      }
    }),
  ]);

  await exited;
}

test.before(async () => {
  const port = await getFreePort();
  baseUrl = `http://127.0.0.1:${port}`;

  serverProcess = spawn("cargo", ["run", "-q", "-p", "aionbd-server"], {
    cwd: repoRoot,
    env: {
      ...process.env,
      AIONBD_BIND: `127.0.0.1:${port}`,
      AIONBD_PERSISTENCE_ENABLED: "false",
      AIONBD_WAL_SYNC_ON_WRITE: "false",
      RUST_LOG: "warn",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  serverProcess.stdout.on("data", (chunk) => captureLine("stdout", chunk));
  serverProcess.stderr.on("data", (chunk) => captureLine("stderr", chunk));

  await waitForServerReady(baseUrl, 90000);
  client = new AionBDClient(baseUrl, { timeoutMs: 10000 });
});

test.after(async () => {
  await stopServer();
});

test("sdk JS end-to-end operations against real aionbd-server", async () => {
  const name = `js_sdk_demo_${Date.now()}_${Math.floor(Math.random() * 10000)}`;

  const live = await client.live();
  assert.equal(live.status, "live");

  const ready = await client.ready();
  assert.equal(ready.status, "ready");

  const created = await client.createCollection(name, 4, true);
  assert.equal(created.name, name);
  assert.equal(created.dimension, 4);

  try {
    const distance = await client.distance([1, 0, 0, 0], [1, 0, 0, 0], "l2");
    assert.equal(distance.metric, "l2");
    assert.equal(distance.value, 0);

    const upsert1 = await client.upsertPoint(name, 1, [1, 0, 0, 0], {
      label: "alpha",
    });
    assert.equal(upsert1.id, 1);

    const batch = await client.upsertPointsBatch(name, [
      { id: 2, values: [0.8, 0.1, 0, 0], payload: { label: "beta" } },
      { id: 3, values: [0, 1, 0, 0], payload: { label: "gamma" } },
    ]);
    assert.equal(batch.created + batch.updated, 2);

    const point2 = await client.getPoint(name, 2);
    assert.equal(point2.id, 2);
    assert.deepEqual(point2.payload, { label: "beta" });

    const top1 = await client.searchCollection(name, [1, 0, 0, 0], {
      metric: "dot",
      mode: "exact",
      includePayload: true,
    });
    assert.equal(top1.id, 1);

    const topk = await client.searchCollectionTopK(name, [1, 0, 0, 0], {
      metric: "dot",
      mode: "auto",
      limit: 2,
      includePayload: true,
    });
    assert.equal(topk.metric, "dot");
    assert.equal(Array.isArray(topk.hits), true);
    assert.equal(topk.hits.length, 2);
    assert.equal(topk.hits[0].id, 1);

    const batchSearch = await client.searchCollectionTopKBatch(
      name,
      [
        [1, 0, 0, 0],
        [0, 1, 0, 0],
      ],
      {
        metric: "dot",
        mode: "auto",
        limit: 2,
      },
    );
    assert.equal(batchSearch.metric, "dot");
    assert.equal(batchSearch.results.length, 2);
    assert.equal(Array.isArray(batchSearch.results[0].hits), true);

    const listedOffset = await client.listPoints(name, {
      limit: 10,
      offset: 0,
    });
    const offsetIds = listedOffset.points.map((item) => item.id);
    assert.deepEqual(offsetIds, [1, 2, 3]);

    const listedCursor = await client.listPoints(name, {
      limit: 10,
      afterId: 1,
    });
    const cursorIds = listedCursor.points.map((item) => item.id);
    assert.deepEqual(cursorIds, [2, 3]);

    const deletedPoint = await client.deletePoint(name, 3);
    assert.equal(deletedPoint.id, 3);
    assert.equal(deletedPoint.deleted, true);

    const metrics = await client.metrics();
    assert.equal(typeof metrics.collections, "number");

    const promText = await client.metricsPrometheus();
    assert.match(promText, /aionbd_/);
  } finally {
    await client.deleteCollection(name);
  }
});
