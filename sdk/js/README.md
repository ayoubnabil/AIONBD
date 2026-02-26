# AIONBD JavaScript SDK

Official JavaScript client for AIONBD HTTP API.

## Requirements

- Node.js `>= 18`

## Install (local workspace)

```bash
cd sdk/js
npm install
```

## Quick Example

```js
import { AionBDClient } from "./src/index.js";

const client = new AionBDClient("http://127.0.0.1:8080", {
  timeoutMs: 5000,
});

const live = await client.live();
console.log(live);

await client.createCollection("demo", 3, true);
await client.upsertPointsBatch("demo", [
  { id: 1, values: [1, 0, 0], payload: { label: "a" } },
  { id: 2, values: [0, 1, 0], payload: { label: "b" } }
]);

const search = await client.searchCollectionTopK("demo", [1, 0, 0], {
  metric: "dot",
  mode: "auto",
  limit: 2,
  includePayload: true,
});
console.log(search);
```

If installed from npm package:

```js
import { AionBDClient } from "@aionbd/sdk-js";
```

## Auth Usage

API key:

```js
const client = new AionBDClient("http://127.0.0.1:8080", {
  apiKey: "secret-key-a",
});
```

Bearer token:

```js
const client = new AionBDClient("http://127.0.0.1:8080", {
  bearerToken: "token-a",
});
```

## API Coverage

- `live()`, `ready()`, `health()`
- `metrics()`, `metricsPrometheus()`
- `distance(left, right, metric)`
- `createCollection(name, dimension, strictFinite)`
- `listCollections()`, `getCollection(name)`, `deleteCollection(name)`
- `upsertPoint(collection, id, values, payload)`
- `upsertPointsBatch(collection, points)`
- `getPoint(collection, id)`, `deletePoint(collection, id)`
- `listPoints(collection, { offset, limit, afterId })`
- `searchCollection(collection, query, options)`
- `searchCollectionTopK(collection, query, options)`
- `searchCollectionTopKBatch(collection, queries, options)`

## Run tests

```bash
cd sdk/js
npm test
```

Integration tests start a local `aionbd-server` process automatically.

## TypeScript support

`index.d.ts` is bundled with the package, so TypeScript projects get type hints without extra setup.

## Release and publish

```bash
cd sdk/js
npm run release:check
npm run publish:dry-run
```

Real publish (when npm auth/scope is ready):

```bash
npm publish --access public
```
