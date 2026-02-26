const DEFAULT_BASE_URL = "http://127.0.0.1:8080";
const DEFAULT_TIMEOUT_MS = 5000;

export class AionBDError extends Error {
  constructor(message, options = {}) {
    super(message, { cause: options.cause });
    this.name = "AionBDError";
    this.status = options.status;
    this.method = options.method;
    this.path = options.path;
    this.body = options.body;
  }
}

function escapePathSegment(value) {
  return encodeURIComponent(String(value));
}

export class AionBDClient {
  constructor(baseUrl = DEFAULT_BASE_URL, options = {}) {
    if (typeof baseUrl === "object" && baseUrl !== null) {
      options = baseUrl;
      baseUrl = options.baseUrl ?? DEFAULT_BASE_URL;
    }

    this.baseUrl = String(baseUrl).replace(/\/+$/, "");
    this.timeoutMs = Number(options.timeoutMs ?? DEFAULT_TIMEOUT_MS);
    this.apiKey = options.apiKey ?? null;
    this.bearerToken = options.bearerToken ?? null;
    this.defaultHeaders = { ...(options.headers ?? {}) };
  }

  async live() {
    return this._request("GET", "/live");
  }

  async ready() {
    return this._request("GET", "/ready");
  }

  async health() {
    return this.ready();
  }

  async metrics() {
    return this._request("GET", "/metrics");
  }

  async metricsPrometheus() {
    return this._request("GET", "/metrics/prometheus", { raw: true });
  }

  async distance(left, right, metric = "dot") {
    return this._request("POST", "/distance", {
      body: { left, right, metric },
    });
  }

  async createCollection(name, dimension, strictFinite = true) {
    return this._request("POST", "/collections", {
      body: {
        name,
        dimension,
        strict_finite: strictFinite,
      },
    });
  }

  async listCollections() {
    return this._request("GET", "/collections");
  }

  async getCollection(name) {
    return this._request("GET", `/collections/${escapePathSegment(name)}`);
  }

  async searchCollection(collection, query, options = {}) {
    const body = {
      query,
      metric: options.metric ?? "dot",
      mode: options.mode ?? "auto",
    };

    if (options.targetRecall !== undefined && options.targetRecall !== null) {
      body.target_recall = Number(options.targetRecall);
    }
    if (options.filter !== undefined) {
      body.filter = options.filter;
    }
    if (options.includePayload !== undefined) {
      body.include_payload = Boolean(options.includePayload);
    }

    return this._request(
      "POST",
      `/collections/${escapePathSegment(collection)}/search`,
      { body },
    );
  }

  async searchCollectionTopK(collection, query, options = {}) {
    const limit = options.limit === undefined ? 10 : options.limit;
    const body = {
      query,
      metric: options.metric ?? "dot",
      mode: options.mode ?? "auto",
    };

    if (limit !== null) {
      const normalized = Number(limit);
      if (!Number.isInteger(normalized) || normalized <= 0) {
        throw new TypeError("limit must be a positive integer or null");
      }
      body.limit = normalized;
    }

    if (options.targetRecall !== undefined && options.targetRecall !== null) {
      body.target_recall = Number(options.targetRecall);
    }
    if (options.filter !== undefined) {
      body.filter = options.filter;
    }
    if (options.includePayload !== undefined) {
      body.include_payload = Boolean(options.includePayload);
    }

    return this._request(
      "POST",
      `/collections/${escapePathSegment(collection)}/search/topk`,
      { body },
    );
  }

  async searchCollectionTopKBatch(collection, queries, options = {}) {
    const limit = options.limit === undefined ? 10 : options.limit;
    const body = {
      queries,
      metric: options.metric ?? "dot",
      mode: options.mode ?? "auto",
    };

    if (limit !== null) {
      const normalized = Number(limit);
      if (!Number.isInteger(normalized) || normalized <= 0) {
        throw new TypeError("limit must be a positive integer or null");
      }
      body.limit = normalized;
    }

    if (options.targetRecall !== undefined && options.targetRecall !== null) {
      body.target_recall = Number(options.targetRecall);
    }
    if (options.filter !== undefined) {
      body.filter = options.filter;
    }
    if (options.includePayload !== undefined) {
      body.include_payload = Boolean(options.includePayload);
    }

    return this._request(
      "POST",
      `/collections/${escapePathSegment(collection)}/search/topk/batch`,
      { body },
    );
  }

  async upsertPoint(collection, pointId, values, payload) {
    const body = { values };
    if (payload !== undefined) {
      body.payload = payload;
    }

    return this._request(
      "PUT",
      `/collections/${escapePathSegment(collection)}/points/${escapePathSegment(pointId)}`,
      { body },
    );
  }

  async upsertPointsBatch(collection, points) {
    return this._request(
      "POST",
      `/collections/${escapePathSegment(collection)}/points`,
      {
        body: { points },
      },
    );
  }

  async getPoint(collection, pointId) {
    return this._request(
      "GET",
      `/collections/${escapePathSegment(collection)}/points/${escapePathSegment(pointId)}`,
    );
  }

  async listPoints(collection, options = {}) {
    const offset = options.offset === undefined ? 0 : options.offset;
    const limit = options.limit === undefined ? 100 : options.limit;
    const afterId = options.afterId === undefined ? null : options.afterId;

    if (afterId !== null && Number(offset) !== 0) {
      throw new TypeError("offset must be 0 when afterId is provided");
    }

    const params = new URLSearchParams();
    if (limit !== null) {
      const normalizedLimit = Number(limit);
      if (!Number.isInteger(normalizedLimit) || normalizedLimit <= 0) {
        throw new TypeError("limit must be a positive integer or null");
      }
      params.set("limit", String(normalizedLimit));
    }

    if (afterId === null) {
      const normalizedOffset = Number(offset);
      if (!Number.isInteger(normalizedOffset) || normalizedOffset < 0) {
        throw new TypeError("offset must be a non-negative integer");
      }
      params.set("offset", String(normalizedOffset));
    } else {
      params.set("after_id", String(Number(afterId)));
    }

    return this._request(
      "GET",
      `/collections/${escapePathSegment(collection)}/points?${params.toString()}`,
    );
  }

  async deletePoint(collection, pointId) {
    return this._request(
      "DELETE",
      `/collections/${escapePathSegment(collection)}/points/${escapePathSegment(pointId)}`,
    );
  }

  async deleteCollection(name) {
    return this._request("DELETE", `/collections/${escapePathSegment(name)}`);
  }

  async _request(method, path, options = {}) {
    const raw = Boolean(options.raw);
    const body = options.body;
    const headers = {
      Accept: raw ? "text/plain" : "application/json",
      ...this.defaultHeaders,
      ...(options.headers ?? {}),
    };

    if (this.apiKey) {
      headers["x-api-key"] = this.apiKey;
    }
    if (this.bearerToken) {
      headers.Authorization = `Bearer ${this.bearerToken}`;
    }

    let payload;
    if (body !== undefined) {
      payload = JSON.stringify(body);
      headers["Content-Type"] = "application/json";
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    let response;
    try {
      response = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: payload,
        signal: controller.signal,
      });
    } catch (error) {
      clearTimeout(timeout);
      throw new AionBDError(
        `request failed for ${method} ${path}: ${error.message}`,
        {
          method,
          path,
          cause: error,
        },
      );
    }
    clearTimeout(timeout);

    const text = await response.text();
    if (!response.ok) {
      throw new AionBDError(
        `HTTP ${response.status} on ${method} ${path}: ${text}`,
        {
          status: response.status,
          method,
          path,
          body: text,
        },
      );
    }

    if (raw) {
      return text;
    }

    if (text.length === 0) {
      return {};
    }

    try {
      return JSON.parse(text);
    } catch (error) {
      throw new AionBDError(
        `invalid JSON response for ${method} ${path}: ${error.message}`,
        { method, path, body: text, cause: error },
      );
    }
  }
}

export default AionBDClient;
