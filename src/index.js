/**
 * index.js
 *
 * a request API compatible with window.fetch
 *
 * All spec algorithm step numbers are based on https://fetch.spec.whatwg.org/commit-snapshots/ae716822cb3a61843226cd090eefc6589446c1d2/.
 */
import http from "http";
import https from "https";
import Stream from "stream";
import Url from "url";
import zlib from "zlib";

import AbortError from "./abort-error";
import Body, { getTotalBytes, writeToStream } from "./body";
import FetchError from "./fetch-error";
import Headers, { createHeadersLenient } from "./headers";
import Request, { getNodeRequestOptions } from "./request";
import Response from "./response";

// fix an issue where "PassThrough", "resolve" aren't a named export for node <10
const PassThrough = Stream.PassThrough;
const resolve_url = Url.resolve;

/**
 * Fetch function
 *
 * @param   Mixed    url   Absolute url or Request instance
 * @param   Object   opts  Fetch options
 * @return  Promise
 */
export default function fetch(url, opts) {
  // allow custom promise
  if (!fetch.Promise) {
    throw new Error(
      "native promise missing, set fetch.Promise to your favorite alternative"
    );
  }

  Body.Promise = fetch.Promise;

  const NS_PER_SEC = 1e9;
  const MS_PER_NS = 1e6;

  function getDnsLookupTime(eventTimes) {
    return eventTimes.dnsLookupAt !== undefined
      ? getHrTimeDurationInMs(eventTimes.startAt, eventTimes.dnsLookupAt)
      : undefined;
  }

  function getTcpConnectionTime(eventTimes) {
    return getHrTimeDurationInMs(
      eventTimes.dnsLookupAt || eventTimes.startAt,
      eventTimes.tcpConnectionAt
    );
  }

  function getTlsHandshakeTime(eventTimes) {
    return eventTimes.tlsHandshakeAt !== undefined
      ? getHrTimeDurationInMs(
          eventTimes.tcpConnectionAt,
          eventTimes.tlsHandshakeAt
        )
      : undefined;
  }

  function getFirstByteTime(eventTimes) {
    return getHrTimeDurationInMs(
      eventTimes.tlsHandshakeAt || eventTimes.tcpConnectionAt,
      eventTimes.firstByteAt
    );
  }

  function getTotalTime(eventTimes) {
    return getHrTimeDurationInMs(eventTimes.startAt, eventTimes.endAt);
  }

  function getHrTimeDurationInMs(startTime, endTime) {
    if (!startTime || !endTime) {
      return undefined;
    }
    const secondDiff = endTime[0] - startTime[0];
    const nanoSecondDiff = endTime[1] - startTime[1];
    const diffInNanoSecond = secondDiff * NS_PER_SEC + nanoSecondDiff;

    return diffInNanoSecond / MS_PER_NS;
  }

  // wrap http.request into fetch
  return new fetch.Promise((resolvePromise, reject) => {
    // build request object
    const request = new Request(url, opts);
    const options = getNodeRequestOptions(request);

    let timings = {};
    const eventTimes = {
      startAt: process.hrtime()
    };

    const resolve = value => {
      eventTimes.endAt = process.hrtime();
      timings.totalTime = getTotalTime(eventTimes);
      resolvePromise(value);
    };

    const send = (options.protocol === "https:" ? https : http).request;
    const { signal } = request;
    let response = null;

    const abort = () => {
      let error = new AbortError("The user aborted a request.");
      error.timings = timings;
      reject(error);
      if (request.body && request.body instanceof Stream.Readable) {
        request.body.destroy(error);
      }
      if (!response || !response.body) return;
      response.body.emit("error", error);
    };

    if (signal && signal.aborted) {
      abort();
      return;
    }

    const abortAndFinalize = () => {
      abort();
      finalize();
    };

    // send request
    const req = send(options);
    let reqTimeout;

    if (signal) {
      signal.addEventListener("abort", abortAndFinalize);
    }

    function finalize() {
      req.abort();
      if (signal) signal.removeEventListener("abort", abortAndFinalize);
      clearTimeout(reqTimeout);
    }

    req.once("socket", socket => {
      socket.on("lookup", () => {
        eventTimes.dnsLookupAt = process.hrtime();
        timings.dnsLookupTime = getDnsLookupTime(eventTimes);
      });

      socket.on("connect", () => {
        eventTimes.tcpConnectionAt = process.hrtime();
        timings.tcpConnectionTime = getTcpConnectionTime(eventTimes);
      });

      socket.on("secureConnect", () => {
        eventTimes.tlsHandshakeAt = process.hrtime();
        timings.tlsHandshakeTime = getTlsHandshakeTime(eventTimes);
      });

      if (request.timeout) {
        reqTimeout = setTimeout(() => {
          const fe = new FetchError(
            `network timeout at: ${request.url}`,
            "request-timeout"
          );
          fe.timings = timings;
          reject(fe);
          finalize();
        }, request.timeout);
      }
    });

    req.on("error", err => {
      const fe = new FetchError(
        `request to ${request.url} failed, reason: ${err.message}`,
        "system",
        err
      );
      fe.timings = timings;
      reject(fe);
      finalize();
    });

    req.on("response", res => {
      clearTimeout(reqTimeout);
      const headers = createHeadersLenient(res.headers);

      // HTTP fetch step 5
      if (fetch.isRedirect(res.statusCode)) {
        // HTTP fetch step 5.2
        const location = headers.get("Location");

        // HTTP fetch step 5.3
        const locationURL =
          location === null ? null : resolve_url(request.url, location);

        // HTTP fetch step 5.5
        switch (request.redirect) {
          case "error":
            const fe = new FetchError(
              `redirect mode is set to error: ${request.url}`,
              "no-redirect"
            );
            fe.timings = timings;
            reject(fe);
            finalize();
            return;
          case "manual":
            // node-fetch-specific step: make manual redirect a bit easier to use by setting the Location header value to the resolved URL.
            if (locationURL !== null) {
              // handle corrupted header
              try {
                headers.set("Location", locationURL);
              } catch (err) {
                // istanbul ignore next: nodejs server prevent invalid response headers, we can't test this through normal request
                err.timings = timings;
                reject(err);
              }
            }
            break;
          case "follow":
            // HTTP-redirect fetch step 2
            if (locationURL === null) {
              break;
            }

            // HTTP-redirect fetch step 5
            if (request.counter >= request.follow) {
              const fe = new FetchError(
                `maximum redirect reached at: ${request.url}`,
                "max-redirect"
              );
              fe.timings = timings;
              reject(fe);
              finalize();
              return;
            }

            // HTTP-redirect fetch step 6 (counter increment)
            // Create a new Request object.
            const requestOpts = {
              headers: new Headers(request.headers),
              follow: request.follow,
              counter: request.counter + 1,
              agent: request.agent,
              compress: request.compress,
              method: request.method,
              body: request.body,
              signal: request.signal
            };

            const totalBytes = getTotalBytes(request);

            // HTTP-redirect fetch step 9
            if (res.statusCode !== 303 && request.body && totalBytes === null) {
              const fe = new FetchError(
                "Cannot follow redirect with body being a readable stream",
                "unsupported-redirect"
              );
              fe.timings = timings;
              reject(fe);
              finalize();
              return;
            }

            // HTTP-redirect fetch step 11
            if (
              res.statusCode === 303 ||
              ((res.statusCode === 301 || res.statusCode === 302) &&
                request.method === "POST")
            ) {
              requestOpts.method = "GET";
              requestOpts.body = undefined;
              requestOpts.headers.delete("content-length");
            }

            // HTTP-redirect fetch step 15
            resolve(fetch(new Request(locationURL, requestOpts)));
            finalize();
            return;
        }
      }

      // prepare response
      res.once("end", () => {
        if (signal) signal.removeEventListener("abort", abortAndFinalize);
      });

      const ip = req.connection.remoteAddress;
      let body = res.pipe(new PassThrough());

      const response_options = {
        url: request.url,
        status: res.statusCode,
        statusText: res.statusMessage,
        headers: headers,
        size: request.size,
        timeout: request.timeout,
        ip
      };

      // HTTP-network fetch step 12.1.1.3
      const codings = headers.get("Content-Encoding");

      res.once("readable", () => {
        eventTimes.firstByteAt = process.hrtime();
        timings.firstByteTime = getFirstByteTime(eventTimes);
      });

      // HTTP-network fetch step 12.1.1.4: handle content codings

      // in following scenarios we ignore compression support
      // 1. compression support is disabled
      // 2. HEAD request
      // 3. no Content-Encoding header
      // 4. no content response (204)
      // 5. content not modified response (304)
      if (
        !request.compress ||
        request.method === "HEAD" ||
        codings === null ||
        res.statusCode === 204 ||
        res.statusCode === 304
      ) {
        response = new Response(body, response_options, timings);
        resolve(response);
        return;
      }

      // For Node v6+
      // Be less strict when decoding compressed responses, since sometimes
      // servers send slightly invalid responses that are still accepted
      // by common browsers.
      // Always using Z_SYNC_FLUSH is what cURL does.
      const zlibOptions = {
        flush: zlib.Z_SYNC_FLUSH,
        finishFlush: zlib.Z_SYNC_FLUSH
      };

      // for gzip
      if (codings == "gzip" || codings == "x-gzip") {
        body = body.pipe(zlib.createGunzip(zlibOptions));
        response = new Response(body, response_options, timings);
        resolve(response);
        return;
      }

      // for deflate
      if (codings == "deflate" || codings == "x-deflate") {
        // handle the infamous raw deflate response from old servers
        // a hack for old IIS and Apache servers
        const raw = res.pipe(new PassThrough());
        raw.once("data", chunk => {
          // see http://stackoverflow.com/questions/37519828
          if ((chunk[0] & 0x0f) === 0x08) {
            body = body.pipe(zlib.createInflate());
          } else {
            body = body.pipe(zlib.createInflateRaw());
          }
          response = new Response(body, response_options, timings);
          resolve(response);
        });
        return;
      }

      // otherwise, use response as-is
      response = new Response(body, response_options, timings);
      resolve(response);
    });

    writeToStream(req, request);
  });
}

/**
 * Redirect code matching
 *
 * @param   Number   code  Status code
 * @return  Boolean
 */
fetch.isRedirect = code =>
  code === 301 || code === 302 || code === 303 || code === 307 || code === 308;

// expose Promise
fetch.Promise = global.Promise;
export { Headers, Request, Response, FetchError };
