// api/pluto.js — Pluto TV proxy for iptvdekh.com (Vercel serverless function).
//
// Why this exists: Pluto TV channels in the iptv-org playlist use
// https://jmp2.uk/plu-<channel-id>.m3u8 — a third-party CORS proxy that is
// DEAD (confirmed by curl timeouts, Nov 2026). Pluto's own stitcher
// (service-stitcher.clusters.pluto.tv) sends no CORS headers, so no
// browser can load its streams directly (verified: hls.js manifestLoadError,
// HTTP code 0). This function fetches the stitcher server-side (no CORS
// server-to-server), rewrites every nested URL in the playlists back
// through itself, and serves everything with CORS headers so hls.js
// in the browser can play it.
//
// Endpoints:
//   GET /api/pluto?id=<24-hex channel id>  — master playlist for a channel
//   GET /api/pluto?u=<encoded absolute URL> — proxied fetch of a nested
//       playlist, media segment, or decryption key referenced by a playlist
//
// SECURITY: the ?u= mode is NOT an open proxy. It only fetches hosts on
// the allowlist below (Pluto hosts). Anything else gets a 403. If Pluto
// moves segments to a new CDN host, playback will fail with a logged
// rejection — add the host to EXTRA_CDN_HOSTS after verifying it is
// really a Pluto CDN host, never a user-supplied one.
//
// Cost note: proxied video counts against Vercel bandwidth. Fine at
// modest usage; watch it if Pluto gets heavy use.
//
// Dependency-free: plain Node fetch, no npm packages, no build step.

const { Readable } = require("node:stream");

const STITCHER_BASE =
  "https://service-stitcher.clusters.pluto.tv/v1/stitch/embed/hls/channel";
const UA =
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36";
// Device params for the stitcher. Location is Salt Lake City — the user
// explicitly authorized sharing location with stream hosts for this work.
const DEVICE_PARAMS = {
  deviceType: "web",
  deviceMake: "Chrome",
  deviceModel: "web",
  deviceVersion: "1.0",
  appVersion: "1.0",
  deviceLat: "40.76",
  deviceLon: "-111.89",
  deviceDNT: "false",
  // Stable generated device ids (fixed so Pluto sees one consistent device).
  deviceId: "8d45cb32-7812-4482-9e71-e0c26b115d40",
  advertisingId: "9573b37f-789d-44a5-b10e-3e2658c3558d",
  appName: "web",
  serverSideAds: "false",
};
const CHANNEL_ID_RE = /^[0-9a-f]{24}$/i;
const FETCH_TIMEOUT_MS = 15000;
const MAX_PLAYLIST_BYTES = 2 * 1024 * 1024;

// Host allowlist for the ?u= fetch mode. Pluto serves its stitcher
// playlists and media from its own domains; anything else is rejected.
// plutotv.net is Pluto's segment/key CDN (verified live 2026-11-14:
// the stitcher playlist for 00s Replay points segments and AES keys at
// siloh-ns1.plutotv.net). EXTRA_CDN_HOSTS is for any further verified
// Pluto host a live run reveals via the rejection log below.
const EXTRA_CDN_HOSTS = [];
function isAllowedHost(hostname) {
  const h = (hostname || "").toLowerCase();
  if (h === "pluto.tv" || h.endsWith(".pluto.tv")) return true;
  if (h === "plutotv.net" || h.endsWith(".plutotv.net")) return true;
  return EXTRA_CDN_HOSTS.includes(h);
}

function proxiedUrl(absoluteUrl) {
  return `/api/pluto?u=${encodeURIComponent(absoluteUrl)}`;
}

// Rewrite every URL in an HLS playlist so it routes back through this
// proxy. Handles: bare URL lines (segments / variant playlists) and
// URI="..." attributes (#EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA,
// #EXT-X-SESSION-KEY, ...). Relative URLs resolve against baseUrl first.
// All other lines pass through byte-identical.
function rewritePlaylist(text, baseUrl) {
  const lines = text.split(/\r?\n/);
  const out = lines.map((line) => {
    if (!line) return line;
    if (line.startsWith("#")) {
      // Rewrite every URI="..." attribute on the tag line.
      return line.replace(/URI="([^"]+)"/g, (_m, uri) => {
        try {
          return `URI="${proxiedUrl(new URL(uri, baseUrl).href)}"`;
        } catch {
          return _m;
        }
      });
    }
    // Bare URL line: a segment or a variant playlist.
    try {
      return proxiedUrl(new URL(line, baseUrl).href);
    } catch {
      return line;
    }
  });
  return out.join("\n");
}

function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Range, Content-Type",
    "Access-Control-Expose-Headers":
      "Content-Length, Content-Range, Accept-Ranges",
  };
}

function sendError(res, status, message) {
  const body = JSON.stringify({ error: message });
  res.writeHead(status, {
    ...corsHeaders(),
    "Content-Type": "application/json",
    "Cache-Control": "no-store",
    "Content-Length": Buffer.byteLength(body),
  });
  res.end(body);
}

function isPlaylistUrl(url, contentType) {
  if ((contentType || "").includes("mpegurl")) return true;
  try {
    return new URL(url).pathname.toLowerCase().endsWith(".m3u8");
  } catch {
    return false;
  }
}

async function fetchUpstream(url, req) {
  const headers = { "User-Agent": UA, Accept: "*/*" };
  const range = req.headers && req.headers.range;
  if (range) headers.Range = range;
  return fetch(url, {
    headers,
    redirect: "follow",
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
}

// Stream an upstream response to the client, with CORS + cache headers.
// Playlists are re-read as text and rewritten; everything else streams.
async function proxyResponse(upstream, req, res, cacheControl) {
  const finalUrl = upstream.url;
  if (!isAllowedHost(new URL(finalUrl).hostname)) {
    console.warn(`pluto proxy: upstream redirected to disallowed host: ${finalUrl}`);
    return sendError(res, 403, "Upstream host not allowed");
  }
  const contentType = upstream.headers.get("content-type") || "";
  const headers = {
    ...corsHeaders(),
    "Cache-Control": cacheControl,
  };
  if (isPlaylistUrl(finalUrl, contentType)) {
    const buf = Buffer.from(await upstream.arrayBuffer());
    if (buf.length > MAX_PLAYLIST_BYTES) {
      return sendError(res, 502, "Playlist too large");
    }
    const rewritten = rewritePlaylist(buf.toString("utf-8"), finalUrl);
    headers["Content-Type"] = "application/vnd.apple.mpegurl";
    headers["Cache-Control"] = "public, max-age=5";
    res.writeHead(upstream.status, headers);
    res.end(rewritten);
    return;
  }
  // Binary (segment / key): stream it through.
  if (contentType) headers["Content-Type"] = contentType;
  for (const h of ["content-length", "content-range", "accept-ranges", "etag", "last-modified"]) {
    const v = upstream.headers.get(h);
    if (v) headers[h] = v;
  }
  res.writeHead(upstream.status, headers);
  if (upstream.body) {
    Readable.fromWeb(upstream.body).pipe(res);
  } else {
    res.end();
  }
}

async function handleMaster(req, res, channelId) {
  if (!CHANNEL_ID_RE.test(channelId || "")) {
    return sendError(res, 400, "Invalid Pluto channel id");
  }
  const params = new URLSearchParams(DEVICE_PARAMS);
  const stitcherUrl = `${STITCHER_BASE}/${channelId}/master.m3u8?${params}`;
  let upstream;
  try {
    upstream = await fetchUpstream(stitcherUrl, req);
  } catch (err) {
    console.warn(`pluto proxy: stitcher fetch failed for ${channelId}: ${err.message}`);
    return sendError(res, 502, "Could not reach Pluto TV");
  }
  if (!upstream.ok) {
    return sendError(res, upstream.status, `Pluto TV returned ${upstream.status}`);
  }
  return proxyResponse(upstream, req, res, "public, max-age=5");
}

async function handleFetch(req, res, rawUrl) {
  let target;
  try {
    target = new URL(rawUrl);
  } catch {
    return sendError(res, 400, "Invalid URL");
  }
  if (target.protocol !== "https:" && target.protocol !== "http:") {
    return sendError(res, 400, "Only http(s) URLs are allowed");
  }
  if (!isAllowedHost(target.hostname)) {
    console.warn(`pluto proxy: rejected disallowed host: ${target.hostname}`);
    return sendError(res, 403, "Host not allowed");
  }
  let upstream;
  try {
    upstream = await fetchUpstream(target.href, req);
  } catch (err) {
    console.warn(`pluto proxy: fetch failed for ${target.href}: ${err.message}`);
    return sendError(res, 502, "Could not fetch upstream URL");
  }
  if (!upstream.ok && upstream.status !== 206) {
    return sendError(res, upstream.status, `Upstream returned ${upstream.status}`);
  }
  const cache = isPlaylistUrl(target.href, upstream.headers.get("content-type"))
    ? "public, max-age=5"
    : "public, max-age=3600";
  return proxyResponse(upstream, req, res, cache);
}

async function handler(req, res) {
  // CORS preflight (hls.js sends Range headers, which trigger one).
  if (req.method === "OPTIONS") {
    res.writeHead(204, { ...corsHeaders(), "Cache-Control": "public, max-age=86400" });
    res.end();
    return;
  }
  if (req.method !== "GET" && req.method !== "HEAD") {
    return sendError(res, 405, "Method not allowed");
  }
  let query = {};
  try {
    const host = (req.headers && req.headers.host) || "localhost";
    query = Object.fromEntries(new URL(req.url, `https://${host}`).searchParams);
  } catch {
    return sendError(res, 400, "Bad request URL");
  }
  try {
    if (query.id) return await handleMaster(req, res, query.id);
    if (query.u) return await handleFetch(req, res, query.u);
    return sendError(res, 400, "Provide ?id=<Pluto channel id> or ?u=<encoded URL>");
  } catch (err) {
    console.warn(`pluto proxy: unexpected error: ${err && err.message}`);
    if (!res.headersSent) return sendError(res, 500, "Proxy error");
    try { res.end(); } catch { /* already closed */ }
  }
}

module.exports = handler;
// Exported for unit tests (Vercel only uses the default handler export).
module.exports.rewritePlaylist = rewritePlaylist;
module.exports.isAllowedHost = isAllowedHost;
module.exports.proxiedUrl = proxiedUrl;
