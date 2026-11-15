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
//       (optional &fp=roku|samsung|web selects the device fingerprint
//       sent to the v1 stitcher; default roku — see FINGERPRINTS below)
//   GET /api/pluto?id=<id>&v1=1 — the OLD v1 embed path (slated as of
//       Nov 2026, kept as a fallback). Without &v1=1 the proxy uses
//       Pluto's v2 API: anonymous boot session + v2 stitcher with JWT.
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
// Device fingerprints for the stitcher (query params on the master URL).
// Pluto slates the generic web profile (verified live 2026-11-14: every
// channel returns the same "service ended" takedown-slate clip) — since
// Feb 2026 Pluto requires an account for its regular web/app API, and
// the unauthenticated web embed profile gets the slate. The Roku Channel
// partner profile was verified serving real content 2026-08-16
// (Free-TV/IPTV#1077) and is the default. ?fp=<name> on the ?id= request
// selects a preset (unknown names fall back to the default).
// Location in the web preset is Salt Lake City — the user explicitly
// authorized sharing location with stream hosts for this work.
const FINGERPRINTS = {
  roku: {
    advertisingId: "channel",
    appName: "rokuchannel",
    appVersion: "1.0",
    bmodel: "bm1",
    channel_id: "channel",
    content: "channel",
    content_rating: "ROKU_ADS_CONTENT_RATING",
    content_type: "livefeed",
    coppa: "false",
    deviceDNT: "1",
    deviceId: "channel",
    deviceMake: "rokuChannel",
    deviceModel: "web",
    deviceType: "rokuChannel",
    deviceVersion: "1.0",
    embedPartner: "rokuChannel",
    genre: "ROKU_ADS_CONTENT_GENRE",
    is_lat: "1",
    platform: "web",
    rdid: "channel",
    studio_id: "viacom",
    tags: "ROKU_CONTENT_TAGS",
  },
  samsung: {
    advertisingId: "{PSID}",
    appVersion: "unknown",
    deviceDNT: "{TARGETOPT}",
    deviceId: "{PSID}",
    deviceLat: "0",
    deviceLon: "0",
    deviceMake: "samsung",
    deviceModel: "samsung",
    deviceType: "samsung-tvplus",
    deviceVersion: "unknown",
    embedPartner: "samsung-tvplus",
  },
  web: {
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
  },
};
const DEFAULT_FINGERPRINT = "roku";
function buildStitcherUrl(channelId, fpName) {
  const params = new URLSearchParams(
    FINGERPRINTS[fpName] || FINGERPRINTS[DEFAULT_FINGERPRINT]
  );
  return `${STITCHER_BASE}/${channelId}/master.m3u8?${params}`;
}

// Pluto v2 API (the DEFAULT path — verified live 2026-11-14 serving
// real content anonymously, e.g. "Along Came a Spider" on 00s Replay,
// while the v1 embed path slates every profile). Step 1: boot session
// at boot.pluto.tv/v4/start — no account, so no username/password.
// The response carries sessionToken + stitcherParams. Step 2: the v2
// stitcher master playlist with jwt=<sessionToken>. Tokens last ~4h;
// the session is cached in memory and refreshed 30 min early, and a
// 401 from the stitcher triggers one fresh-boot retry.
const BOOT_URL = "https://boot.pluto.tv/v4/start";
const V2_STITCHER_BASE =
  "https://cfd-v4-service-channel-stitcher-use1-1.prd.pluto.tv/v2/stitch/hls/channel";
const V2_BOOT_PARAMS = {
  appName: "web",
  appVersion: "8.0.0-111b2b9dc00bd0bea9030b30662159ed9e7c8bc6",
  deviceVersion: "122.0.0",
  deviceModel: "web",
  deviceMake: "chrome",
  deviceType: "web",
  // Stable generated client id (one consistent virtual device).
  clientID: "add63b26-3628-4929-b6e5-1785248975e5",
  clientModelNumber: "1.0.0",
  serverSideAds: "false",
  drmCapabilities: "",
};
// V2 session cache (module-level: survives warm invocations; cold
// starts re-boot). Refresh 30 min before the ~4h token expiry.
let v2Session = null; // { token, stitcherParams, expiresAt }
const V2_SESSION_TTL_MS = 3.5 * 60 * 60 * 1000;

async function bootV2Session() {
  const bootUrl = `${BOOT_URL}?${new URLSearchParams(V2_BOOT_PARAMS)}`;
  const r = await fetch(bootUrl, {
    headers: {
      "User-Agent": UA,
      Accept: "application/json",
      Origin: "https://pluto.tv",
      Referer: "https://pluto.tv/",
    },
    redirect: "follow",
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });
  if (!r.ok) throw new Error(`boot returned ${r.status}`);
  const boot = await r.json();
  if (!boot || !boot.sessionToken || !boot.stitcherParams) {
    throw new Error("boot response missing sessionToken/stitcherParams");
  }
  return {
    token: boot.sessionToken,
    stitcherParams: boot.stitcherParams,
    expiresAt: Date.now() + V2_SESSION_TTL_MS,
  };
}

async function getV2Session() {
  if (v2Session && v2Session.expiresAt > Date.now()) return v2Session;
  v2Session = await bootV2Session();
  return v2Session;
}

function buildV2StitcherUrl(channelId, session) {
  const params =
    typeof session.stitcherParams === "string"
      ? session.stitcherParams
      : new URLSearchParams(session.stitcherParams).toString();
  return (
    `${V2_STITCHER_BASE}/${channelId}/master.m3u8?${params}` +
    `&jwt=${encodeURIComponent(session.token)}&masterJWTPassthrough=true&includeExtendedEvents=true`
  );
}
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

async function handleMaster(req, res, channelId, fpName) {
  if (!CHANNEL_ID_RE.test(channelId || "")) {
    return sendError(res, 400, "Invalid Pluto channel id");
  }
  const stitcherUrl = buildStitcherUrl(channelId, fpName);
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

async function handleMasterV2(req, res, channelId) {
  if (!CHANNEL_ID_RE.test(channelId || "")) {
    return sendError(res, 400, "Invalid Pluto channel id");
  }
  let session;
  try {
    session = await getV2Session();
  } catch (err) {
    console.warn(`pluto proxy: v2 boot failed: ${err.message}`);
    return sendError(res, 502, "Could not reach Pluto TV");
  }
  let upstream;
  try {
    upstream = await fetchUpstream(buildV2StitcherUrl(channelId, session), req);
  } catch (err) {
    console.warn(`pluto proxy: v2 stitcher fetch failed for ${channelId}: ${err.message}`);
    return sendError(res, 502, "Could not reach Pluto TV");
  }
  // Token expired mid-flight: clear the cache, re-boot once, retry.
  if (upstream.status === 401) {
    console.warn("pluto proxy: v2 stitcher 401, refreshing session and retrying");
    v2Session = null;
    try {
      session = await getV2Session();
      upstream = await fetchUpstream(buildV2StitcherUrl(channelId, session), req);
    } catch (err) {
      console.warn(`pluto proxy: v2 stitcher retry failed for ${channelId}: ${err.message}`);
      return sendError(res, 502, "Could not reach Pluto TV");
    }
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
    if (query.id) {
      // v2 is the default (v1 slates every profile); &v1=1 keeps the
      // old embed path available as a fallback.
      if (query.v1 === "1") {
        return await handleMaster(req, res, query.id, query.fp);
      }
      return await handleMasterV2(req, res, query.id);
    }
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
module.exports.buildStitcherUrl = buildStitcherUrl;
module.exports.FINGERPRINTS = FINGERPRINTS;
// Test-only: clear the cached v2 session between tests.
module.exports._resetV2Session = () => { v2Session = null; };
