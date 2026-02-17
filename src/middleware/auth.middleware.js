import crypto from "crypto";

function timingSafeEqual(a, b) {
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function isEnabled(flag) {
  return ["1", "true", "yes", "on"].includes((flag ?? "").toLowerCase());
}

export function apiKeyGate(req, res, next) {
  const required = isEnabled(process.env.AUTH_REQUIRED); // defaults to false if not set, so auth is opt-in
  const provided = (req.header("x-api-key") ?? "").trim();

  // Allow rotation: API_KEYS=key1,key2
  const allowed = (process.env.API_KEYS ?? "")
    .split(",")
    .map(s => s.trim())
    .filter(Boolean);

  if (allowed.length === 0) {
    if (!required) {
      res.setHeader("x-auth-warning", "auth-not-configured");
      return next();
    }
    return res.status(500).json({ error: "Auth not configured" });
  }

  const ok = provided && allowed.some(k => timingSafeEqual(provided, k));

  if (!ok) {
    if (!required) {
      res.setHeader("x-auth-warning", "missing-or-invalid-x-api-key");
      return next();
    }
    return res.status(401).json({ error: "Unauthorized" });
  }

  res.setHeader("x-auth-status", "ok");
  return next();
}
