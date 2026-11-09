#!/usr/bin/env python3
"""stroohm-monta: fetch your own charging sessions for a Stroohm charge card from Monta's Public API.

Stroohm runs on Monta's platform, so this works for any Monta account, Stroohm or otherwise.
Standalone and standard-library only, so it can be copied out of this repo and used on its own:

    export MONTA_CLIENT_ID=... MONTA_CLIENT_SECRET=...
    python3 stroohm_monta.py                       # every completed session, as JSON
    python3 stroohm_monta.py --format csv > sessions.csv
    python3 stroohm_monta.py --from 2026-11-01     # sessions created on or after this day (UTC)

or as a library:

    import stroohm_monta
    for s in stroohm_monta.Monta(client_id, client_secret).sessions():
        print(s["started_at"], s["energy_kwh"], s["price_incl_vat"])

Credentials: in Monta Hub, Profile -> Applications -> "+ Add Application"
(https://hub.monta.app/profile/applications). The client secret is shown once. This is the
*Public* API (public-api.monta.com), meant for a user's own data; the operator-facing Partner API
(partner-api.monta.com) uses different credentials and rejects these. Docs:
https://docs.public-api.monta.com/

What a session record does and doesn't tell you
-----------------------------------------------
- `price` is what the session cost the user, incl. VAT: on a Belgian receipt, `price` was exactly
  the receipt's excl.-VAT total x 1.21. `price_excl_vat` divides by `vat_rate` (default 0.21, the
  BE and NL rate), so set it to the rate of the country you charge in. A company receipt with
  reverse-charged VAT ("BTW verlegd") shows the excl. amount as its total.
- `cost` is NOT what the user paid (it was lower than the receipt), so it is passed through in
  `raw` only.
- A session carries a numeric `chargePointId` but no address, name or operator. The charge point
  lookup endpoint only answers for charge points the user owns (403 otherwise), so naming
  third-party chargers is up to the caller: keep your own id -> place table.
- Times are UTC.

Rate limit: 10 requests per minute, shared by all applications of one user. The client spaces
its requests to stay under that and waits out a 429 if it gets one anyway.
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timezone

API = "https://public-api.monta.com/api/v1"
RATE_LIMIT = 10          # requests ...
RATE_WINDOW = 60.0       # ... per this many seconds
DEFAULT_VAT_RATE = 0.21

# Field order for CSV output (and the keys of every session dict).
FIELDS = [
    "id", "human_readable_id", "state", "charge_point_id", "started_at", "stopped_at",
    "energy_kwh", "price_incl_vat", "price_excl_vat", "vat_rate", "price_per_kwh_incl_vat",
    "currency",
]


class MontaError(Exception):
    def __init__(self, status, body):
        self.status, self.body = status, body
        detail = body.get("message") if isinstance(body, dict) else body
        super().__init__(f"Monta API returned {status}: {detail}")


class Monta:
    def __init__(self, client_id, client_secret, base_url=API, timeout=30,
                 rate_limit=RATE_LIMIT, rate_window=RATE_WINDOW, sleep=time.sleep):
        if not client_id or not client_secret:
            raise ValueError("client_id and client_secret are required")
        self._id, self._secret = client_id, client_secret
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._rate_limit, self._rate_window = rate_limit, rate_window
        self._sleep = sleep
        self._sent = deque()     # monotonic times of recent requests
        self._token = None

    # -- HTTP ---------------------------------------------------------------------------------

    def _throttle(self):
        while len(self._sent) >= self._rate_limit:
            wait = self._sent[0] + self._rate_window - time.monotonic()
            if wait <= 0:
                self._sent.popleft()
            else:
                self._sleep(wait + 0.5)
        self._sent.append(time.monotonic())

    def _request(self, method, path, body=None, auth=True, _retries=3):
        self._throttle()
        req = urllib.request.Request(
            self.base_url + path, method=method,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        if auth:
            req.add_header("Authorization", f"Bearer {self._access_token()}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                err = json.loads(raw)
            except ValueError:
                err = raw[:500].decode(errors="replace")
            if _retries and e.code == 429:
                self._sleep(self._rate_window + 1)
                return self._request(method, path, body, auth, _retries - 1)
            if _retries and auth and e.code == 401:
                self._token = None   # expired or revoked mid-run: log in again once
                return self._request(method, path, body, auth, 0)
            raise MontaError(e.code, err) from None

    def _access_token(self):
        if self._token is None:
            tok = self._request("POST", "/auth/token",
                                {"clientId": self._id, "clientSecret": self._secret}, auth=False)
            self._token = tok["accessToken"]
        return self._token

    # -- API ----------------------------------------------------------------------------------

    def charges(self, from_date=None, to_date=None, state="completed", per_page=100):
        """Yield raw charge objects, all pages. from_date/to_date filter on `createdAt` (datetime
        or ISO string); state=None returns every state, including sessions still running."""
        params = {"perPage": per_page}
        if state:
            params["state"] = state
        for key, value in (("fromDate", from_date), ("toDate", to_date)):
            if value:
                params[key] = _iso_z(value)
        page = 0
        while True:
            query = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in
                             {**params, "page": page}.items())
            res = self._request("GET", f"/charges?{query}")
            yield from res.get("data") or []
            total_pages = (res.get("meta") or {}).get("totalPageCount") or 0
            page += 1
            if page >= total_pages:
                return

    def sessions(self, vat_rate=DEFAULT_VAT_RATE, **kwargs):
        """Like charges(), but each one flattened by session() into plain fields."""
        for charge in self.charges(**kwargs):
            yield session(charge, vat_rate)


def _iso_z(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return value if "T" in value else f"{value}T00:00:00Z"


def _ts(value):
    """Monta timestamp ('2026-11-07T14:15:03Z', sometimes with fractions) -> aware datetime."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def session(charge, vat_rate=DEFAULT_VAT_RATE):
    """Flatten one raw charge into FIELDS (+ `raw`, the untouched object). Start is `startedAt`
    (falling back to plug-in, then creation), end is `stoppedAt` (falling back to the last
    update): those match the times on Monta's receipt."""
    started = _ts(charge.get("startedAt") or charge.get("cablePluggedInAt") or charge.get("createdAt"))
    stopped = _ts(charge.get("stoppedAt") or charge.get("updatedAt"))
    kwh = charge.get("consumedKwh")
    price = charge.get("price")
    return {
        "id": charge["id"],
        "human_readable_id": charge.get("humanReadableId"),
        "state": charge.get("state"),
        "charge_point_id": charge.get("chargePointId"),
        "started_at": started,
        "stopped_at": stopped,
        "energy_kwh": kwh,
        "price_incl_vat": price,
        "price_excl_vat": None if price is None else round(price / (1 + vat_rate), 2),
        "vat_rate": vat_rate,
        "price_per_kwh_incl_vat": round(price / kwh, 4) if price is not None and kwh else None,
        "currency": ((charge.get("currency") or {}).get("identifier") or "").upper() or None,
        "raw": charge,
    }


def main(argv=None):
    ap = argparse.ArgumentParser(description="Export your Stroohm/Monta charging sessions (Monta Public API).")
    ap.add_argument("--from", dest="from_date", help="created on/after, YYYY-MM-DD or ISO8601 UTC")
    ap.add_argument("--to", dest="to_date", help="created on/before, YYYY-MM-DD or ISO8601 UTC")
    ap.add_argument("--all-states", action="store_true",
                    help="include sessions that aren't completed yet (default: completed only)")
    ap.add_argument("--format", choices=("json", "csv"), default="json")
    ap.add_argument("--vat", type=float, default=DEFAULT_VAT_RATE,
                    help="VAT rate included in `price`, for price_excl_vat (default 0.21)")
    ap.add_argument("--raw", action="store_true", help="JSON only: include Monta's full object")
    args = ap.parse_args(argv)

    client_id, secret = os.environ.get("MONTA_CLIENT_ID"), os.environ.get("MONTA_CLIENT_SECRET")
    if not client_id or not secret:
        ap.error("set MONTA_CLIENT_ID and MONTA_CLIENT_SECRET in the environment")

    client = Monta(client_id, secret)
    rows = list(client.sessions(vat_rate=args.vat, from_date=args.from_date, to_date=args.to_date,
                                state=None if args.all_states else "completed"))
    rows.sort(key=lambda s: s["started_at"] or datetime.min.replace(tzinfo=timezone.utc))
    for s in rows:
        for k in ("started_at", "stopped_at"):
            s[k] = s[k].isoformat() if s[k] else None
        if not args.raw:
            s.pop("raw")

    if args.format == "csv":
        w = csv.DictWriter(sys.stdout, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    else:
        json.dump(rows, sys.stdout, indent=1)
        print()
    print(f"{len(rows)} session(s)", file=sys.stderr)


if __name__ == "__main__":
    main()
