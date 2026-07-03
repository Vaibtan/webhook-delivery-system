#!/usr/bin/env python3
"""Compute the X-Hub-Signature-256 header for the Go webhook delivery system.

The Go service signs HMAC-SHA256 over the canonical grammar (one construction in
both directions):

    timestamp + "." + idempotency_key + "." + body

- timestamp        : the Webhook-Timestamp header value (unix seconds).
- idempotency_key  : the optional X-Idempotency-Key (empty string when absent).
- body             : the exact request body bytes.

The header value is "sha256=<hex>". This differs from the old Python scheme
(which signed canonical JSON with no timestamp) — see the README migration note.

Example:
    python scripts/sign_webhook.py --secret "$SECRET" --body '{"event":"order.created"}'
    # prints the Webhook-Timestamp and X-Hub-Signature-256 headers to send.
"""
import argparse
import hashlib
import hmac
import time


def sign(secret: str, body: str, timestamp: str, idempotency_key: str = "") -> str:
    message = f"{timestamp}.{idempotency_key}.{body}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Sign a webhook payload for the Go delivery service.")
    parser.add_argument("--secret", required=True, help="the subscription's signing secret")
    parser.add_argument("--body", required=True, help="the raw JSON body to send")
    parser.add_argument("--timestamp", default=str(int(time.time())), help="unix seconds (default: now)")
    parser.add_argument("--idempotency-key", default="", help="optional X-Idempotency-Key value")
    args = parser.parse_args()

    signature = sign(args.secret, args.body, args.timestamp, args.idempotency_key)
    print(f"Webhook-Timestamp: {args.timestamp}")
    if args.idempotency_key:
        print(f"X-Idempotency-Key: {args.idempotency_key}")
    print(f"X-Hub-Signature-256: {signature}")


if __name__ == "__main__":
    main()
