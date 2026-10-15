from payment_service.webhooks.signing import build_signature_header, verify_signature_header

SECRET = "secret-secret-secret"
BODY = b'{"id":"1"}'
NOW = 1_700_000_000


def test_roundtrip() -> None:
    header = build_signature_header(SECRET, NOW, BODY)
    assert verify_signature_header(SECRET, header, BODY, now=NOW + 10)


def test_tampered_body_is_rejected() -> None:
    header = build_signature_header(SECRET, NOW, BODY)
    assert not verify_signature_header(SECRET, header, b'{"id":"2"}', now=NOW)


def test_wrong_secret_is_rejected() -> None:
    header = build_signature_header(SECRET, NOW, BODY)
    assert not verify_signature_header("another-secret-value", header, BODY, now=NOW)


def test_stale_timestamp_is_rejected() -> None:
    header = build_signature_header(SECRET, NOW, BODY)
    assert not verify_signature_header(SECRET, header, BODY, now=NOW + 301)


def test_malformed_header_is_rejected() -> None:
    for header in ("", "garbage", "t=abc,v1=00", "v1=00"):
        assert not verify_signature_header(SECRET, header, BODY, now=NOW)
