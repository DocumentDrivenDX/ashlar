-- Proposed ashlar-publication-receipt/0.1; separate from ashlar-delta/0.3.
-- Not executed, not an in-place migration, and not an authority/fencing proof.
-- Append one immutable intent and one immutable complete record per batch.
-- Publisher validates phase, exact payload/digest, membership and uniqueness.
CREATE TABLE publication_receipt (
  stream STRING NOT NULL,
  apply_batch_id STRING NOT NULL,
  phase STRING NOT NULL,
  profile_version STRING NOT NULL,
  receipt_json STRING NOT NULL,
  receipt_digest STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL
) USING DELTA;
-- Logical key: (stream, apply_batch_id, phase); no native unique enforcement.
-- phase is intent or complete; profile_version is ashlar-publication-receipt/0.1.
-- receipt_digest is lower-case SHA256 of exact receipt_json UTF-8 bytes.
-- All native source IDs/cursor/generation integers inside receipt_json retain
-- their qualified textual representation; no implicit numeric narrowing.
-- recorded_at is ingestion metadata, excluded from payload replay identity.
-- No TTL, VACUUM, automatic writer takeover or catalog-managed atomicity implied.
