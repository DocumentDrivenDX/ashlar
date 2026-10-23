INSERT INTO `spark_catalog`.`default`.`edge_current` SELECT source_system, rel_type_id, id, source_type,
          source_id, target_type, target_id, schema_revision, entity_version, props_json,
          retained_json, order_key, source_feed, source_epoch, source_position, published_at,
          xxhash64(CAST(source_system AS STRING), CAST(rel_type_id AS BIGINT), CAST(id AS BIGINT)),
          apply_batch_id, source_cursor_json, source_delivery_id
        FROM (SELECT 'layout-probe' AS source_system, CAST(1 AS BIGINT) AS rel_type_id, id,
          CAST(1 AS BIGINT) AS source_type, CAST(floor(id/5) AS BIGINT) AS source_id,
          CAST(1 AS BIGINT) AS target_type,
          pmod(CAST(floor(id/5) AS BIGINT) + CAST(floor(pmod(id,5)/2) AS BIGINT), 45000000) AS target_id,
          'layout-probe-r1' AS schema_revision, CAST(1 AS BIGINT) AS entity_version,
          '{}' AS props_json, '{}' AS retained_json, CAST(NULL AS STRING) AS order_key,
          'direct-layout-fixture' AS source_feed, '1' AS source_epoch, id AS source_position,
          TIMESTAMP '2026-10-15T00:00:00.000Z' AS published_at, 'layout-probe' AS apply_batch_id,
          '{}' AS source_cursor_json, CAST(id AS STRING) AS source_delivery_id
          FROM range(225000000)) fixture