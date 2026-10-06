
SET threads=4; SET memory_limit='2GB';
CREATE TABLE objects AS SELECT t AS type_id, i AS id,
 json_object('101', 'g'||cast(i%100 as varchar), '102', i%1000, '103', repeat('x',512))::VARCHAR AS props,
 '{"future":{"preserve":true}}' AS retained
FROM range(1,4) tt(t) CROSS JOIN range(1,10001) ii(i) ORDER BY type_id,id;
CREATE TABLE edges AS SELECT rel AS rel_type_id, (rel-1)*40000+i*4+k AS id,
 rel AS source_type, i AS source_id, rel+1 AS target_type,
 CASE WHEN i%100=0 THEN 1 ELSE ((i+k-1)%10000)+1 END AS target_id,
 json_object('201', k/10.0)::VARCHAR AS props
FROM range(1,3) rr(rel) CROSS JOIN range(1,10001) ii(i) CROSS JOIN range(0,4) kk(k)
ORDER BY rel_type_id,source_id,id;
-- Controlled hub: n additional AB edges from A1, with stable edge identities.
INSERT INTO edges SELECT 1, 200000+i, 1,1,2,i,'{"201":0.9}' FROM range(1,10001) ii(i);
CREATE TABLE promoted_objects AS SELECT *, json_extract_string(props,'$.101') AS group_value,
 cast(json_extract_string(props,'$.102') AS BIGINT) AS rank_value FROM objects ORDER BY type_id,id;
CREATE TABLE promoted_edges AS SELECT *,cast(json_extract_string(props,'$.201') AS DOUBLE) AS score FROM edges ORDER BY rel_type_id,source_id,id;
CREATE TABLE node_a AS SELECT * FROM promoted_objects WHERE type_id=1 ORDER BY id;
CREATE TABLE node_b AS SELECT * FROM promoted_objects WHERE type_id=2 ORDER BY id;
CREATE TABLE node_c AS SELECT * FROM promoted_objects WHERE type_id=3 ORDER BY id;
CREATE TABLE edge_ab AS SELECT * FROM promoted_edges WHERE rel_type_id=1 ORDER BY source_id,id;
CREATE TABLE edge_bc AS SELECT * FROM promoted_edges WHERE rel_type_id=2 ORDER BY source_id,id;
CREATE TABLE edge_ab_reverse AS SELECT * FROM edge_ab ORDER BY target_id,id;

-- ('bag', 'lookup')
SELECT id,props,retained FROM objects WHERE type_id=1 AND id=123;
-- ('bag', 'list')
SELECT id FROM objects WHERE type_id=1 AND json_extract_string(props,'$.101')='g42' ORDER BY id LIMIT 100;
-- ('bag', 'count')
SELECT json_extract_string(props,'$.101') AS grp,count(*) AS n FROM objects WHERE type_id=1 GROUP BY grp ORDER BY grp;
-- ('bag', 'one_hop')
SELECT id,target_id FROM edges WHERE rel_type_id=1 AND source_type=1 AND source_id=123 ORDER BY id;
-- ('bag', 'two_hop')
SELECT e.id AS first_edge,f.id AS second_edge,f.target_id FROM edges e JOIN edges f ON e.target_id=f.source_id AND e.target_type=f.source_type JOIN objects b ON b.id=e.target_id AND b.type_id=e.target_type WHERE e.rel_type_id=1 AND e.source_id=123 AND e.source_type=1 AND f.rel_type_id=2 AND cast(json_extract_string(e.props,'$.201') AS DOUBLE)>=0.1 AND cast(json_extract_string(f.props,'$.201') AS DOUBLE)>=0.1 AND cast(json_extract_string(b.props,'$.102') AS BIGINT)>100 ORDER BY first_edge,second_edge;
-- ('bag', 'reverse')
SELECT id,source_id FROM edges WHERE rel_type_id=1 AND target_type=2 AND target_id=1 ORDER BY id;
-- ('bag', 'hub_bounded')
SELECT id,target_id FROM edges WHERE rel_type_id=1 AND source_id=1 ORDER BY id LIMIT 100;
-- ('bag', 'hub_count')
SELECT count(*) AS n FROM edges e JOIN edges f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.rel_type_id=1 AND e.source_id=1 AND e.source_type=1 AND f.rel_type_id=2;
-- ('promoted', 'lookup')
SELECT id,props,retained FROM promoted_objects WHERE type_id=1 AND id=123;
-- ('promoted', 'list')
SELECT id FROM promoted_objects WHERE type_id=1 AND group_value='g42' ORDER BY id LIMIT 100;
-- ('promoted', 'count')
SELECT group_value AS grp,count(*) AS n FROM promoted_objects WHERE type_id=1 GROUP BY grp ORDER BY grp;
-- ('promoted', 'one_hop')
SELECT id,target_id FROM promoted_edges WHERE rel_type_id=1 AND source_type=1 AND source_id=123 ORDER BY id;
-- ('promoted', 'two_hop')
SELECT e.id AS first_edge,f.id AS second_edge,f.target_id FROM promoted_edges e JOIN promoted_edges f ON e.target_id=f.source_id AND e.target_type=f.source_type JOIN promoted_objects b ON b.id=e.target_id AND b.type_id=e.target_type WHERE e.rel_type_id=1 AND e.source_id=123 AND e.source_type=1 AND f.rel_type_id=2 AND e.score>=0.1 AND f.score>=0.1 AND b.rank_value>100 ORDER BY first_edge,second_edge;
-- ('promoted', 'reverse')
SELECT id,source_id FROM promoted_edges WHERE rel_type_id=1 AND target_type=2 AND target_id=1 ORDER BY id;
-- ('promoted', 'hub_bounded')
SELECT id,target_id FROM promoted_edges WHERE rel_type_id=1 AND source_id=1 ORDER BY id LIMIT 100;
-- ('promoted', 'hub_count')
SELECT count(*) AS n FROM promoted_edges e JOIN promoted_edges f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.rel_type_id=1 AND e.source_id=1 AND e.source_type=1 AND f.rel_type_id=2;
-- ('typed', 'lookup')
SELECT id,props,retained FROM node_a WHERE type_id=1 AND id=123;
-- ('typed', 'list')
SELECT id FROM node_a WHERE type_id=1 AND group_value='g42' ORDER BY id LIMIT 100;
-- ('typed', 'count')
SELECT group_value AS grp,count(*) AS n FROM node_a WHERE type_id=1 GROUP BY grp ORDER BY grp;
-- ('typed', 'one_hop')
SELECT id,target_id FROM edge_ab WHERE rel_type_id=1 AND source_type=1 AND source_id=123 ORDER BY id;
-- ('typed', 'two_hop')
SELECT e.id AS first_edge,f.id AS second_edge,f.target_id FROM edge_ab e JOIN edge_bc f ON e.target_id=f.source_id AND e.target_type=f.source_type JOIN node_b b ON b.id=e.target_id AND b.type_id=e.target_type WHERE e.rel_type_id=1 AND e.source_id=123 AND e.source_type=1 AND f.rel_type_id=2 AND e.score>=0.1 AND f.score>=0.1 AND b.rank_value>100 ORDER BY first_edge,second_edge;
-- ('typed', 'reverse')
SELECT id,source_id FROM edge_ab WHERE rel_type_id=1 AND target_type=2 AND target_id=1 ORDER BY id;
-- ('typed', 'hub_bounded')
SELECT id,target_id FROM edge_ab WHERE rel_type_id=1 AND source_id=1 ORDER BY id LIMIT 100;
-- ('typed', 'hub_count')
SELECT count(*) AS n FROM edge_ab e JOIN edge_bc f ON e.target_id=f.source_id AND e.target_type=f.source_type WHERE e.rel_type_id=1 AND e.source_id=1 AND e.source_type=1 AND f.rel_type_id=2;
-- ('typed_reverse', 'reverse')
SELECT id,source_id FROM edge_ab_reverse WHERE rel_type_id=1 AND target_type=2 AND target_id=1 ORDER BY id;