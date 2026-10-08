BEGIN;
SET LOCAL ROLE ashlar_pin_writer;
SELECT ashlar_pins.register('fixture-pin','private-development','recovery','fixture-request','client_dev.ashlar_recoverable_graph_20261008.object_current','e420f55d-449f-4c21-b91b-460ffc6493e2',4,decode(repeat('ab',32),'hex'));
SELECT ashlar_pins.register('fixture-pin','private-development','recovery','fixture-request','client_dev.ashlar_recoverable_graph_20261008.object_current','e420f55d-449f-4c21-b91b-460ffc6493e2',4,decode(repeat('ab',32),'hex'));
COMMIT;
DO $$ BEGIN
  SET LOCAL ROLE ashlar_pin_writer;
  BEGIN
    DELETE FROM ashlar_pins.pin; RAISE EXCEPTION 'Direct delete unexpectedly allowed';
  EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'PASS writer direct delete refused'; END;
  BEGIN
    PERFORM ashlar_pins.register('fixture-pin','private-development','recovery','fixture-request','client_dev.ashlar_recoverable_graph_20261008.object_current','e420f55d-449f-4c21-b91b-460ffc6493e2',5,decode(repeat('ab',32),'hex'));
    RAISE EXCEPTION 'Changed pin unexpectedly allowed';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'Immutable pin conflict or released original' THEN RAISE; END IF;
    RAISE NOTICE 'PASS changed version refused';
  END;
END $$;
DO $$ BEGIN
  SET LOCAL ROLE ashlar_pin_reader;
  IF (SELECT count(*) FROM ashlar_pins.pin WHERE NOT released) <> 1 THEN RAISE EXCEPTION 'Replay duplicated or released original'; END IF;
  BEGIN
    PERFORM ashlar_pins.release('fixture-pin','private-development',decode(repeat('ab',32),'hex'));
    RAISE EXCEPTION 'Reader release unexpectedly allowed';
  EXCEPTION WHEN insufficient_privilege THEN RAISE NOTICE 'PASS reader release refused'; END;
END $$;
DO $$ BEGIN
  SET LOCAL ROLE ashlar_pin_maintenance;
  BEGIN
    PERFORM ashlar_pins.assert_unpinned('client_dev.ashlar_recoverable_graph_20261008.object_current','e420f55d-449f-4c21-b91b-460ffc6493e2',4);
    RAISE EXCEPTION 'Active pin unexpectedly admitted retention';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'Active pin refuses retention' THEN RAISE; END IF;
    RAISE NOTICE 'PASS active pin retention refused';
  END;
END $$;
BEGIN;
SET LOCAL ROLE ashlar_pin_writer;
SELECT ashlar_pins.release('fixture-pin','private-development',decode(repeat('ab',32),'hex'));
ROLLBACK;
SELECT pin_id,version,released FROM ashlar_pins.pin;
