"""Trusted read-only script wrapper for an already admitted compiler statement."""
import json
from ashlar.publication import ResolutionError


def guarded_statement(sql, profile):
    from weft_native_profile import EXPECTED_ENGINE, EXPECTED_PROFILE
    if profile != EXPECTED_PROFILE:
        raise ResolutionError('Unadmitted same-execution native profile')
    if type(sql) is not str or not sql.strip():
        raise ResolutionError('Original compiled statement required')
    engine = json.dumps(EXPECTED_ENGINE, separators=(',', ':'))
    # Only host-owned constants are interpolated. Caller values retain markers.
    return """BEGIN
 DECLARE __ashlar_ansi_observed BOOLEAN DEFAULT false;
 DECLARE __ashlar_ansi_probe INT;
 BEGIN
  DECLARE EXIT HANDLER FOR SQLSTATE '22018' SET __ashlar_ansi_observed = true;
  SET __ashlar_ansi_probe = CAST('ashlar_ansi_probe' AS INT);
 END;
 IF NOT __ashlar_ansi_observed THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Ashlar requires ANSI exact-or-error';
 END IF;
 IF NOT (to_json(current_version(), map('ignoreNullFields','false')) <=> '""" + engine + """') THEN
  SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Ashlar native engine/build drift';
 END IF;
 """ + sql.strip().removesuffix(';') + ";\nEND"
