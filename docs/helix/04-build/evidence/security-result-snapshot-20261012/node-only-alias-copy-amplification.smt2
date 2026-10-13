; benchmark generated from python API
(set-info :status unknown)
(declare-fun copied_payload_bytes () Int)
(declare-fun next_copy_bytes () Int)
(assert
 (let (($x70 (= next_copy_bytes 1048576)))
(and $x70 (= copied_payload_bytes (* 32 next_copy_bytes)) (> copied_payload_bytes 8388608))))
(check-sat)
