; benchmark generated from python API
(set-info :status unknown)
(declare-fun next_copy_bytes () Int)
(declare-fun remaining_copy_budget () Int)
(declare-fun copied_payload_bytes () Int)
(assert
 (let (($x67 (or (> (+ copied_payload_bytes next_copy_bytes) 8388608) (< (- remaining_copy_budget next_copy_bytes) 0))))
(let (($x60 (= (+ copied_payload_bytes remaining_copy_budget) 8388608)))
(let (($x57 (>= remaining_copy_budget 0)))
(let (($x56 (>= next_copy_bytes 0)))
(let (($x55 (>= copied_payload_bytes 0)))
(let (($x62 (and $x55 $x56 $x57 $x60 (<= next_copy_bytes remaining_copy_budget))))
(and $x62 $x67))))))))
(check-sat)
