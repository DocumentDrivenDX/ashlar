import unittest
from types import SimpleNamespace
from weft_path_capture import PathCaptureConfig, PathCaptureError, capture_string_frame


class Frame:
    def __init__(self, rows, names=('value',), kind='string', nullable=False):
        self.closed = False
        self.schema = SimpleNamespace(fields=[SimpleNamespace(
            name=name, nullable=nullable,
            dataType=SimpleNamespace(simpleString=lambda: kind)) for name in names])
        self.rows = rows

    def toLocalIterator(self, *, prefetchPartitions):
        assert prefetchPartitions is False
        def iterate():
            try:
                yield from self.rows
            finally:
                self.closed = True
        return iterate()


class Capture(unittest.TestCase):
    def test_exact_positions_duplicates_and_null(self):
        frame = Frame([['é', None]], names=('x', 'x'), nullable=True)
        self.assertEqual(capture_string_frame(frame, config=PathCaptureConfig(1, 2, 2)),
                         {'schema': [['x', 'STRING'], ['x', 'STRING']],
                          'native_schema': [{'name': 'x', 'native_type': 'STRING', 'nullable': True}]*2,
                          'rows': [['é', None]]})
        self.assertTrue(frame.closed)

    def test_limits_refuse_entire_result_and_close(self):
        for rows, config in [([['x'], ['y']], PathCaptureConfig(1, 1, 2)),
                             ([['é']], PathCaptureConfig(1, 1, 1)),
                             ([['xx'], ['xx']], PathCaptureConfig(2, 2, 3))]:
            frame = Frame(rows)
            with self.assertRaises(PathCaptureError):
                capture_string_frame(frame, config=config)
            self.assertTrue(frame.closed)

    def test_native_shape_and_empty_schema(self):
        config = PathCaptureConfig(2, 8, 16)
        for rows in [[[None]], [[1]], [['x', 'y']], [['\ud800']]]:
            frame = Frame(rows)
            with self.assertRaises(PathCaptureError):
                capture_string_frame(frame, config=config)
            self.assertTrue(frame.closed)
        with self.assertRaises(PathCaptureError):
            capture_string_frame(Frame([], kind='bigint'), config=config)
        self.assertEqual(capture_string_frame(Frame([]), config=config)['rows'], [])

    def test_configuration_has_no_defaults_or_boolean_limits(self):
        for values in [(True, 1, 1), (1, 0, 1), (1, 2, 1)]:
            with self.assertRaises(PathCaptureError):
                PathCaptureConfig(*values)

    def test_cleanup_failure_preserves_safe_primary_refusal(self):
        class Iterator:
            def __iter__(self): return self
            def __next__(self): raise RuntimeError('private native payload')
            def close(self): raise RuntimeError('private cleanup payload')
        frame = Frame([])
        frame.toLocalIterator = lambda **_: Iterator()
        with self.assertRaises(PathCaptureError) as observed:
            capture_string_frame(frame, config=PathCaptureConfig(1, 1, 1))
        self.assertEqual(str(observed.exception), 'Native result capture failed')
        self.assertTrue(observed.exception.cleanup_failed)

    def test_cleanup_failure_after_complete_capture_refuses_result(self):
        class Iterator:
            def __iter__(self): return iter([['x']])
            def close(self): raise RuntimeError('private cleanup payload')
        frame = Frame([])
        frame.toLocalIterator = lambda **_: Iterator()
        with self.assertRaises(PathCaptureError) as observed:
            capture_string_frame(frame, config=PathCaptureConfig(1, 1, 1))
        self.assertEqual(str(observed.exception), 'Result iterator cleanup failed')
        self.assertTrue(observed.exception.cleanup_failed)

    def test_native_acquisition_failure_is_safe(self):
        class BrokenSchema:
            @property
            def schema(self): raise RuntimeError('private schema payload')
        frame = Frame([])
        def broken(**_): raise RuntimeError('private iterator payload')
        frame.toLocalIterator = broken
        for candidate in (BrokenSchema(), frame):
            with self.assertRaises(PathCaptureError) as observed:
                capture_string_frame(candidate, config=PathCaptureConfig(1, 1, 1))
            self.assertEqual(str(observed.exception), 'Native result capture failed')

    def test_empty_result_retains_native_nullable_metadata(self):
        result = capture_string_frame(Frame([], nullable=True), config=PathCaptureConfig(1, 1, 1))
        self.assertEqual(result['native_schema'], [{'name': 'value', 'native_type': 'STRING', 'nullable': True}])


if __name__ == '__main__':
    unittest.main()
