"""Independent count bag multiplicity and pre-HAVING failure controls."""
import unittest
from ashlar_host.count_star_oracle import evaluate_complete_bags
class CountStarOracleTests(unittest.TestCase):
    def test_parallel_paths_count_occurrences_not_terminal_destinations(self):
        rows,total=evaluate_complete_bags([('line',[('1','3'),('2','3')])],threshold=1)
        self.assertEqual(rows,[['line','2']]);self.assertEqual(total,2)
    def test_having_cannot_hide_complete_bag_overflow(self):
        with self.assertRaises(ValueError):evaluate_complete_bags([('line',[('1','3'),('2','3')])],threshold=99,maximum_occurrences=1)
    def test_having_cannot_hide_duplicate_occurrence_identity(self):
        with self.assertRaises(ValueError):evaluate_complete_bags([('line',[('1','3'),('1','3')])],threshold=99)
    def test_total_capacity_includes_all_groups_even_when_none_survive(self):
        with self.assertRaises(ValueError):evaluate_complete_bags([('a',[('1',)]),('b',[('2',)])],threshold=99,maximum_occurrences=1)
    def test_strict_greater_zero_groups_and_binary_order(self):
        self.assertEqual(evaluate_complete_bags([('z',[]),('b',[('1',)]),('a',[('2',),('3',)])],threshold=1),([['a','2']],3))
    def test_exact_original_source_positive_having_cases(self):
        import test_host_paths_query as fixture
        from ashlar_host.count_star_oracle import commerce_count_star_cases,original_commerce_count_star_oracle
        expected={'plain-count':[['1']],'grouped-count':[['[42,0,"P1"]','1']],'grouped-count-having':[['[42,0,"P1"]','1']],'expanded-count-having':[['[42,0,"L1"]','1']]}
        for name,sql in commerce_count_star_cases():
            self.assertEqual(original_commerce_count_star_oracle(fixture.MODEL,fixture.GRAPH,{'sql':sql})['rows'],expected[name])
if __name__=='__main__':unittest.main()
