import copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_graph_augmentations_graphframes import required_cases
SOURCE=Path(__file__).resolve().parents[1]/'examples/graph-augmentations/source.json'
class GraphAugmentationTests(unittest.TestCase):
    def fixture(self):return json.loads(SOURCE.read_bytes())
    def test_complete_prescribed_cases(self):required_cases(self.fixture())
    def test_semantically_valid_lookalike_does_not_satisfy_scalar_corpus(self):
        for mode in ('unicode','integer','decimal','boolean'):
            f=self.fixture();field={'unicode':'text','integer':'integer','decimal':'decimal','boolean':'boolean'}[mode]
            member=next(m for n in f['nodes']if n['case']=='A-SCALAR'for m in n['values']if m['field']['element']==field and m['state']=='present')
            member['value']={'unicode':{'string':'different'},'integer':{'integerToken':'1'},'decimal':{'decimalToken':'0.00'},'boolean':{'boolean':1}}[mode]
            with self.assertRaises(ValueError):required_cases(f)
    def test_parallel_edge_and_presence_null_absent_cannot_collapse(self):
        for mode in ('parallel','presence'):
            f=self.fixture()
            if mode=='parallel':f['edges'].pop(1)
            else:
                n=next(n for n in f['nodes']if n['key']=='presence-null');m=next(m for m in n['values']if m['field']['element']=='text');m['state']='absent';del m['value']
            with self.assertRaises(ValueError):required_cases(f)
if __name__=='__main__':unittest.main()
