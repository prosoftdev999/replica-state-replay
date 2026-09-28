import json
import math
from pathlib import Path

ARTIFACT = Path('/app/output/replay.json')
EXPECTED = Path('/tests/expected.json')
TOL = 5e-7
REQ_KEYS = {'request_id','replica','model_rev','norm_rev','policy_rev','probability','decision'}

def test_replay():
    assert ARTIFACT.is_file(), 'missing /app/output/replay.json'
    with ARTIFACT.open() as f:
        got = json.load(f)
    with EXPECTED.open() as f:
        exp = json.load(f)
    assert isinstance(got, dict) and set(got) == {'results'}
    assert isinstance(got['results'], list)
    assert len(got['results']) == len(exp['results'])
    ids = [x.get('request_id') if isinstance(x,dict) else None for x in got['results']]
    assert ids == sorted(ids), 'results must be sorted by request_id'
    assert ids == [x['request_id'] for x in exp['results']]
    for g,e in zip(got['results'], exp['results']):
        assert isinstance(g, dict) and set(g) == REQ_KEYS
        for k in ('request_id','replica','model_rev','norm_rev','policy_rev'):
            assert g[k] == e[k]
        assert type(g['decision']) is int and g['decision'] in (0,1)
        assert g['decision'] == e['decision']
        assert type(g['probability']) in (int,float) and not isinstance(g['probability'], bool)
        assert math.isfinite(float(g['probability']))
        assert abs(float(g['probability']) - float(e['probability'])) <= TOL
