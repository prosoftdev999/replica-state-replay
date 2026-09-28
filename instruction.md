# Replica-state inference replay

A monitoring gap left a set of inference decisions without a trustworthy replay record. Reconstruct the decisions for the request IDs listed in `/app/data/review_set.txt` from the serving evidence under `/app/data/`.

Write `/app/output/replay.json` as a JSON object with one key, `results`. `results` must contain exactly one object for every request in the review set. Each object must contain:

- `request_id`: request ID string
- `replica`: serving replica ID
- `model_rev`: model revision ID active for that inference
- `norm_rev`: normalization revision ID active for that inference
- `policy_rev`: post-processing policy revision ID active for that inference
- `probability`: replayed positive-class probability
- `decision`: integer `0` or `1`

Report probabilities to at least 8 decimal places. A probability is accepted within an absolute tolerance of 0.0000005. Sort `results` by `request_id` ascending.

The records in `/app/data/serving_contract.txt` are the operational contract for replay semantics. Other files in `/app/data/` are contemporaneous service records. Treat half-open validity intervals and effective-time state changes exactly as documented there.

You have 9000 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
