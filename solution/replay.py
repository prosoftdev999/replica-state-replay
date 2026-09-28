#!/usr/bin/env python3
import csv
import itertools
import json
import math
import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/app/data"))
OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH", "/app/output/replay.json"))
TOKENS = ("tA", "tB", "tC", "tD")


def read_rows(name):
    with (DATA_DIR / name).open(newline="") as handle:
        return list(csv.DictReader(handle))


def load_clocks():
    grouped = {}
    for row in read_rows("clock_beacons.csv"):
        grouped.setdefault(row["replica"], []).append(
            (float(row["coordinator_us"]), float(row["replica_local_us"]))
        )
    for points in grouped.values():
        points.sort()
    return grouped


def coordinator_from_local(points, local_time):
    if local_time <= points[0][1]:
        left, right = points[0], points[1]
    elif local_time >= points[-1][1]:
        left, right = points[-2], points[-1]
    else:
        for left, right in zip(points, points[1:]):
            if left[1] <= local_time <= right[1]:
                break
    slope = (right[1] - left[1]) / (right[0] - left[0])
    return left[0] + (local_time - left[1]) / slope


def build_journal(name, value_key, scoped=True):
    if scoped:
        grouped = {}
        for row in read_rows(name):
            grouped.setdefault(row["replica"], []).append(
                (float(row["coord_effective_us"]), row[value_key])
            )
        for entries in grouped.values():
            entries.sort()
        return grouped
    entries = [
        (float(row["coord_effective_us"]), row[value_key])
        for row in read_rows(name)
    ]
    entries.sort()
    return entries


def active_value(history, timestamp):
    values = [value for effective, value in history if effective <= timestamp]
    if not values:
        raise RuntimeError("no active state")
    return values[-1]


def main():
    clocks = load_clocks()
    dispatcher_clock = sorted(
        (float(row["coordinator_us"]), float(row["dispatcher_local_us"]))
        for row in read_rows("dispatcher_beacons.csv")
    )
    cache_clock = sorted(
        (float(row["coordinator_us"]), float(row["cache_local_us"]))
        for row in read_rows("cache_beacons.csv")
    )
    stream_windows = read_rows("stream_windows.csv")
    channel_windows = read_rows("channel_windows.csv")
    lane_windows = read_rows("lane_windows.csv")
    worker_windows = read_rows("worker_windows.csv")
    cache_windows = read_rows("feature_cache_windows.csv")
    requests = {row["request_id"]: row for row in read_rows("requests.csv")}
    packets = {
        (row["epoch_code"], row["packet_slot"]): row
        for row in read_rows("feature_packets.csv")
    }
    schemas = {
        row["schema_rev"]: [row["f0"], row["f1"], row["f2"], row["f3"]]
        for row in read_rows("feature_schemas.csv")
    }
    models = {row["model_rev"]: row for row in read_rows("model_snapshots.csv")}
    normalizers = {row["norm_rev"]: row for row in read_rows("normalizers.csv")}
    model_journal = build_journal("deployment_journal.csv", "model_rev")
    norm_journal = build_journal("normalization_journal.csv", "norm_rev")
    residual_journal = build_journal("residual_journal.csv", "residual_rev")
    residual_params = {row["residual_rev"]: row for row in read_rows("residual_params.csv")}
    residual_seeds = {row["replica"]: float(row["z0"]) for row in read_rows("residual_seeds.csv")}
    calibration_journal = build_journal("calibration_journal.csv", "calibration_rev", scoped=False)
    calibration_params = {row["calibration_rev"]: row for row in read_rows("calibration_params.csv")}
    calibration_seeds = {row["lane_id"]: float(row["s0"]) for row in read_rows("calibration_seeds.csv")}
    pressure_journal = build_journal("pressure_journal.csv", "pressure_rev", scoped=False)
    pressure_params = {row["pressure_rev"]: row for row in read_rows("pressure_params.csv")}
    pressure_seeds = {row["pool_id"]: float(row["state0"]) for row in read_rows("pressure_seeds.csv")}
    pressure_events = []
    for row in read_rows("pressure_events.csv"):
        coord_time = coordinator_from_local(dispatcher_clock, float(row["dispatcher_local_us"]))
        pressure_events.append((coord_time, row))
    pressure_events.sort(key=lambda item: item[0])
    cache_events = []
    for row in read_rows("feature_cache_events.csv"):
        coord_time = coordinator_from_local(cache_clock, float(row["cache_local_us"]))
        cache_events.append((coord_time, row))
    cache_events.sort(key=lambda item: item[0])
    cache_seeds = {
        row["bank_id"]: {token: float(row[token]) for token in TOKENS}
        for row in read_rows("feature_cache_seeds.csv")
    }
    policies = sorted(
        (float(row["coord_effective_us"]), row)
        for row in read_rows("postprocess_journal.csv")
    )

    def request_context(request):
        local_time = float(request["replica_local_us"])
        candidates = []
        for window in stream_windows:
            if window["stream_handle"] != request["stream_handle"]:
                continue
            replica = window["replica"]
            coordinator_time = coordinator_from_local(clocks[replica], local_time)
            if not (float(window["coord_start_us"]) <= coordinator_time < float(window["coord_end_us"])):
                continue
            channel_match = any(
                channel["channel_token"] == request["channel_token"]
                and channel["replica"] == replica
                and float(channel["coord_start_us"]) <= coordinator_time < float(channel["coord_end_us"])
                for channel in channel_windows
            )
            if channel_match:
                candidates.append((replica, coordinator_time))
        if len(candidates) != 1:
            raise RuntimeError(f"ambiguous mapping for {request['request_id']}: {candidates}")

        replica, coordinator_time = candidates[0]
        model_rev = active_value(model_journal[replica], coordinator_time)
        norm_rev = active_value(norm_journal[replica], coordinator_time)
        residual_rev = active_value(residual_journal[replica], coordinator_time)
        calibration_rev = active_value(calibration_journal, coordinator_time)
        policy_rows = [row for effective, row in policies if effective <= coordinator_time]
        if not policy_rows:
            raise RuntimeError("no active policy")
        policy = policy_rows[-1]
        packet = packets[(request["epoch_code"], request["packet_slot"])]
        lane_matches = [
            row["lane_id"] for row in lane_windows
            if row["lane_key"] == packet["lane_key"]
            and float(row["coord_start_us"]) <= coordinator_time < float(row["coord_end_us"])
        ]
        if len(lane_matches) != 1:
            raise RuntimeError(f"ambiguous calibration lane for {request['request_id']}")
        pool_matches = [
            row["pool_id"] for row in worker_windows
            if row["worker_key"] == packet["worker_key"]
            and float(row["coord_start_us"]) <= coordinator_time < float(row["coord_end_us"])
        ]
        if len(pool_matches) != 1:
            raise RuntimeError(f"ambiguous pressure pool for {request['request_id']}")
        cache_matches = [
            row["bank_id"] for row in cache_windows
            if row["cache_key"] == packet["cache_key"]
            and float(row["coord_start_us"]) <= coordinator_time < float(row["coord_end_us"])
        ]
        if len(cache_matches) != 1:
            raise RuntimeError(f"ambiguous feature-cache bank for {request['request_id']}")
        return (replica, coordinator_time, model_rev, norm_rev, policy, residual_rev,
                calibration_rev, lane_matches[0], None, pool_matches[0], cache_matches[0], packet)

    base_contexts = {request_id: request_context(row) for request_id, row in requests.items()}

    # Feature packets are cache innovations, not complete feature vectors.  Rebuild
    # the cache chronology first, including asynchronous maintenance deltas recorded
    # on the cache recorder's independent clock.
    cache_states = {bank: dict(values) for bank, values in cache_seeds.items()}
    feature_values = {}
    cache_timeline = [(ctx[1], 1, "R", request_id) for request_id, ctx in base_contexts.items()]
    cache_timeline.extend((t, 0, "C", row) for t, row in cache_events)
    cache_timeline.sort(key=lambda item: (item[0], item[1]))
    for _, _, kind, payload in cache_timeline:
        if kind == "C":
            bank = payload["bank_id"]
            for token, field in zip(TOKENS, ("dA", "dB", "dC", "dD")):
                cache_states[bank][token] += float(payload[field])
            continue
        request_id = payload
        request = requests[request_id]
        context = base_contexts[request_id]
        bank = context[10]
        packet = context[11]
        slot_tokens = schemas[request["schema_rev"]]
        features = dict(cache_states[bank])
        for index, token in enumerate(slot_tokens):
            features[token] += float(packet[f"p{index}"])
        feature_values[request_id] = features
        cache_states[bank] = dict(features)

    contexts = {
        request_id: context[:8] + (feature_values[request_id],) + context[9:10]
        for request_id, context in base_contexts.items()
    }
    global_order = sorted((context[1], request_id) for request_id, context in contexts.items())
    global_timeline = [(t, 1, "R", request_id) for t, request_id in global_order]
    global_timeline.extend((t, 0, "E", row) for t, row in pressure_events)
    global_timeline.sort(key=lambda item: (item[0], item[1]))

    def evaluate(context, layouts, residual_state, calibration_state, pressure_state):
        replica, _, model_rev, norm_rev, policy, _, _, _, features, _ = context
        model = models[model_rev]
        normalizer = normalizers[norm_rev]
        model_layout = layouts[model["format_rev"]]
        norm_layout = layouts[normalizer["format_rev"]]
        policy_orientation = layouts[policy["format_rev"]]

        standardized = {}
        for index, token in enumerate(norm_layout):
            center = float(normalizer[f"c{2 * index}"])
            scale = float(normalizer[f"c{2 * index + 1}"])
            standardized[token] = (features[token] - center) / scale

        score = float(model["a4"])
        for index, token in enumerate(model_layout):
            score += float(model[f"a{index}"]) * standardized[token]
        score += residual_state + calibration_state + pressure_state

        k0, k1 = float(policy["k0"]), float(policy["k1"])
        temperature, threshold = (k0, k1) if policy_orientation == 0 else (k1, k0)
        probability = 1.0 / (1.0 + math.exp(-score / temperature))
        decision = 1 if probability >= threshold else 0
        return probability, decision, standardized

    samples = {
        row["request_id"]: (float(row["probability"]), int(row["decision"]))
        for row in read_rows("replay_samples.csv")
    }

    def decode_residual(row, layouts):
        spec = layouts[row["format_rev"]]
        values = [float(row[f"x{i}"]) for i in range(3)]
        return values[spec[0]], values[spec[1]], values[spec[2]] * spec[3], spec[4]

    def decode_calibration(row, layouts):
        spec = layouts[row["format_rev"]]
        values = [float(row[f"q{i}"]) for i in range(3)]
        return values[spec[0]], values[spec[1]], values[spec[2]], row["basis_token"]

    def decode_pressure(row):
        return float(row["decay"]), float(row["gain"])

    def replay(layouts, allow_unknown=False):
        outputs = {}
        residual_states = dict(residual_seeds)
        lane_states = dict(calibration_seeds)
        pressure_states = dict(pressure_seeds)
        for event_time, _, kind, payload in global_timeline:
            if kind == "E":
                pressure_rev = active_value(pressure_journal, event_time)
                pressure_row = pressure_params[pressure_rev]
                decay, gain = decode_pressure(pressure_row)
                pool_id = payload["pool_id"]
                pressure_states[pool_id] = (
                    decay * pressure_states[pool_id] + gain * float(payload["pulse"])
                )
                continue

            request_id = payload
            context = contexts[request_id]
            replica = context[0]
            lane_id = context[7]
            pool_id = context[9]
            model = models[context[2]]
            normalizer = normalizers[context[3]]
            policy = context[4]
            residual_row = residual_params[context[5]]
            calibration_row = calibration_params[context[6]]
            pressure_rev = active_value(pressure_journal, event_time)
            pressure_row = pressure_params[pressure_rev]
            required = (model["format_rev"], normalizer["format_rev"], policy["format_rev"],
                        residual_row["format_rev"], calibration_row["format_rev"])
            if any(key not in layouts for key in required):
                if allow_unknown:
                    break
                raise RuntimeError("incomplete layout assignment")
            probability, decision, standardized = evaluate(
                context, layouts, residual_states[replica], lane_states[lane_id], pressure_states[pool_id]
            )
            outputs[request_id] = (probability, decision)
            decay, gain, decision_push, prob_scale = decode_residual(residual_row, layouts)
            residual_states[replica] = (
                decay * residual_states[replica]
                + gain * (prob_scale * (probability - 0.5))
                + decision_push * (2 * decision - 1)
            )
            c_decay, c_gain, feature_gain, basis_token = decode_calibration(calibration_row, layouts)
            lane_states[lane_id] = (
                c_decay * lane_states[lane_id]
                + c_gain * (probability - 0.5)
                + feature_gain * standardized[basis_token]
            )
        return outputs

    def matching_samples(outputs):
        seen = 0
        for request_id, expected in samples.items():
            if request_id not in outputs:
                continue
            seen += 1
            probability, decision = outputs[request_id]
            if abs(probability - expected[0]) > 5e-7 or decision != expected[1]:
                return False, seen
        return True, seen

    model_specs = list(itertools.permutations(TOKENS))
    norm_specs = list(itertools.permutations(TOKENS))
    policy_specs = (0, 1)
    residual_specs = [
        (*perm, sign, scale)
        for perm in itertools.permutations(range(3))
        for sign in (1.0, -1.0)
        for scale in (1.0, 2.0)
    ]
    calibration_specs = list(itertools.permutations(range(3)))

    # Recover the first archive era jointly.  These five conventions are all
    # active before any later format appears, so the retained early outputs
    # constrain them together without assuming meanings from column position.
    first_era = []
    for model_spec in model_specs:
        for norm_spec in norm_specs:
            for policy_spec in policy_specs:
                for residual_spec in residual_specs:
                    for calibration_spec in calibration_specs:
                        candidate = {
                            "L1": model_spec, "N1": norm_spec, "P1": policy_spec,
                            "U1": residual_spec, "Q1": calibration_spec,
                        }
                        outputs = replay(candidate, allow_unknown=True)
                        viable, seen = matching_samples(outputs)
                        if viable and seen >= 5:
                            first_era.append(candidate)
    if len(first_era) != 1:
        raise RuntimeError(f"first archive era is not uniquely identified: {len(first_era)}")
    layouts = first_era[0]

    # Later format changes are recovered as a joint search.  Some transitions
    # are not identifiable at the instant they first appear, so keep every
    # surviving partial assignment until later retained outputs disambiguate it.
    stages = [
        (("U2", residual_specs), ("Q2", calibration_specs), 7),
        (("L2", model_specs), ("N2", norm_specs), 9),
        (("P2", policy_specs), 10),
        (("U3", residual_specs), ("Q3", calibration_specs), 12),
    ]
    candidates = [layouts]
    for stage in stages:
        *variables, minimum_seen = stage
        names = [name for name, _ in variables]
        spaces = [space for _, space in variables]
        survivors = []
        for base in candidates:
            for values in itertools.product(*spaces):
                candidate = dict(base)
                candidate.update(zip(names, values))
                outputs = replay(candidate, allow_unknown=True)
                viable, seen = matching_samples(outputs)
                if viable and seen >= minimum_seen:
                    survivors.append(candidate)
        if not survivors:
            raise RuntimeError(f"no viable assignments after archive formats {','.join(names)}")
        candidates = survivors
    if len(candidates) != 1:
        raise RuntimeError(f"archive conventions are not uniquely identified: {len(candidates)}")
    layouts = candidates[0]

    outputs = replay(layouts)
    if not all(
        abs(outputs[request_id][0] - expected[0]) <= 5e-7
        and outputs[request_id][1] == expected[1]
        for request_id, expected in samples.items()
    ):
        raise RuntimeError("recovered serving state does not reproduce retained samples")

    results = []
    for request_id in (DATA_DIR / "review_set.txt").read_text().split():
        context = contexts[request_id]
        replica, _, model_rev, norm_rev, policy, _, _, _, _, _ = context
        probability, decision = outputs[request_id]
        results.append({"request_id": request_id, "replica": replica, "model_rev": model_rev,
                        "norm_rev": norm_rev, "policy_rev": policy["policy_rev"],
                        "probability": round(probability, 8), "decision": decision})
    results.sort(key=lambda item: item["request_id"])
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w") as handle:
        json.dump({"results": results}, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
