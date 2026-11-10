---
title: Alerts API
sort_rank: 6
nav_icon: sliders
---

**Important**: Prometheus takes care of sending alerts to the Alertmanager.
It is recommended to configure alerting rules in Prometheus based on time
series data instead of sending alerts to the Alerts API, as Prometheus supports
a number of special cases to make sure alerts are delivered even if Alertmanager
crashes or restarts.

You send alerts to Alertmanager via APIv2. The APIv2 is specified as an
OpenAPI specification that can be found [here](https://github.com/prometheus/alertmanager/blob/master/api/v2/openapi.yaml).

APIv1 was deprecated in Alertmanager version 0.16.0 and removed in Alertmanager
version 0.27.0.

To send alerts to APIv2 make a POST request to `api/v2/alerts`. You must set
the `Content-Type` header to `application/json`, and send JSON data containing
an array of alerts.

Here is an example:

```json
[
  {
    "labels": {
      "alertname": "<required_value>",
      "<name>": "<value>",
      ...
    },
    "annotations": {
      "<name>": "<value>",
    },
    "startsAt": "<RFC3339>",
    "endsAt": "<RFC3339>",
    "generatorURL": "<value>"
  },
  ...
]
```

All alerts have labels, annotations, an optional `startsAt` timestamp and an
optional `endsAt` timestamp. All timestamps are expected in the RFC3339 format.

Labels are used to deduplicate identical instances of the same alert, while
annotations are used to include other information about the alert, such as a
summary, description or a URL to a runbook.

The `startsAt` timestamp is the time the alert fired. If omitted, Alertmanager
sets `startsAt` to the current time.

The `endsAt` timestamp is the time the alert should be resolved. If omitted,
Alertmanager sets `endsAt` to the current time + `resolve_timeout`.

The `generatorURL` is a unique URL which links to the source of the alert. For
example, it might link to the firing rule in Prometheus.

## Expectations from clients

Clients are expected to re-send firing alerts to the Alertmanager at regular
intervals until the alert is resolved.

The exact interval depends on a number of variables such as the `endsAt`
timestamp, or if omitted the value of `resolve_timeout`. If the `endsAt`
timestamp is omitted, the Alertmanager will update the existing `endsAt`
timestamp for the alert to the current time + `resolve_timeout`.

Firing alerts are resolved once their `endsAt` timestamp has elapsed.

To ensure resolved notifications are sent for resolved alerts, clients are also
expected to re-send resolved alerts to the Alertmanager for up to 5 minutes
after the alert has resolved. As the Alertmanager is stateless, this ensures
that a resolved notification is sent even if the Alertmanager crashes or is
restarted.

## Source-level batch idempotency

Senders that want to safely retry a batch of alerts after a timeout can attach
an optional source-level batch idempotency identifier to `POST api/v2/alerts`:

| Header | Description |
|---|---|
| `X-Alertmanager-Source-Id` | Stable identifier of the sending source. |
| `X-Alertmanager-Batch-Id` | Unique identifier of the batch within one source. |
| `X-Alertmanager-Batch-Sequence` | Non-negative integer that increases monotonically per source. |

The three headers are all-or-nothing: either send all three, or send none.
Requests without any of them keep the legacy receive semantics unchanged.

When the identifier is present, Alertmanager durably records every accepted
batch, including across process restarts:

- Re-submitting the same source and batch id returns `200` without driving
  grouping or notifications again, so timed-out requests can be retried
  safely.
- A batch whose sequence is not greater than the latest accepted sequence for
  the same source is rejected with `400` and a `stale batch` error, so older
  batches cannot overwrite newer state.
- A batch left unfinished by a previous process (for example because it
  crashed mid-ingestion) is rejected with `400` and an `incomplete batch
  state` error; resubmit it under a new batch id instead of replaying it.
- Requests missing part of the identifier, or carrying a non-numeric sequence,
  are rejected with `400` and an `incomplete batch identifier` error.
- Failures while persisting the batch state are rejected with `500` and an
  error message prefixed with `idempotency:`.

Records are kept under the configured storage path and garbage-collected
after `--data.retention`. Idempotency state is local to each Alertmanager
instance and is not replicated over the gossip cluster, so retries should be
directed to the same instance whenever possible.
