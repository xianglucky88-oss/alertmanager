---
title: Alertmanager
sort_rank: 2
nav_icon: sliders
---

The [Alertmanager](https://github.com/prometheus/alertmanager) handles alerts
sent by client applications such as the Prometheus server.
It takes care of deduplicating, grouping, and routing
them to the correct receiver integration such as email, PagerDuty, or OpsGenie.
It also takes care of silencing and inhibition of alerts.

The following describes the core concepts the Alertmanager implements. Consult
the [configuration documentation](configuration.md) to learn how to use them
in more detail.

## Architecture

Alert generators send alerts to Alertmanager's API. Alertmanager stores active
alerts, groups and routes them according to its configuration, applies
inhibition and silences, and sends notifications through configured receivers.

In a high-availability deployment, every Alertmanager instance receives alerts
independently, while silences and notification state are replicated between
peers. See [High Availability](high_availability.md) for details.

## Grouping

Grouping categorizes alerts of similar nature into a single notification. This
is especially useful during larger outages when many systems fail at once and
hundreds to thousands of alerts may be firing simultaneously.

**Example:** Dozens or hundreds of instances of a service are running in your
cluster when a network partition occurs. Half of your service instances
can no longer reach the database.
Alerting rules in Prometheus were configured to send an alert for each service
instance if it cannot communicate with the database. As a result hundreds of
alerts are sent to Alertmanager.

As a user, one only wants to get a single page while still being able to see
exactly which service instances were affected. Thus one can configure
Alertmanager to group alerts by their cluster and alertname so it sends a
single compact notification.

Grouping of alerts, timing for the grouped notifications, and the receivers
of those notifications are configured by a routing tree in the configuration
file.

## Inhibition

Inhibition is a concept of suppressing notifications for certain alerts if
certain other alerts are already firing.

**Example:** An alert is firing that informs that an entire cluster is not
reachable. Alertmanager can be configured to mute all other alerts concerning
this cluster if that particular alert is firing.
This prevents notifications for hundreds or thousands of firing alerts that
are unrelated to the actual issue.

Inhibitions are configured through the Alertmanager's configuration file.

## Silences

Silences are a straightforward way to simply mute alerts for a given time.
A silence is configured based on matchers, just like the routing tree. Incoming
alerts are checked whether they match all the equality or regular expression
matchers of an active silence.
If they do, no notifications will be sent out for that alert.

Silences are configured in the web interface of the Alertmanager.


## Client behavior

The Alertmanager has [special requirements](alerts_api.md) for behavior of its
client. Those are only relevant for advanced use cases where Prometheus
is not used to send alerts.

## High Availability

Alertmanager supports configuration to create a cluster for high availability.
This can be configured using the [--cluster-*](https://github.com/prometheus/alertmanager#high-availability) flags.

It's important not to load balance traffic between Prometheus and its Alertmanagers, but instead, point Prometheus to a list of all Alertmanagers.

## Alert limits (optional)

Alertmanager supports configuration to limit the number of active alerts per alertname.
This can be configured using the [--alerts.per-alertname-limit] flag.

When the limit is reached, new alert instances are no longer dropped: they are retained in
an in-memory overflow queue instead, keeping the latest firing or resolved state of each
instance. Heartbeats from already known alerts continue to be processed, and repeated
reports of a queued instance update its state in place without creating duplicates.

As active alerts expire or are garbage collected, queued instances re-enter the regular
pipeline in stable first-in-first-out order, so they go through the same grouping,
silence/inhibition and notification stages as directly admitted alerts. An instance that
resolves while still queued is removed without being notified, since it was never admitted
while firing. The overflow state is not persisted: after a process restart alerts are
re-reported by their senders and admitted normally, while the notification log prevents
duplicate notifications.

This feature is useful when an unexpected high number of instances of the same alert are sent to Alertmanager.
Limiting the number of alerts per alertname can prevent reliability issues and avoid alert receivers from being flooded.

The `alertmanager_alerts_limited_total` metric shows the total number of alerts that were deferred to the overflow queue due to the per alert name limit.
The `alertmanager_alerts_overflow_pending` gauge shows the number of alerts currently held in the overflow queue.
Enabling the `alert-names-in-metrics` feature flag will add the `alertname` label to the `alertmanager_alerts_limited_total` metric.
