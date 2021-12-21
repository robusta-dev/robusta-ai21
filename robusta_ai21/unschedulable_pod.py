import logging
from datetime import datetime, timedelta
from robusta.api import *


class UnschedulablePodParams(ActionParams):
    namespace: str = None
    grace_minutes: int = 30


def _ts_date(ts):
    if not ts:
        return None
    return datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ')


@action
def unschedulable_pod(event: EventEvent, config: UnschedulablePodParams):
    """
    playbooks configuration example:
    - triggers:
      - on_event_all_changes: {}
      actions:
      - unschedulable_pod:
          namespace: default
          grace_minutes: 20
    """
    event_obj = event.get_event()
    if event_obj is None:
        return

    pod = event_obj.involvedObject
    first_ts = _ts_date(event_obj.firstTimestamp)
    last_ts = _ts_date(event_obj.lastTimestamp)

    if event_obj.reason != 'FailedScheduling' or \
        pod is None or pod.kind != 'Pod' or \
        (config.namespace is not None and config.namespace != pod.namespace) or \
        pod.name.startswith('airflow-') or \
        first_ts is None or last_ts is None or \
        last_ts - first_ts < timedelta(minutes=config.grace_minutes):
        return

    finding = Finding(
        title=f"Unschedulable pod {pod.name} in namespace {pod.namespace}",
        source=FindingSource.KUBERNETES_API_SERVER,
        severity=FindingSeverity.HIGH,
        aggregation_key="unschedulable_pod",
    )

    enrichment_blocks = [MarkdownBlock(f'For the past {last_ts - first_ts} hours')]
    if event_obj.message:
        enrichment_blocks.append(MarkdownBlock(f'```{event_obj.message}```'))

    finding.add_enrichment(enrichment_blocks)
    event.add_finding(finding)
