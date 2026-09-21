import runpy
import sys
from pathlib import Path

from src.anomaly_detector import AnomalyDetector
from src.aiops_pipeline import load_data, run_pipeline
from src.event_consumer import EventConsumer
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_normal_record_is_not_anomaly():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 120,
        "cpu_percent": 42,
        "memory_percent": 51,
        "log_level": "INFO",
        "message": "Payment request processed successfully"
    }

    assert detector.detect(record) is None


def test_anomalous_record_is_detected():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:05:00",
        "service": "payment-service",
        "response_time_ms": 610,
        "cpu_percent": 75,
        "memory_percent": 70,
        "log_level": "ERROR",
        "message": "Payment service timeout"
    }

    event = detector.detect(record)

    assert event is not None
    assert event["type"] == "ANOMALY"


def test_producer_publishes_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    assert producer.publish(event)
    assert len(topic.get_messages()) == 1


def test_consumer_receives_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)
    consumer = EventConsumer(topic)

    event = {
        "type": "ANOMALY",
        "service": "payment-service"
    }

    producer.publish(event)

    messages = consumer.consume()

    assert len(messages) == 1


def test_load_data_reads_service_data_file():
    repo_root = Path(__file__).resolve().parents[1]

    data = load_data(repo_root / "data" / "service_data.json")

    assert len(data) == 10
    assert data[0]["service"] == "payment-service"


def test_pipeline_main_entrypoint_runs_and_prints_summary(capsys):
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "src" / "aiops_pipeline.py"

    runpy.run_path(str(script_path), run_name="__main__")

    captured = capsys.readouterr()

    assert "AIOps Pipeline Result" in captured.out
    assert "Records processed: 10" in captured.out
    assert "Anomalies detected: 2" in captured.out
    assert "Events consumed: 0" in captured.out


def test_warning_log_is_reported_as_anomaly_reason():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:10:00",
        "service": "checkout-service",
        "response_time_ms": 120,
        "cpu_percent": 30,
        "memory_percent": 40,
        "log_level": "WARNING",
        "message": "Warning: degraded latency"
    }

    event = detector.detect(record)

    assert event is not None
    assert "Error log detected" in event["reasons"]


def test_producer_rejects_empty_event():
    topic = EventTopic("anomaly-events")
    producer = EventProducer(topic)

    assert producer.publish({}) is False
    assert topic.get_messages() == []


def test_event_topic_clear_removes_messages():
    topic = EventTopic("anomaly-events")
    topic.publish({"type": "ANOMALY", "service": "payment-service"})

    topic.clear()

    assert topic.get_messages() == []


def test_run_pipeline_processes_service_data():
    result = run_pipeline("data/service_data.json")

    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) == 2
    assert result["anomalies_detected"][0]["type"] == "ANOMALY"
    assert result["events_consumed"] == []