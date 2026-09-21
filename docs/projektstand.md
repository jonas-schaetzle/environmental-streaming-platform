# Projektstand

**Phase-0-Abschluss, 17.09.2026**

> Dieses Dokument ist ein historischer Snapshot zum Abschluss von Phase 0. Der
> aktuelle Repository-Stand kann in einzelnen Punkten bereits weiter sein.

Phase 0 ist abgeschlossen. Die Lern- und Sample-Pipelines wurden entfernt; das
Repository bildet ausschließlich den realen OpenAQ-Kafka-Spark-Pfad ab.

## Kennzahlen zum Abschluss

| Kennzahl | Wert |
|---|---:|
| Grüne Tests | 20 |
| Echte Messungen im letzten Lauf | 8 |
| Quarantine-Events im letzten Lauf | 0 |

Git-Stand des Snapshots: `dev` und `origin/dev` auf Commit `9009fd5`, Working
Tree sauber.

## Laufende Datenstrecke

```text
OpenAQ API
    |
    v
Python Producer
    |
    v
Kafka Canonical Topic
    |
    v
Spark Streaming
```

Der Producer ruft aktuelle OpenAQ-Messungen ab, reichert sie mit
Sensormetadaten an und veröffentlicht kanonische Events. Spark liest die Events
checkpoint-basiert, normalisiert Einheiten und trennt gültige von ungültigen
Datensätzen.

## Aktuelle Fähigkeiten

### Ingestion

- Ein oder mehrere OpenAQ-Standorte
- Einmaliger Lauf oder kontinuierliches Polling
- Cache für Sensormetadaten

### Zuverlässigkeit

- Persistente Duplikatunterdrückung
- Kafka-Zustellprüfung
- Spark-Checkpoints und Kafka-Offsets

### Datenqualität

- Kanonischer Messvertrag
- Normalisierte Feinstaub-Einheiten
- Quarantine mit Fehlergrund

### Entwicklungsbasis

- Lokaler Kafka-Broker mit Docker Compose
- Automatische Java-Erkennung für Spark
- Fokussierte Tests und Ruff-Linting

## Technischer Bestand

Der verbleibende Code ist bewusst klein und auf klar getrennte
Verantwortlichkeiten zugeschnitten.

| Modul | Verantwortung |
|---|---|
| `openaq_client.py` | HTTP-Zugriff, Endpunkte und API-Key-Handling |
| `openaq_kafka_producer.py` | Polling, Metadatenanreicherung und Deduplication |
| `kafka_publisher.py` | Serialisierung, Sensor-Key und Zustellprüfung |
| `measurement_model.py` | Schema, Validierung, Quarantine und Einheiten |
| `measurement_kafka_stream.py` | Kafka-Quelle, Spark-Orchestrierung und Parquet-Sinks |
| `kafka_measurement_consumer.py` | Betriebsdiagnostik ohne Offset-Commit |
| `spark_runtime.py` | Robuste lokale Java-Erkennung |

## Laufzeitpfade

| Element | Wert |
|---|---|
| Kafka Topic | `environment.measurements.canonical` |
| Gültige Daten | `data/lake/canonical_measurements` |
| Quarantine | `data/lake/quarantine_measurements` |
| Checkpoints | `data/checkpoints/` |
| Producer State | `data/state/openaq_kafka_producer.json` |

## Zum Abschluss von Phase 0 noch offen

- `main` und `origin/main` enthalten den Phase-0-Stand noch nicht.
- Eine CI-Pipeline auf GitHub ist noch nicht eingerichtet.
- Die Datenbasis umfasst bisher einen Standort und jeweils die neuesten Werte.
- Parquet ist ein Übergangsziel; Apache Iceberg ist als Lakehouse-Schicht
  vorgesehen.

**Phase 0 liefert einen stabilen technischen Kern. Phase 1 macht daraus ein
kontinuierliches, fachlich nutzbares Datenprodukt.**
