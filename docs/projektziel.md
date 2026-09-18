# Projektziel

**Zielbild zum Phase-0-Abschluss, 17.09.2026**

Eine belastbare Environmental Streaming Data Platform, die reale Umweltdaten
kontinuierlich verarbeitet und daraus nachvollziehbare, aktuelle Informationen
für Analyse und Warnung erzeugt.

> Reale Quellen -> robuste Events -> verlässliche Datenprodukte -> sichtbarer
> Nutzen

## Fachliches Zielbild

Das primäre Datenprodukt ist ein Live Air Quality Monitor für mehrere Städte und
Messstationen. Er zeigt aktuelle Belastung, zeitliche Entwicklung, Datenfrische
und auffällige Veränderungen.

### Beobachten

- Aktuelle Schadstoffwerte je Standort
- Zeitreihen und gleitende Mittelwerte
- Datenfrische und Stationsstatus

### Verstehen

- Vergleich von Städten und Zeiträumen
- Einfluss von Wetter und Tageszeit
- Unterscheidung von Ereignis und Sensorfehler

### Reagieren

- Grenzwert- und Anomalie-Alerts
- Nachvollziehbare Quarantine-Fälle
- Replay und historische Neubewertung

## Zielarchitektur

```text
OpenAQ + Wetter
       |
       v
Kafka Events
       |
       v
Spark / Flink Processing
       |
       v
Iceberg Lakehouse
```

Spark bleibt die zentrale Verarbeitungsschicht für Validierung,
Event-Time-Aggregationen und Lakehouse-Schreibvorgänge. Flink wird erst ergänzt,
wenn ein klarer Low-Latency-Alerting-Use-Case einen zweiten Streaming-Runner
fachlich rechtfertigt.

## Portfolio-Signal

- Streaming Engineering mit Kafka, Spark, Checkpoints und Event Time
- Data Quality, Quarantine, Idempotenz und Replay-Fähigkeit
- Lakehouse-Modellierung mit Iceberg und reproduzierbaren Tabellen
- Observability, Infrastructure as Code und Cloud Deployment
- Ein fachlich sichtbares Produkt statt einer Sammlung isolierter Technik-Demos

## Roadmap zum Zielbild

Die nächsten Phasen liefern jeweils ein eigenständig sichtbares Ergebnis.

| Phase | Schwerpunkt |
|---|---|
| Phase 1 | Mehrere Standorte |
| Phase 2 | Event Time und Iceberg |
| Phase 3 | Insights und Alerts |
| Phase 4 | Cloud und Observability |

### Phase 1 - Reale Datenbasis

- Kuratierten Satz realer Standorte festlegen und konfigurieren.
- OpenAQ dauerhaft pollen und Fehler mit Retry/Backoff behandeln.
- Datenfrische, Event-Anzahl und Abdeckung sichtbar machen.

### Phase 2 - Streaming Analytics und Lakehouse

- Event-Time-Fenster, Watermarks und verspätete Events umsetzen.
- Parquet-Sinks durch Apache-Iceberg-Tabellen ersetzen.
- Replay, Backfill und Tabellenwartung dokumentieren und testen.

### Phase 3 - Fachliches Datenprodukt

- Wetterdaten anbinden und räumlich/zeitlich verknüpfen.
- Städtevergleich, Zeitreihen und Anomalien bereitstellen.
- Gezielte Alerts für Belastung und fehlende Daten erzeugen.

### Phase 4 - Betrieb und Cloud

- Metriken, Logs, Kafka-Lag und Datenfrische überwachen.
- Infrastruktur mit Terraform aufbauen und reproduzierbar deployen.
- CI/CD, Security Checks und dokumentierte Betriebsabläufe ergänzen.

## Erfolgskriterien

| Dimension | Nachweis |
|---|---|
| Fachlich | Mehrere reale Standorte, verständliche Trends und Alerts |
| Technisch | Restart-fähige, idempotente und beobachtbare Datenstrecke |
| Datenqualität | Explizite Regeln, Quarantine und nachvollziehbare Fehler |
| Betrieb | Automatisierte Checks, Deployment und Monitoring |
| Portfolio | Klare Architekturentscheidungen und reproduzierbare Demo |
