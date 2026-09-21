# Arbeitsweise

**Aktueller Prozess, 18.09.2026**

Ab Phase 1 arbeiten wir mit einem schlanken, professionellen Lieferprozess.
Issues bleiben vorerst bewusst außen vor.

```text
Branch von dev -> kleine Commits -> Merge-Push nach dev -> CI + Self-Review
```

## Branch-Modell

| Branch | Verwendung | Beispiel |
|---|---|---|
| `main` | Veröffentlichter und stabiler Meilenstein | Release-PR aus `dev` |
| `dev` | Integrierter Stand der nächsten Version | Direktes Merge-Ziel |
| `feature/*` | Neue fachliche oder technische Fähigkeit | `feature/multi-location-ingestion` |
| `fix/*` | Gezielte Fehlerbehebung | `fix/openaq-retry-handling` |
| `refactor/*` | Strukturänderung ohne neue Funktion | `refactor/iceberg-sink` |
| `chore/*` | CI, Tooling und Wartung | `chore/add-ci` |

## Commit-Standard

Commits bleiben klein, fachlich geschlossen und erhalten einen kurzen,
imperativen Betreff ohne Typ-Präfix.

```text
Add multi-location ingestion
Handle transient OpenAQ failures
Add CI
```

## Merge- und Release-Standard

- Vor dem Merge werden der vollständige Diff und alle relevanten Checks geprüft.
- Fertige Arbeitsbranches werden lokal nach `dev` gemergt; anschließend wird
  `dev` direkt gepusht.
- Nur Releases von `dev` nach `main` laufen über einen Pull Request.
- Nach jedem Merge-Push werden Quelle und Ziel als
  `source-branch -> target-branch` gemeldet.
- Nach erfolgreichem Push wird der Arbeitsbranch lokal und remote gelöscht.

Beispiel für die Abschlussmeldung:

```text
chore/update-git-guidance -> dev
```

## Qualitätsgates

| Gate | Zweck |
|---|---|
| Ruff | Statische Prüfung |
| Pytest | Automatisierte Tests |
| Review | Vollständiger Diff |

## Definition of Done

Eine Änderung gilt erst als abgeschlossen, wenn Code, Datenverhalten und
Dokumentation zusammenpassen.

| Bereich | Erwartung |
|---|---|
| Scope | Änderung erfüllt ihr Ziel ohne fachfremde Nebenarbeiten. |
| Code | Bestehende Modulgrenzen und das kanonische Datenmodell bleiben konsistent. |
| Tests | Relevante Tests laufen; vor dem Merge sind Ruff und Pytest grün. |
| Daten | Idempotenz, Replay, Checkpoints und Quarantine werden berücksichtigt. |
| Security | Keine API-Keys, Runtime-Daten oder Secrets werden committed. |
| Dokumentation | README und Betriebsbefehle werden bei Verhaltensänderungen aktualisiert. |
| Git | Diff geprüft, Merge-Ziel benannt und Arbeitsbranch anschließend gelöscht. |

## Etablierter Lieferprozess

- GitHub Actions führt Ruff und Pytest auf `dev`, `main` und Release-PRs aus.
- Arbeitsbranches werden nach Self-Review direkt nach `dev` gemergt und gepusht.
- Release-PRs von `dev` nach `main` markieren abgeschlossene Meilensteine.
- `AGENTS.md` hält die verbindliche Arbeitsweise für Codex fest.

## Bewusst später

Issues, CODEOWNERS, aufwendige PR-Templates, Branch Protection und
Release-Automatisierung werden erst eingeführt, wenn Projektumfang oder
Zusammenarbeit ihren Aufwand rechtfertigen.

## Nächste Arbeitspakete

| Reihenfolge | Branch | Ergebnis |
|---:|---|---|
| 1 | `chore/add-codex-project-guidance` | Repositoryweite `AGENTS.md` |
| 2 | `chore/add-ci` | Automatische Ruff- und Pytest-Checks |
| 3 | `feature/multi-location-ingestion` | Kontinuierliche reale Datenbasis |
| 4 | `feature/event-time-aggregation` | Fachlich nutzbare Zeitfenster |
