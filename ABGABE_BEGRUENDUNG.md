# Begruendung Kafka-Konfiguration und Idempotenz

## Topics

- `raw-transactions`: 1 Partition, Replication Factor 1
- `valid-transactions`: 3 Partitionen, Replication Factor 1
- `fraud-alerts`: 1 Partition, Replication Factor 1

Der Replication Factor ist 1, weil das lokale Docker-Compose-Setup nur einen Kafka-Broker startet. Ein hoeherer Replication Factor waere erst mit mehreren Brokern moeglich.

## Partitionen

Fuer `raw-transactions` wird eine Partition verwendet, weil aktuell genau eine Instanz des Fraud-Alert-Service konsumiert. Die implementierten Fraud-Strategien pruefen jede Transaktion unabhaengig, sodass fuer diesen Schritt keine parallele oder kontobezogen geordnete Verarbeitung erforderlich ist.

Fuer `fraud-alerts` wird ebenfalls eine Partition verwendet, weil aktuell nur eine Notification-Service-Instanz die unabhaengig voneinander verarbeiteten Alerts protokolliert. Bei hoeherem Nachrichtenaufkommen kann die Partitionsanzahl erhoeht und der Notification-Service horizontal skaliert werden.

Fuer `valid-transactions` werden 3 Partitionen verwendet. Das passt zum Transfer-Service, der in Docker Compose dreimal gestartet wird und das Topic innerhalb einer gemeinsamen Consumer Group parallel verarbeitet.

## Consumer Groups

Der Fraud-Alert-Service nutzt eine eigene Consumer Group `fraud-check-group`. Dadurch wird jede rohe Transaktion genau von einer Fraud-Instanz innerhalb dieser Group verarbeitet.

Der Notification-Service nutzt `notification-group`, weil Benachrichtigungen unabhaengig vom Transfer verarbeitet werden sollen.

Alle drei Transfer-Service-Instanzen nutzen dieselbe Consumer Group `transfer-service-group`. Dadurch verteilt Kafka die Partitionen auf die Instanzen und jede valide Transaktion wird nur von einer Transfer-Instanz verarbeitet. Unterschiedliche Groups waeren hier falsch, weil dann jede Instanz dieselbe Nachricht erhalten und die Ueberweisung mehrfach ausfuehren wuerde.

## Reihenfolge

Der Fraud-Service schreibt valide Transaktionen mit `fromAccount` als Kafka-Key in das Topic `valid-transactions`. Kafka legt Nachrichten mit gleichem Key in dieselbe Partition. Damit bleibt die Reihenfolge pro Quellkonto innerhalb dieser Partition erhalten.

## Idempotenz

Der Transfer-Service prueft vor der Verarbeitung, ob die `transactionId` bereits in der Transaktionstabelle existiert. Ist sie vorhanden, wird die Nachricht uebersprungen. Zusaetzlich ist `transaction_id` in der Datenbank eindeutig. Damit schuetzt die Datenbank gegen doppelte Persistierung derselben Transaktion, auch wenn Kafka eine Nachricht erneut zustellt.
