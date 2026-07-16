/**
 * Steuert die komplette interaktive Lernansicht.
 * Die Inhalte sind bewusst als Daten modelliert: Eine neue Aufgabe kann später
 * ergänzt werden, ohne die HTML-Struktur umbauen zu müssen.
 */
class LernflussApp {
    constructor() {
        this.currentTask = 0;
        this.currentCode = 0;
        this.completed = new Set(JSON.parse(localStorage.getItem("kafka-lernfluss-progress") || "[]"));
        this.tasks = this.createTasks();
        this.glossaryTerms = this.createGlossary();
        this.cacheElements();
        this.renderNavigation();
        this.renderFlow();
        this.renderGlossaryItems();
        this.bindEvents();
        this.selectTask(0);
    }

    cacheElements() {
        const ids = ["task-navigation", "progress-label", "progress-bar", "flow-diagram", "detail-number",
            "detail-kicker", "detail-title", "detail-summary", "detail-body", "detail-memory", "code-tabs",
            "file-label", "code-content", "code-note", "previous-button", "next-button", "understood-button",
            "copy-button", "tour-button", "reset-button", "glossary", "glossary-search", "toast"];
        ids.forEach(id => this[id.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase())] = document.getElementById(id));
    }

    createTasks() {
        return [
            {
                number: 1,
                short: "Topics & REST",
                kicker: "Eingang in das System",
                title: "Transaktion annehmen und an Kafka senden",
                summary: "Der Transaction-Service prüft den POST-Request, ergänzt fehlende technische Werte und veröffentlicht nur gültige Transaktionen in raw-transactions.",
                body: `
                    <h3>Was kommt hinein?</h3><p>Ein JSON-Objekt mit Quellkonto, Zielkonto, Betrag und Währung. Transaktions-ID und Zeitstempel dürfen fehlen.</p>
                    <h3>Was passiert?</h3><p>Der <span class="term" data-term="REST">REST-Endpunkt</span> ruft den ValidationService auf. Ungültige Daten ergeben HTTP 400. Fehlende ID und Zeit werden erzeugt. Danach sendet ein <span class="term" data-term="Producer">Producer</span> die Nachricht an Kafka.</p>
                    <h3>Warum eine Partition?</h3><p>Aktuell konsumiert genau eine Fraud-Service-Instanz aus raw-transactions. Die Fraud-Regeln prüfen jede Transaktion unabhängig, daher ist für diesen Schritt keine parallele Verarbeitung notwendig. Der <span class="term" data-term="Replication Factor">Replication Factor</span> ist 1, weil lokal nur ein Broker läuft.</p>`,
                memory: "Validieren → ID und Zeit ergänzen → ohne Kafka-Key nach raw-transactions senden.",
                nodes: ["transaction", "raw"],
                codes: [
                    { label: "REST", file: "transaction-service/.../TransactionResource.java", note: "Die gültige Transaktion wird ohne Kafka-Key in das Topic geschrieben.", code: `@POST
public Response postTransaction(Transaction tx) {
    if (!validationService.isValid(tx)) {
        return Response.status(Response.Status.BAD_REQUEST)
                .entity("Invalid transaction: missing fields or invalid amount").build();
    }
    if (tx.getTransactionId() == null || tx.getTransactionId().isBlank()) {
        tx.setTransactionId(UUID.randomUUID().toString());
    }
    if (tx.getTimestamp() == null || tx.getTimestamp().isBlank()) {
        tx.setTimestamp(Instant.now().toString());
    }
    rawEmitter.send(tx);
    return Response.accepted().entity(tx).build();
}` },
                    { label: "Validierung", file: "transaction-service/.../ValidationService.java", note: "Nur fachlich verwendbare Nachrichten gelangen in den verteilten Workflow.", code: `public boolean isValid(Transaction tx) {
    if (tx.getFromAccount() == null || tx.getFromAccount().isBlank()) return false;
    if (tx.getToAccount() == null || tx.getToAccount().isBlank()) return false;
    if (tx.getAmount() <= 0) return false;
    if (tx.getCurrency() == null || tx.getCurrency().isBlank()) return false;
    return true;
}` },
                    { label: "Topics", file: "docker-compose.yml", note: "kafka-init wartet auf den Broker und erstellt raw-transactions passend zur einen aktuellen Fraud-Service-Instanz.", code: `kafka-topics.sh --create --if-not-exists \\
  --topic raw-transactions --partitions 1 --replication-factor 1
kafka-topics.sh --create --if-not-exists \\
  --topic valid-transactions --partitions 3 --replication-factor 1
kafka-topics.sh --create --if-not-exists \\
  --topic fraud-alerts --partitions 3 --replication-factor 1` }
                ]
            },
            {
                number: 2,
                short: "Fraud-Prüfung",
                kicker: "Strategy Pattern",
                title: "Betrug erkennen und Nachrichten aufteilen",
                summary: "Der Fraud-Alert-Service prüft jede rohe Transaktion mit austauschbaren Strategien und leitet sie entweder als Fraud Alert oder als gültige Transaktion weiter.",
                body: `
                    <h3>Wie wird geprüft?</h3><p>Der Consumer übergibt jede Nachricht an den FraudCheckService. Dieser läuft über alle Implementierungen des <span class="term" data-term="Strategy Pattern">Strategy Patterns</span>.</p>
                    <h3>Welche Regeln gibt es?</h3><p>LocationStrategy erkennt NG, KP und RU in den Kontonamen. HighAmountStrategy erkennt ausschließlich Beträge über 10.000.</p>
                    <h3>Was kommt heraus?</h3><p>Beim ersten Treffer entsteht ein FraudAlert in fraud-alerts. Ohne Treffer wird die ursprüngliche Transaktion nach valid-transactions gesendet.</p>`,
                memory: "Eine Strategie schlägt an: fraud-alerts. Keine Strategie schlägt an: valid-transactions.",
                nodes: ["raw", "fraud", "alerts", "valid"],
                codes: [
                    { label: "High Amount", file: "fraud-alert-service/.../HighAmountStrategy.java", note: "Die Grenze selbst ist als Konstante benannt; genau 10.000 ist noch gültig, erst ein größerer Betrag gilt als Fraud.", code: `public class HighAmountStrategy implements IFraudStrategy {
    private static final double LIMIT = 10000.0;

    @Override
    public boolean isFraud(Transaction tx) {
        return tx.getAmount() > LIMIT;
    }

    @Override
    public FraudeAlertType getAlertType() {
        return FraudeAlertType.HIGH_AMOUNT;
    }
}` },
                    { label: "Strategien", file: "fraud-alert-service/.../FraudCheckService.java", note: "return beendet die Prüfung beim ersten Treffer. Neue Regeln lassen sich als weitere Strategie registrieren.", code: `public FraudCheckService() {
    strategies.add(new LocationStrategy());
    strategies.add(new HighAmountStrategy());
}

public void checkFraud(Transaction tx) {
    for (IFraudStrategy strategy : strategies) {
        if (strategy.isFraud(tx)) {
            fraudAlertProducer.sendFraud(tx, strategy.getAlertType());
            return;
        }
    }
    validTxProducer.sendValidTransaction(tx);
}` },
                    { label: "Routing", file: "fraud-alert-service/.../ValidTransactionProducer.java", note: "Auch beim zweiten Kafka-Schritt bleibt fromAccount der Key und damit die Reihenfolge pro Quellkonto erhalten.", code: `@Channel("valid-transactions-out")
Emitter<Record<String, Transaction>> producer;

public void sendValidTransaction(Transaction tx) {
    producer.send(Record.of(tx.getFromAccount(), tx));
    LOG.info("Valid Transaction sent: " + tx);
}` }
                ]
            },
            {
                number: 3,
                short: "Benachrichtigung",
                kicker: "Publish/Subscribe",
                title: "Gültige Transaktionen und Warnungen protokollieren",
                summary: "Der Notification-Service beobachtet beide Ergebnis-Topics. Normale Vorgänge erscheinen als INFO, Betrugsfälle auffälliger als WARN.",
                body: `
                    <h3>Zwei unabhängige Eingänge</h3><p>Je eine @Incoming-Methode konsumiert valid-transactions und fraud-alerts. Beide Channels sind der notification-group zugeordnet.</p>
                    <h3>Warum eine eigene Consumer Group?</h3><p>Eine <span class="term" data-term="Consumer Group">Consumer Group</span> erhält ihre eigene Sicht auf ein Topic. Deshalb kann Notification jede gültige Nachricht sehen, obwohl der Transfer-Service sie ebenfalls konsumiert.</p>
                    <h3>Wie skaliert das?</h3><p>Aktuell gibt es je Channel einen Consumer. Mit drei Partitionen wären bis zu drei Notification-Instanzen innerhalb derselben Gruppe parallel sinnvoll.</p>`,
                memory: "Andere Gruppe als Transfer bedeutet: Notification und Transfer bekommen beide jede gültige Nachricht.",
                nodes: ["valid", "alerts", "notification"],
                codes: [
                    { label: "Consumer", file: "notification-service/.../NotificationConsumer.java", note: "Die Log-Level bilden die fachliche Wichtigkeit ab: INFO für den Normalfall, WARN für einen Betrugsverdacht.", code: `@Incoming("valid-transactions-in")
public void consumeValidTransaction(Transaction tx) {
    LOG.info("Valid transaction: " + tx.getTransactionId()
            + " amount=" + tx.getAmount() + " " + tx.getCurrency());
}

@Incoming("fraud-alerts-in")
public void consumeFraudAlert(FraudAlert alert) {
    LOG.warn("Fraud alert: " + alert.getAlertType()
            + " severity=" + alert.getSeverity());
}` },
                    { label: "Konfiguration", file: "notification-service/src/main/resources/application.properties", note: "Die Offsets werden pro Kombination aus Gruppe, Topic und Partition verwaltet. Daher dürfen beide Channels denselben Gruppennamen tragen.", code: `mp.messaging.incoming.valid-transactions-in.topic=valid-transactions
mp.messaging.incoming.valid-transactions-in.group.id=notification-group

mp.messaging.incoming.fraud-alerts-in.topic=fraud-alerts
mp.messaging.incoming.fraud-alerts-in.group.id=notification-group` }
                ]
            },
            {
                number: 4,
                short: "Überweisung",
                kicker: "Job Queue und Datenbank",
                title: "Kontostände atomar verändern und Transaktion speichern",
                summary: "Der Transfer-Service lädt oder erzeugt beide Konten, bucht den Betrag um und speichert einen Transaktionsdatensatz – alles innerhalb derselben Datenbanktransaktion.",
                body: `
                    <h3>Konten vorbereiten</h3><p>getOrCreateAccount lädt ein Konto oder legt es mit 1.000,00 Startguthaben an. Für Geld wird <span class="term" data-term="BigDecimal">BigDecimal</span> verwendet.</p>
                    <h3>Umbuchung</h3><p>Vom Quellkonto wird amount subtrahiert, beim Zielkonto addiert. Verwaltete JPA-Entities werden beim Commit automatisch aktualisiert.</p>
                    <h3>Warum @Transactional?</h3><p><span class="term" data-term="Transaktion (DB)">@Transactional</span> macht Abbuchung, Gutschrift und Historieneintrag atomar: entweder gelingt alles oder alles wird zurückgerollt.</p>`,
                memory: "Eine fachliche Überweisung ist eine einzige DB-Transaktion aus Abbuchung + Gutschrift + Historie.",
                nodes: ["valid", "transfer", "database"],
                codes: [
                    { label: "Verarbeitung", file: "transfer-service/.../AccountService.java", note: "fromAccount und toAccount sind verwaltete Entities. Das Ändern ihrer Felder genügt; Hibernate schreibt sie beim Commit.", code: `@Transactional
public boolean processTransaction(Transaction tx) {
    if (transactionRepository.findByTransactionId(tx.getTransactionId()) != null) {
        return false;
    }

    BigDecimal amount = BigDecimal.valueOf(tx.getAmount());
    Account fromAccount = getOrCreateAccount(tx.getFromAccount());
    Account toAccount = getOrCreateAccount(tx.getToAccount());

    fromAccount.setBalance(fromAccount.getBalance().subtract(amount));
    toAccount.setBalance(toAccount.getBalance().add(amount));

    transactionRepository.persist(new TransactionEntity(
        tx.getTransactionId(), tx.getFromAccount(), tx.getToAccount(),
        amount, tx.getCurrency(), tx.getTimestamp()));
    return true;
}` },
                    { label: "Consumer", file: "transfer-service/.../ValidTransactionConsumer.java", note: "Der Consumer bleibt schlank. Fachlogik und Datenbankzugriff liegen zentral im AccountService.", code: `@Incoming("valid-transactions-in")
@Transactional
public void processValid(Transaction tx) {
    boolean processed = accountService.processTransaction(tx);
    if (processed) {
        LOG.info("Processed transaction: " + tx.getTransactionId());
    } else {
        LOG.info("Skipped duplicate transaction: " + tx.getTransactionId());
    }
}` }
                ]
            },
            {
                number: 5,
                short: "Skalierung",
                kicker: "Drei Transfer-Instanzen",
                title: "Arbeit verteilen, ohne dreimal zu überweisen",
                summary: "Drei Partitionen und drei Transfer-Consumer in genau einer gemeinsamen Gruppe erlauben drei parallele Arbeiter, aber nur einen Empfänger pro Nachricht.",
                body: `
                    <h3>Warum dieselbe Gruppe?</h3><p>Alle Instanzen nutzen transfer-service-group. Kafka weist jede Partition innerhalb einer Gruppe höchstens einem Consumer zu. Dadurch wird eine Nachricht regulär nur einmal bearbeitet.</p>
                    <h3>Warum genau drei Partitionen?</h3><p>Mit drei Partitionen können drei Instanzen gleichzeitig arbeiten. Ein vierter Consumer wäre nur Reserve, solange keine weitere Partition existiert.</p>
                    <h3>Wie bleibt die Reihenfolge erhalten?</h3><p>fromAccount ist der <span class="term" data-term="Message Key">Message Key</span>. Alle Nachrichten desselben Quellkontos landen in derselben Partition und bleiben dort geordnet.</p>`,
                memory: "Parallelität = min(Anzahl Partitionen, Anzahl Consumer derselben Gruppe).",
                nodes: ["valid", "transfer"],
                codes: [
                    { label: "3 Instanzen", file: "docker-compose.yml", note: "Ports unterscheiden die Instanzen, der Gruppenname bleibt absichtlich gleich. So sind sie konkurrierende Worker derselben Job Queue.", code: `transfer-service-1:
  environment:
    QUARKUS_HTTP_PORT: 8084
    KAFKA_CONSUMER_GROUP_ID: transfer-service-group

transfer-service-2:
  environment:
    QUARKUS_HTTP_PORT: 8086
    KAFKA_CONSUMER_GROUP_ID: transfer-service-group

transfer-service-3:
  environment:
    QUARKUS_HTTP_PORT: 8087
    KAFKA_CONSUMER_GROUP_ID: transfer-service-group` },
                    { label: "Kafka-Key", file: "fraud-alert-service/.../ValidTransactionProducer.java", note: "Kafka hasht den Key zur Partition. Gleiche Quellkonten werden deshalb nicht auf parallele Worker aufgeteilt.", code: `producer.send(Record.of(tx.getFromAccount(), tx));` },
                    { label: "Gruppe", file: "transfer-service/src/main/resources/application.properties", note: "Die Umgebungsvariable setzt bei allen Containern denselben Wert; der Default hilft beim lokalen Start.", code: `mp.messaging.incoming.valid-transactions-in.topic=valid-transactions
mp.messaging.incoming.valid-transactions-in.group.id=
  \${KAFKA_CONSUMER_GROUP_ID:transfer-service-group}` }
                ]
            },
            {
                number: 6,
                short: "Idempotenz",
                kicker: "Schutz vor Wiederholung",
                title: "Dieselbe Transaktion höchstens einmal ausführen",
                summary: "Weil Kafka Nachrichten erneut zustellen darf, schützt die transactionId durch Vorabprüfung, Unique Constraint und Rollback vor doppelten Abbuchungen.",
                body: `
                    <h3>Warum kann eine Nachricht doppelt kommen?</h3><p>Kafka bietet hier <span class="term" data-term="At-least-once">At-least-once</span>. Nach Verarbeitung, aber vor dem erfolgreichen Offset-Commit, kann ein Absturz eine erneute Zustellung verursachen.</p>
                    <h3>Erste Schutzschicht</h3><p>Vor der Buchung sucht das Repository nach der transactionId. Ein bereits bekannter Auftrag wird ohne Änderungen übersprungen.</p>
                    <h3>Zweite Schutzschicht</h3><p>Eine <span class="term" data-term="Unique Constraint">Unique Constraint</span> in PostgreSQL verhindert auch bei einem zeitgleichen Zugriff zwei gleiche Datensätze. Der zweite DB-Vorgang scheitert und seine Kontostandsänderungen werden zurückgerollt.</p>`,
                memory: "Consumer Group verhindert normale Mehrfachverteilung; Idempotenz schützt zusätzlich vor erneuter Zustellung.",
                nodes: ["transfer", "database"],
                codes: [
                    { label: "Vorabprüfung", file: "transfer-service/.../AccountService.java", note: "Der schnelle Normalfall eines Duplikats endet sofort, bevor Konten verändert werden.", code: `if (transactionRepository.findByTransactionId(tx.getTransactionId()) != null) {
    return false;
}` },
                    { label: "DB-Schutz", file: "transfer-service/.../003-add-unique-constraints.xml", note: "Die Datenbank ist die letzte, gemeinsame Instanz für alle drei Service-Prozesse und kann konkurrierende Inserts zuverlässig entscheiden.", code: `<addUniqueConstraint
    tableName="transactions"
    columnNames="transaction_id"
    constraintName="uk_transactions_transaction_id"/>` },
                    { label: "Entity", file: "transfer-service/.../TransactionEntity.java", note: "Die JPA-Definition dokumentiert dieselbe Eindeutigkeitsregel zusätzlich auf Anwendungsebene.", code: `@Column(name = "transaction_id", nullable = false, unique = true)
private String transactionId;` }
                ]
            }
        ];
    }

    createGlossary() {
        return {
            "REST": "HTTP-Schnittstelle. Der Client sendet hier per POST eine neue Transaktion an den Transaction-Service.",
            "Topic": "Benannter Nachrichtenstrom in Kafka, zum Beispiel raw-transactions.",
            "Producer": "Komponente, die Nachrichten in ein Kafka-Topic schreibt.",
            "Consumer": "Komponente, die Nachrichten aus einem Kafka-Topic liest.",
            "Partition": "Geordneter Teil eines Topics. Mehrere Partitionen ermöglichen parallele Verarbeitung.",
            "Consumer Group": "Gemeinsam arbeitende Consumer. Innerhalb einer Gruppe geht jede Partition nur an einen Consumer.",
            "Message Key": "Schlüssel einer Kafka-Nachricht. Gleiche Keys werden derselben Partition zugeordnet.",
            "Replication Factor": "Anzahl der Broker, auf denen eine Partition gespeichert wird. Im Ein-Broker-Labor muss er 1 sein.",
            "Strategy Pattern": "Entwurfsmuster für austauschbare Regeln hinter einer gemeinsamen Schnittstelle.",
            "BigDecimal": "Java-Typ für genaue Dezimalrechnung; für Geld sicherer als typische Gleitkommazahlen.",
            "Transaktion (DB)": "Zusammengehörige Datenbankoperationen, die vollständig bestätigt oder vollständig zurückgerollt werden.",
            "At-least-once": "Zustellgarantie: Eine Nachricht kommt mindestens einmal, kann bei Fehlern aber wiederholt werden.",
            "Idempotenz": "Wiederholtes Ausführen derselben Operation hat nach dem ersten Erfolg keine weitere Wirkung.",
            "Unique Constraint": "Datenbankregel, die doppelte Werte in einer Spalte zuverlässig verhindert.",
            "Offset": "Position eines Consumers in einer Kafka-Partition. Kafka merkt sich damit den Lesefortschritt.",
            "DTO": "Einfaches Datenobjekt zum Transport von Werten zwischen REST, Kafka und Anwendungscode.",
            "Entity": "Java-Objekt, das über JPA einer Datenbanktabelle zugeordnet ist.",
            "Rollback": "Rücknahme aller Änderungen einer fehlgeschlagenen Datenbanktransaktion."
        };
    }

    renderNavigation() {
        this.taskNavigation.innerHTML = "";
        this.tasks.forEach((task, index) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = "task-link";
            button.dataset.index = index;
            button.innerHTML = `<span class="index">${task.number}</span><span>${task.short}</span><span class="done">✓</span>`;
            button.addEventListener("click", () => this.selectTask(index));
            this.taskNavigation.append(button);
        });
        this.updateProgress();
    }

    renderFlow() {
        const nodes = [
            { id: "transaction", type: "service", small: "Port 8081", title: "Transaction-Service", sub: "validiert REST" },
            { arrow: "→" },
            { id: "raw", type: "topic", small: "Kafka-Topic", title: "raw-transactions", sub: "1 Partition" },
            { arrow: "→" },
            { id: "fraud", type: "service", small: "Port 8083", title: "Fraud-Service", sub: "prüft Strategien" }
        ];

        nodes.forEach(item => {
            if (item.arrow) {
                const arrow = document.createElement("div");
                arrow.className = "arrow";
                arrow.textContent = item.arrow;
                this.flowDiagram.append(arrow);
            } else {
                this.flowDiagram.append(this.createFlowNode(item));
            }
        });

        const splitArrow = document.createElement("div");
        splitArrow.className = "arrow";
        splitArrow.textContent = "⇉";
        this.flowDiagram.append(splitArrow);

        const outcomes = document.createElement("div");
        outcomes.className = "flow-outcomes";
        outcomes.append(
            this.createFlowPath([
                { id: "alerts", type: "topic", small: "Fraud", title: "fraud-alerts", sub: "Warnung" },
                { id: "notification", type: "service", small: "Port 8085", title: "Notification", sub: "WARN; beobachtet auch valid" }
            ]),
            this.createFlowPath([
                { id: "valid", type: "topic", small: "Kein Fraud", title: "valid-transactions", sub: "Job Queue" },
                { id: "transfer", type: "service", small: "3 Instanzen", title: "Transfer-Service", sub: "bucht Geld um" },
                { id: "database", type: "database", small: "PostgreSQL", title: "Konten + Historie", sub: "idempotent" }
            ])
        );
        this.flowDiagram.append(outcomes);
    }

    createFlowPath(nodes) {
        const path = document.createElement("div");
        path.className = "flow-path";
        nodes.forEach((node, index) => {
            if (index > 0) {
                const arrow = document.createElement("span");
                arrow.className = "arrow";
                arrow.textContent = "→";
                path.append(arrow);
            }
            path.append(this.createFlowNode(node));
        });
        return path;
    }

    createFlowNode(node) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = `flow-node ${node.type}`;
        button.dataset.node = node.id;
        button.innerHTML = `<small>${node.small}</small><strong>${node.title}</strong><span>${node.sub}</span>`;
        button.addEventListener("click", () => {
            const index = this.tasks.findIndex(task => task.nodes.includes(node.id));
            if (index >= 0) this.selectTask(index);
        });
        return button;
    }

    selectTask(index) {
        this.currentTask = Math.max(0, Math.min(index, this.tasks.length - 1));
        this.currentCode = 0;
        const task = this.tasks[this.currentTask];
        this.detailNumber.textContent = task.number;
        this.detailKicker.textContent = task.kicker;
        this.detailTitle.textContent = task.title;
        this.detailSummary.textContent = task.summary;
        this.detailBody.innerHTML = task.body;
        this.detailMemory.textContent = task.memory;
        this.previousButton.disabled = this.currentTask === 0;
        this.nextButton.disabled = this.currentTask === this.tasks.length - 1;
        this.understoodButton.textContent = this.completed.has(task.number) ? "✓ Verstanden" : "Als verstanden markieren";
        this.renderCodeTabs();
        this.updateActiveStates();
        this.bindTermLinks();
    }

    renderCodeTabs() {
        const task = this.tasks[this.currentTask];
        this.codeTabs.innerHTML = "";
        task.codes.forEach((snippet, index) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `code-tab${index === this.currentCode ? " active" : ""}`;
            button.textContent = snippet.label;
            button.addEventListener("click", () => { this.currentCode = index; this.renderCodeTabs(); });
            this.codeTabs.append(button);
        });
        const snippet = task.codes[this.currentCode];
        this.fileLabel.textContent = snippet.file;
        this.codeContent.textContent = snippet.code;
        this.codeNote.textContent = snippet.note;
    }

    updateActiveStates() {
        document.querySelectorAll(".task-link").forEach((button, index) => {
            button.classList.toggle("active", index === this.currentTask);
            button.classList.toggle("completed", this.completed.has(this.tasks[index].number));
        });
        const activeNodes = this.tasks[this.currentTask].nodes;
        document.querySelectorAll(".flow-node").forEach(node => node.classList.toggle("active", activeNodes.includes(node.dataset.node)));
    }

    markUnderstood() {
        const number = this.tasks[this.currentTask].number;
        this.completed.add(number);
        localStorage.setItem("kafka-lernfluss-progress", JSON.stringify([...this.completed]));
        this.understoodButton.textContent = "✓ Verstanden";
        this.updateProgress();
        this.updateActiveStates();
        this.showToast(`Aufgabe ${number} als verstanden markiert`);
    }

    updateProgress() {
        const count = this.completed.size;
        if (this.progressLabel) this.progressLabel.textContent = `${count} / ${this.tasks.length} angesehen`;
        if (this.progressBar) this.progressBar.style.width = `${count / this.tasks.length * 100}%`;
    }

    renderGlossaryItems(filter = "") {
        const query = filter.trim().toLocaleLowerCase("de");
        this.glossary.innerHTML = "";
        Object.entries(this.glossaryTerms).forEach(([term, explanation]) => {
            if (query && !`${term} ${explanation}`.toLocaleLowerCase("de").includes(query)) return;
            const item = document.createElement("article");
            item.className = "glossary-item";
            item.dataset.term = term;
            item.innerHTML = `<h3></h3><p></p>`;
            item.querySelector("h3").textContent = term;
            item.querySelector("p").textContent = explanation;
            this.glossary.append(item);
        });
    }

    bindTermLinks() {
        document.querySelectorAll(".term").forEach(term => term.addEventListener("click", () => {
            this.glossarySearch.value = term.dataset.term;
            this.renderGlossaryItems(term.dataset.term);
            const item = this.glossary.querySelector(".glossary-item");
            if (item) {
                item.classList.add("highlight");
                item.scrollIntoView({ behavior: "smooth", block: "center" });
            }
        }));
    }

    bindEvents() {
        this.previousButton.addEventListener("click", () => this.selectTask(this.currentTask - 1));
        this.nextButton.addEventListener("click", () => this.selectTask(this.currentTask + 1));
        this.understoodButton.addEventListener("click", () => this.markUnderstood());
        this.copyButton.addEventListener("click", async () => {
            try {
                await navigator.clipboard.writeText(this.codeContent.textContent);
                this.showToast("Code kopiert");
            } catch (_) {
                this.showToast("Kopieren ist in diesem Browser blockiert");
            }
        });
        this.glossarySearch.addEventListener("input", event => this.renderGlossaryItems(event.target.value));
        this.tourButton.addEventListener("click", () => {
            this.selectTask(0);
            document.querySelector(".detail-grid").scrollIntoView({ behavior: "smooth" });
        });
        this.resetButton.addEventListener("click", () => {
            this.completed.clear();
            localStorage.removeItem("kafka-lernfluss-progress");
            this.updateProgress();
            this.updateActiveStates();
            this.showToast("Fortschritt zurückgesetzt");
        });
        document.addEventListener("keydown", event => {
            if (event.target.matches("input")) return;
            if (event.key === "ArrowRight") this.selectTask(this.currentTask + 1);
            if (event.key === "ArrowLeft") this.selectTask(this.currentTask - 1);
        });
    }

    showToast(message) {
        this.toast.textContent = message;
        this.toast.classList.add("visible");
        clearTimeout(this.toastTimer);
        this.toastTimer = setTimeout(() => this.toast.classList.remove("visible"), 1800);
    }
}

document.addEventListener("DOMContentLoaded", () => new LernflussApp());
