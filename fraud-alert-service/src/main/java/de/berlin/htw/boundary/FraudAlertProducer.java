package de.berlin.htw.boundary;

import de.berlin.htw.boundary.dto.FraudAlert;
import de.berlin.htw.boundary.dto.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.util.UUID;

@ApplicationScoped
public class FraudAlertProducer {

    private static final Logger LOG = Logger.getLogger(FraudAlertProducer.class);

    @Inject
    @Channel("fraud-alerts-out")
    Emitter<FraudAlert> fraudAlertEmitter;

    public void sendAlert(FraudAlert alert) {
        fraudAlertEmitter.send(alert);
        LOG.warn("Fraud Alert sent: " + alert.getAlertType() + " account=" + alert.getAccountId());
    }

    public void sendFraud(Transaction tx, FraudeAlertType alertType) {
        FraudAlert alert = new FraudAlert();
        alert.setAlertId(UUID.randomUUID().toString());
        alert.setAccountId(tx.getFromAccount());
        alert.setTransactionAmount(tx.getAmount());
        alert.setAlertType(alertType);
        alert.setTimestamp(Instant.now().toString());
        alert.setDetectedBy("fraud-alert-service");
        alert.setSeverity(alertType == FraudeAlertType.HIGH_AMOUNT ? "HIGH" : "MEDIUM");

        sendAlert(alert);
    }
}
