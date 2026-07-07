package de.berlin.htw.boundary;

import de.berlin.htw.boundary.dto.FraudAlert;
import de.berlin.htw.boundary.dto.Transaction;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class NotificationConsumer {

    private static final Logger LOG = Logger.getLogger(NotificationConsumer.class);

    @Incoming("valid-transactions-in")
    public void consumeValidTransaction(Transaction tx) {
        LOG.info("Valid transaction: " + tx.getTransactionId() + " from " + tx.getFromAccount()
                + " to " + tx.getToAccount() + " amount=" + tx.getAmount() + " " + tx.getCurrency());
    }

    @Incoming("fraud-alerts-in")
    public void consumeFraudAlert(FraudAlert alert) {
        LOG.warn("Fraud alert: " + alert.getAlertType() + " account=" + alert.getAccountId()
                + " amount=" + alert.getTransactionAmount() + " severity=" + alert.getSeverity());
    }
}
