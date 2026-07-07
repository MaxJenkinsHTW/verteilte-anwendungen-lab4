package de.berlin.htw.control;

import de.berlin.htw.boundary.FraudeAlertType;
import de.berlin.htw.boundary.dto.Transaction;

public class HighAmountStrategy implements IFraudStrategy {

    private static final double LIMIT = 10000.0;

    @Override
    public boolean isFraud(Transaction tx) {
        return tx.getAmount() > LIMIT;
    }

    @Override
    public FraudeAlertType getAlertType() {
        return FraudeAlertType.HIGH_AMOUNT;
    }
}
