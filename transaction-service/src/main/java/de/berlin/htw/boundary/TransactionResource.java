package de.berlin.htw.boundary;

import de.berlin.htw.boundary.dto.Transaction;
import de.berlin.htw.control.ValidationService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import io.smallrye.reactive.messaging.kafka.Record;

import java.time.Instant;
import java.util.UUID;

@Path("/api/transactions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@ApplicationScoped
public class TransactionResource {

    @Inject
    @Channel("raw-transactions-out")
    Emitter<Record<String, Transaction>> rawEmitter;

    @Inject
    ValidationService validationService;

    @POST
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
        rawEmitter.send(Record.of(tx.getFromAccount(), tx));
        return Response.accepted().entity(tx).build();
    }
}

