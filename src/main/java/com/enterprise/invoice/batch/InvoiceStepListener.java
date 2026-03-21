package com.enterprise.invoice.batch;

import com.enterprise.invoice.entity.Invoice;
import com.enterprise.invoice.service.StagingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.listener.ItemWriteListener;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.stream.Collectors;

@Component
@Slf4j
@RequiredArgsConstructor
public class InvoiceStepListener implements ItemWriteListener<Invoice> {

    private final StagingService stagingService;

    @Override
    public void beforeWrite(Chunk<? extends Invoice> items) {
        if (items == null) return;
        log.debug("About to write {} invoice items", items.getItems().size());
    }

    @Override
    public void afterWrite(Chunk<? extends Invoice> items) {
        if (items == null) return;
        log.info("Total invoice items written: {}", items.getItems().size());
        // for each distinct invoiceNumber mark staging as processed; cleanup optionally
        Set<String> invoiceNumbers = items.getItems().stream()
                .map(Invoice::getInvoiceNumber)
                .filter(inv -> inv != null && !inv.isBlank())
                .collect(Collectors.toSet());
        for (String inv : invoiceNumbers) {
            try {
                stagingService.markSuccessByInvoiceNumber(inv, true); // cleanup by default
            } catch (Exception ex) {
                log.error("Failed to mark/cleanup staging for invoiceNumber={}: {}", inv, ex.getMessage(), ex);
            }
        }
    }

    @Override
    public void onWriteError(Exception exception, Chunk<? extends Invoice> items) {
        log.error("Error writing {} invoice items: {}", items == null ? 0 : items.getItems().size(), exception.getMessage(), exception);
        // attempt to mark failed by invoice numbers
        if (items != null) {
            Set<String> invoiceNumbers = items.getItems().stream()
                    .map(Invoice::getInvoiceNumber)
                    .filter(inv -> inv != null && !inv.isBlank())
                    .collect(Collectors.toSet());
            for (String inv : invoiceNumbers) {
                try {
                    stagingService.markFailedByInvoiceNumber(inv, "WRITE_ERROR: " + exception.getMessage());
                } catch (Exception ex) {
                    log.error("Failed to mark failed staging for invoiceNumber={} after write error: {}", inv, ex.getMessage(), ex);
                }
            }
        }
    }
}
