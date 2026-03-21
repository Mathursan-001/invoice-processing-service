package com.enterprise.invoice.service;

import com.enterprise.invoice.entity.Staging;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class StagingService {

    private final JdbcTemplate jdbcTemplate;

    @Transactional
    public void markFailed(Staging item, String errorMessage) {
        if (item == null) return;
        try {
            // Try update by id
            int updated = jdbcTemplate.update(
                    "UPDATE staging SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                    "FAILED", errorMessage, item.getId()
            );
            if (updated > 0) {
                log.info("Marked staging id={} invoiceNumber={} as FAILED", item.getId(), item.getInvoiceNumber());
                return;
            }

            // Fallback: insert a failed staging record
            jdbcTemplate.update(
                    "INSERT INTO staging (invoice_number, attributes, status, error, src_file, created_at) VALUES (?, cast(? as jsonb), ?, ?, ?, CURRENT_TIMESTAMP)",
                    item.getInvoiceNumber(), item.getAttributes(), "FAILED", errorMessage, item.getSrcFile()
            );
            log.info("Inserted fallback failed staging for invoiceNumber={}", item.getInvoiceNumber());
        } catch (Exception ex) {
            log.error("Failed to mark staging id={} invoiceNumber={} as FAILED: {}", item.getId(), item.getInvoiceNumber(), getRootCauseMessage(ex), ex);
            throw ex;
        }
    }

    @Transactional
    public void markFailedByInvoiceNumber(String invoiceNumber, String errorMessage) {
        if (invoiceNumber == null) return;
        try {
            List<Long> ids = jdbcTemplate.queryForList(
                    "SELECT id FROM staging WHERE invoice_number = ? ORDER BY created_at DESC LIMIT 1",
                    Long.class, invoiceNumber
            );
            if (!ids.isEmpty()) {
                Long id = ids.get(0);
                jdbcTemplate.update(
                        "UPDATE staging SET status = ?, error = ? WHERE id = ?",
                        "FAILED", errorMessage, id
                );
                log.info("Marked staging id={} invoiceNumber={} as FAILED", id, invoiceNumber);
                return;
            }

            // nothing to update; insert a lightweight failed record
            jdbcTemplate.update(
                    "INSERT INTO staging (invoice_number, status, error, created_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                    invoiceNumber, "FAILED", errorMessage
            );
            log.info("Inserted fallback failed staging for invoiceNumber={}", invoiceNumber);
        } catch (Exception ex) {
            log.error("Failed to mark staging by invoiceNumber={} as FAILED: {}", invoiceNumber, getRootCauseMessage(ex), ex);
            throw ex;
        }
    }

    /**
     * Mark staging rows for the given invoice as PROCESSED and optionally delete them (cleanup).
     */
    @Transactional
    public void markSuccessByInvoiceNumber(String invoiceNumber, boolean cleanup) {
        if (invoiceNumber == null) return;
        try {
            if (cleanup) {
                int deleted = jdbcTemplate.update("DELETE FROM staging WHERE invoice_number = ?", invoiceNumber);
                log.info("Deleted {} staging rows for invoiceNumber={}", deleted, invoiceNumber);
            } else {
                int updated = jdbcTemplate.update("UPDATE staging SET status = ?, error = NULL, updated_at = CURRENT_TIMESTAMP WHERE invoice_number = ?", "PROCESSED", invoiceNumber);
                log.info("Marked {} staging rows as PROCESSED for invoiceNumber={}", updated, invoiceNumber);
            }
        } catch (Exception ex) {
            log.error("Failed to mark/cleanup staging for invoiceNumber={} : {}", invoiceNumber, getRootCauseMessage(ex), ex);
            throw ex;
        }
    }

    public String getRootCauseMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) root = root.getCause();
        return root.getMessage() == null ? root.toString() : root.getMessage();
    }
}
