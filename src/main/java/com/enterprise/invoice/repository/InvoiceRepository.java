package com.enterprise.invoice.repository;

import com.enterprise.invoice.entity.Invoice;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface InvoiceRepository extends JpaRepository<Invoice,Long> {

    Optional<Invoice> findByInvoiceNumber(String invoiceNumber);

}
