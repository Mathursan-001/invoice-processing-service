package com.enterprise.invoice.repository;

import com.enterprise.invoice.entity.Staging;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface StagingRepository extends JpaRepository<Staging, Long> {
    Staging findByInvoiceNumberAndSrcFile(String invoiceNumber,String srcFile);
}
