package com.enterprise.invoice.batch;

import com.enterprise.invoice.entity.Invoice;
import com.enterprise.invoice.entity.Staging;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

public class StagingMapper implements RowMapper<Staging> {
    @Override
    public Staging mapRow(ResultSet rs, int rowNum) throws SQLException {
        return Staging
                .builder()
                .id(rs.getLong("id"))
                .invoiceNumber(rs.getString("invoice_number"))
                .attributes(rs.getString("attributes"))
                .srcFile(rs.getString("src_file"))
                .createdAt(rs.getTimestamp("created_at").toLocalDateTime())
                .build();
    }
}
