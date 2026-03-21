package com.enterprise.invoice.entity;


import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.Type;
import org.hibernate.type.SqlTypes;
import tools.jackson.databind.JsonNode;

import java.time.LocalDateTime;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "staging")
public class Staging {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "invoice_number")
    private String invoiceNumber;
    //explicitly mapping json node to jsonb, cuz hibernate doesn't support jsonb mapping by default
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "attributes", columnDefinition = "jsonb")
    private String attributes;
    private String status;
    private String error;
    @Column(name = "src_file")
    private String srcFile;
    private LocalDateTime createdAt = LocalDateTime.now();

}
