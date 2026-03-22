package com.enterprise.invoice.controller;


import com.enterprise.invoice.service.InvoiceJobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("v1/invoice")
@RequiredArgsConstructor
public class InvoiceJobController {

    private final InvoiceJobService invoiceJob;

    //not used, just for testing purpose, the actual batch job is triggered by scheduler
    @PostMapping
    public ResponseEntity<String> runInvoiceJob(@RequestParam("file") MultipartFile file) {

        if (file.isEmpty()) {
            return ResponseEntity.badRequest().body("Empty file");
        }

        invoiceJob.processStaging(file);

        return ResponseEntity.ok("Batch Job Started for file:" + file.getOriginalFilename());
    }

}
