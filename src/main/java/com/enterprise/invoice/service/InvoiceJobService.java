package com.enterprise.invoice.service;

import com.enterprise.invoice.entity.Staging;
import com.enterprise.invoice.repository.StagingRepository;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.JsonNodeFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceJobService {


    private final JobOperator jobOperator;
    private final Job stagingJob;
    private final StagingRepository stagingRepository;

    public void processStaging(MultipartFile file) {

        //save file temporarily
        String filePath = System.getProperty("user.dir") + "/uploads/" + file.getOriginalFilename();

        try {
            File tempFile = new File(filePath);
            file.transferTo(tempFile);

            FileReader fileReader = new FileReader(tempFile);
            CSVReader csvReader = new CSVReader(fileReader);
            csvReader.skip(1);

            csvReader.readAll().forEach(line -> {

                stagingRepository.save(Staging.builder()
                        .invoiceNumber(line[0])
                        .attributes(line[1])
                        .status("NEW")
                        .srcFile(file.getOriginalFilename())
                        .build());

            });

        } catch (IOException | CsvException e) {
            throw new RuntimeException(e);
        }

    }

    @Scheduled(cron = "0 */1 * * * *")
    public void processInvoice() {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("run.id", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();

            JobExecution execution = jobOperator.start(stagingJob, jobParameters);

            log.info("Execution Started:{}", execution.getJobParameters());

        } catch (JobInstanceAlreadyCompleteException | InvalidJobParametersException |
                 JobExecutionAlreadyRunningException | JobRestartException e) {
            throw new RuntimeException(e);
        }
    }

}
