package com.enterprise.invoice.service;

import com.enterprise.invoice.entity.Staging;
import com.enterprise.invoice.repository.StagingRepository;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.batch.core.*;
import org.springframework.batch.core.job.flow.JobFlowExecutor;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;


import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceJobService {


    private final JobOperator jobOperator;
    private final Job stagingJob;
    private final JobLauncher jobLauncher;
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

            JobExecution execution = jobLauncher.run(stagingJob, jobParameters);

            log.info("Execution ID:{} Status:{}", execution.getJobId(), execution.getStatus());

        } catch (JobParametersInvalidException |
                 JobInstanceAlreadyCompleteException | JobExecutionAlreadyRunningException | JobRestartException e) {
            throw new RuntimeException(e);
        }
    }

}
