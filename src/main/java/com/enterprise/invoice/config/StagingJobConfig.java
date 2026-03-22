package com.enterprise.invoice.config;

import com.enterprise.invoice.batch.InvoiceStepListener;
import com.enterprise.invoice.batch.StagingMapper;
import com.enterprise.invoice.entity.Invoice;
import com.enterprise.invoice.entity.Staging;
import com.enterprise.invoice.service.StagingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.*;

import org.springframework.batch.core.job.builder.JobBuilder;

import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;

import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.CollectionUtils;


import javax.sql.DataSource;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.MDC;

@Configuration
@EnableBatchProcessing()
@Slf4j
@RequiredArgsConstructor
public class StagingJobConfig {

    private static final AtomicLong PROCESSED_COUNT = new AtomicLong(0);
    private static final AtomicLong FAILED_COUNT = new AtomicLong(0);

    private final StagingService stagingService;


    @Bean
    public JobExecutionDecider invoiceProcessingDecider() {
        return new JobExecutionDecider() {
            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                // This decider can be enhanced to check job parameters or execution context to decide flow
                int count = stagingService.checkExistingStaging();
                log.info("Decider checked existing staging count: {}", count);

                if (count > 0) {
                    log.info("{} No of NEW staging records found,", count);
                    return new FlowExecutionStatus("CONTINUE");
                } else {
                    log.info("No NEW staging records found, stopping processing step");
                    return new FlowExecutionStatus("STOP");
                }

            }
        };
    }

    @Bean
    public Job stagingJob(JobRepository jobRepository,
                          @Qualifier("stagingStep") Step stagingStep, @Qualifier("stagingCleanupStep") Step stagingCleanupStep, JobExecutionDecider jobExecutionDecider) {
        return new JobBuilder("stagingJob", jobRepository)
                .listener(jobExecutionListener())
                .start(jobExecutionDecider).on("STOP").end()
                .from(jobExecutionDecider).on("CONTINUE").to(stagingStep)
                .next(stagingCleanupStep)
                .end()
                .build();
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                MDC.put("jobExecutionId", String.valueOf(jobExecution.getId()));
                log.info("Invoice Job Started, id={}", jobExecution.getId());
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                log.info("Invoice Job Completed with status: {} (id={})", jobExecution.getStatus(), jobExecution.getId());
                log.info("Processing summary - processed={}, failed={}", PROCESSED_COUNT.get(), FAILED_COUNT.get());
                MDC.remove("jobExecutionId");

                // reset counters for next run
                PROCESSED_COUNT.set(0);
                FAILED_COUNT.set(0);
            }
        };
    }

    @Bean
    public Step stagingStep(JobRepository jobRepository,
                            ItemReader<Staging> reader,
                            ItemProcessor<Staging, Invoice> processor,
                            ItemWriter<Invoice> writer,
                            ThreadPoolTaskExecutor taskExecutor,
                            InvoiceStepListener invoiceStepListener,
                            PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("stagingStep", jobRepository)
                .<Staging, Invoice>chunk(100, platformTransactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .taskExecutor(taskExecutor) // use injected, configured task executor for better thread management
                .listener(invoiceStepListener)
                .faultTolerant()
                .skip(Exception.class)
                .skipLimit(100)
                .listener(skipListener(stagingService))
                .build();
    }

    @Bean
    public Step stagingCleanupStep(JobRepository jobRepository, PlatformTransactionManager platformTransactionManager) {
        return new StepBuilder("stagingCleanupStep", jobRepository)
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        var jobCtx = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
                        @SuppressWarnings("unchecked")
                        Collection<String> invoiceNumbers = (Collection<String>) jobCtx.get("writtenInvoiceNumbers");
                        if (CollectionUtils.isEmpty(invoiceNumbers)) {
                            log.info("No processed invoiceNumbers found for cleanup");
                            return RepeatStatus.FINISHED;
                        }

                        // copy and clear
                        Set<String> toCleanup = new HashSet<>(invoiceNumbers);
                        invoiceNumbers.clear();
                        jobCtx.remove("writtenInvoiceNumbers");

                        log.info("Starting cleanup of {} staging records", toCleanup.size());
                        int success = 0;
                        for (String inv : toCleanup) {
                            try {
                                stagingService.markSuccessByInvoiceNumber(inv, true);
                                success++;
                            } catch (Exception ex) {
                                log.error("Failed to cleanup staging for invoiceNumber={}: {}", inv, stagingService.getRootCauseMessage(ex), ex);
                            }
                        }
                        log.info("Completed staging cleanup: {} succeeded, {} attempted", success, toCleanup.size());
                        return RepeatStatus.FINISHED;
                    }
                }, platformTransactionManager).build();
    }

    @Bean
    public JdbcPagingItemReader<Staging> reader(DataSource dataSource, PagingQueryProvider pagingQueryProvider) {

        JdbcPagingItemReader<Staging> reader = new JdbcPagingItemReader<>();

        reader.setDataSource(dataSource);
        reader.setPageSize(100);
        reader.setRowMapper(new StagingMapper());
        reader.setQueryProvider(pagingQueryProvider);

        return reader;
    }

    @Bean
    public SqlPagingQueryProviderFactoryBean queryProvider(DataSource dataSource) {
        SqlPagingQueryProviderFactoryBean provider = new SqlPagingQueryProviderFactoryBean();

        provider.setDataSource(dataSource);
        provider.setSelectClause("SELECT id,invoice_number,attributes,status,src_file,created_at,error");
        provider.setFromClause("FROM staging");
        provider.setWhereClause("WHERE status = 'NEW'");
        provider.setSortKey("id");
        return provider;
    }

    // Processor
    @Bean
    public ItemProcessor<Staging, Invoice> processor() {
        ObjectMapper objectMapper = new JsonMapper();
        return staging -> {
            try {
                if (staging == null) {
                    log.warn("Received null staging item in processor");
                    throw new ValidationException("Null staging item");
                }

                String attributesJson = staging.getAttributes();
                if (attributesJson == null || attributesJson.trim().isEmpty()) {
                    String err = "Missing attributes JSON for invoice=" + staging.getInvoiceNumber();
                    log.warn(err);
                    throw new ValidationException(err);
                }

                JsonNode attributes;
                try {
                    attributes = objectMapper.readTree(attributesJson);
                } catch (Exception ex) {
                    String err = "Invalid JSON for invoice=" + staging.getInvoiceNumber() + ": " + stagingService.getRootCauseMessage(ex);
                    log.warn(err, ex);
                    throw new ValidationException(err);
                }

                JsonNode amountNode = attributes.path("amount");
                if (amountNode.isMissingNode() || !amountNode.isNumber()) {
                    String err = "Missing or non-numeric 'amount' for invoice=" + staging.getInvoiceNumber();
                    log.warn(err);
                    throw new ValidationException(err);
                }

                BigDecimal value = amountNode.decimalValue();

                if (value.compareTo(BigDecimal.ZERO) <= 0) {
                    String err = "Invalid amount for invoice=" + staging.getInvoiceNumber() + ": " + value;
                    log.warn(err);
                    throw new ValidationException(err);
                }

                Invoice invoice = Invoice
                        .builder()
                        .invoiceNumber(staging.getInvoiceNumber())
                        .vendorCode(attributes.path("vendorCode").isMissingNode() ? null : attributes.path("vendorCode").asText())
                        .attributes(attributesJson)
                        .build();

                log.debug("Processed staging id={} invoiceNumber={}", staging.getId(), staging.getInvoiceNumber());
                PROCESSED_COUNT.incrementAndGet();
                return invoice;
            } catch (ValidationException ve) {
                FAILED_COUNT.incrementAndGet();
                // rethrow validation exceptions unchanged so skip handling is consistent
                throw ve;
            } catch (Exception ex) {
                FAILED_COUNT.incrementAndGet();
                // unexpected exceptions: log with root cause and rethrow as ValidationException so the SkipListener will capture it
                String msg = "Unexpected processing error for staging id=" + (staging == null ? null : staging.getId()) + " invoiceNumber=" + (staging == null ? null : staging.getInvoiceNumber()) + ": " + stagingService.getRootCauseMessage(ex);
                log.error(msg, ex);
                throw new ValidationException(msg);
            }
        };
    }

    @Bean
    public JdbcBatchItemWriter<Invoice> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Invoice> writer = new JdbcBatchItemWriter<>();
        writer.setDataSource(dataSource);
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>()); //automatically map bean properties to SQL parameters

        // Use DB current timestamp for created_at and updated_at on insert,
        // and set updated_at to CURRENT_TIMESTAMP on conflict (upsert).
        writer.setSql("INSERT INTO invoice (invoice_number, vendor_code, attributes, created_at, updated_at) VALUES (:invoiceNumber, :vendorCode, cast(:attributes as jsonb), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) ON CONFLICT (invoice_number) DO UPDATE SET vendor_code = EXCLUDED.vendor_code, attributes = EXCLUDED.attributes, updated_at = CURRENT_TIMESTAMP");
        // ensure the writer is fully initialized
        try {
            writer.afterPropertiesSet();
        } catch (Exception ex) {
            log.error("Failed to initialize JdbcBatchItemWriter: {}", stagingService.getRootCauseMessage(ex), ex);
            throw new RuntimeException("Writer initialization failed", ex);
        }
        return writer;
    }

    // Skip Listener to handle invalid rows
    @Bean
    public SkipListener<Staging, Invoice> skipListener(StagingService stagingService) {
        return new SkipListener<>() {
            @Override
            public void onSkipInProcess(Staging item, Throwable t) {
                FAILED_COUNT.incrementAndGet();
                String message = t == null ? "Processing skipped" : t.getMessage();
                log.warn("Skipping staging record during processing (id={}, invoiceNumber={}) reason={}", item == null ? null : item.getId(), item == null ? null : item.getInvoiceNumber(), message, t);
                try {
                    stagingService.markFailed(item, "ERROR: " + message);
                } catch (Exception ex) {
                    log.error("Failed to persist staging failure for id={}, invoiceNumber={}", item == null ? null : item.getId(), item == null ? null : item.getInvoiceNumber(), ex);
                }
            }

            @Override
            public void onSkipInRead(Throwable t) {
                FAILED_COUNT.incrementAndGet();
                log.error("Error reading staging input: {}", t.getMessage(), t);
            }

            @Override
            public void onSkipInWrite(Invoice item, Throwable t) {
                FAILED_COUNT.incrementAndGet();
                String invoiceNumber = item == null ? null : item.getInvoiceNumber();
                String message = t == null ? "Write skipped" : t.getMessage();
                log.warn("Skipping invoice during write (invoiceNumber={}) reason={}", invoiceNumber, message, t);
                try {
                    stagingService.markFailedByInvoiceNumber(invoiceNumber, "WRITE_ERROR: " + message);
                } catch (Exception ex) {
                    log.error("Failed to persist staging failure for invoiceNumber={}", invoiceNumber, ex);
                }
            }
        };
    }


    // Validation exception
    static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }
}
