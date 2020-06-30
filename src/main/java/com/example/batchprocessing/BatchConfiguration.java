package com.example.batchprocessing;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

	/**
	 * The PROCESSOR ( transformer ) to make available to the step1.
	 */
	@Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

	/**
	 * The READER to make available to the step1.
	 */
	@Bean
    public FlatFileItemReader<Person> reader() {
        FlatFileItemReader<Person> personFlatFileItemReader = new FlatFileItemReader<>();
        personFlatFileItemReader.setLineMapper(new LineMapper<Person>() {
            @Override
            public Person mapLine(String line, int lineNumber) throws Exception {
                String[] split = line.split(",");
                return new Person(split[0], split[1]);
            }
        });
        personFlatFileItemReader.setName("personItemReader");
        personFlatFileItemReader.setResource(new ClassPathResource("sample-data.csv"));
        personFlatFileItemReader.setLinesToSkip(1);
        personFlatFileItemReader.setStrict(false);
        personFlatFileItemReader.setRecordSeparatorPolicy(new SimpleRecordSeparatorPolicy());
        return personFlatFileItemReader;
    }

	/**
	 * The WRITER to make available to the step1.
	 */
	@Bean
    public ItemWriter<Person> writer() {
        return new SimplePersonItemWriter();
    }

    /**
     * Jobs are built from steps, where each step can involve a reader, a processor, and a writer.
     */
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("personItemReader")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

	/**
	 * Step of the job.
	 */
    @Bean
    public Step step1(StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
                      ItemWriter<Person> writer) {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
}
