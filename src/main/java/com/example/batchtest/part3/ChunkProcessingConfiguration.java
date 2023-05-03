package com.example.batchtest.part3;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class ChunkProcessingConfiguration {
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job chunkProcessingJob() {
		return this.jobBuilderFactory.get( "chunkProcessingJob" )
				.incrementer( new RunIdIncrementer() )
				.start( this.taskBaseStep() )
				.next( this.chunkBaseStep(null) )
				.build();
	}

	@Bean
	@JobScope
	public Step chunkBaseStep(@Value ( "#{jobParameters[chunkSize]}" ) String value) {
		return this.stepBuilderFactory.get( "chunkBaseStep" )
				.<String, String>chunk( StringUtils.isNotEmpty( value ) ? Integer.parseInt( value ) : 10 )
				.reader( itemReader() )
				.processor( itemProcessor() )
				.writer( itemWriter() )
				.build();
	}

	private ItemReader<String> itemReader() {
		return new ListItemReader<>( getItems() );
	}

	private ItemProcessor<String, String> itemProcessor() {
		return item -> item + ", Spring Batch";
	}

	private ItemWriter<String> itemWriter() {
		return items -> log.info( "chunk item size : {}", items.size() );
	}

	@Bean
	public Step taskBaseStep() {
		return this.stepBuilderFactory.get( "taskBaseStep" )
				.tasklet( this.tasklet(null) )
				.build();
	}

	@Bean
	@StepScope
	public Tasklet tasklet(@Value ( "#{jobParameters[chunkSize]}" ) String value) {
		List<String> items = getItems();
		return (contribution, chunkContext) -> {
			StepExecution stepExecution = contribution.getStepExecution();
//			JobParameters jobParameters = stepExecution.getJobParameters();
//			String value = jobParameters.getString( "chunkSize", "10" );
			int chunkSize = StringUtils.isNotEmpty( value ) ? Integer.parseInt( value ) : 10;
			int fromIndex = stepExecution.getReadCount();
			int toIndex = fromIndex + chunkSize;

			if ( fromIndex >= items.size() ) {
				return RepeatStatus.FINISHED;
			}

			List<String> subList = items.subList( fromIndex, toIndex );

			log.info( "task item size : {}", subList.size() );

			stepExecution.setReadCount( toIndex );
			return RepeatStatus.CONTINUABLE;
		};
	}

	private List<String> getItems() {
		List<String> items = new ArrayList<>();
		for ( int i = 0; i < 100; i++ ) {
			items.add( i + " Hello" );
		}
		return items;
	}

}
