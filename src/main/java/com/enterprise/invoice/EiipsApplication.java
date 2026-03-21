package com.enterprise.invoice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class EiipsApplication {

	public static void main(String[] args) {
		SpringApplication.run(EiipsApplication.class, args);
	}

}
