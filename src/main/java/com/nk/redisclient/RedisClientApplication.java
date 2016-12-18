package com.nk.redisclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class RedisClientApplication {

	public static void main(String[] args) {
		BatchReadFromList b = SpringApplication.run(RedisClientApplication.class, args).getBean(BatchReadFromList.class);
		// b.loadScript();
		b.execScript();
	}
}
