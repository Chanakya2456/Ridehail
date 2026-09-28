package com.ridehail.location;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LocationApplication {
    public static void main(String[] args) { SpringApplication.run(LocationApplication.class, args); }
}
