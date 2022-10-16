package com.hackathon.tinker.demo;

import com.hackathon.tinker.demo.api.TinkerController;
import com.hackathon.tinker.demo.server.TinkerGraphTempServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);

	}

}
