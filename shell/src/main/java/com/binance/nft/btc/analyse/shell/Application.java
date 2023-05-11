package com.binance.nft.btc.analyse.shell;

import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.SpringApplication;

@SpringBootApplication
@ComponentScan(basePackages = {"com.binance.nft.btc.analyse.**"})
public class Application {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(Application.class);
        app.setBannerMode(Banner.Mode.OFF);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.run(args);
    }
}
