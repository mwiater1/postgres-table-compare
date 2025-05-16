package dev.mwiater.postgrestablecompare;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties("compare")
public class CompareConfig {

    private List<Compare> tables;
}
