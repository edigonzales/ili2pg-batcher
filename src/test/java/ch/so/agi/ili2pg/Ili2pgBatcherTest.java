package ch.so.agi.ili2pg;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class Ili2pgBatcherTest {
    
    
    static String WAIT_PATTERN = ".*database system is ready to accept connections.*\\s";
    // static: will be shared between test methods
    // non static: will be started before and stopped after each test method

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("sogis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"))
        .withDatabaseName("edit")
        .withUsername(DbUtil.PG_CON_DDLUSER)
        .withPassword(DbUtil.PG_CON_DDLPASS)
        //.withInitScript("init_postgresql.sql")
        .waitingFor(Wait.forLogMessage(WAIT_PATTERN, 2));

    
    @Test
    void dummy() throws Exception {
        assertTrue(postgres.isRunning());
        
        System.out.println(postgres.getJdbcUrl());
        System.out.println(postgres.getUsername());        
        System.out.println(postgres.getPassword());        
    }

    
}
