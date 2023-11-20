package ch.so.agi.ili2pg;

import org.assertj.db.type.Source;
import org.assertj.db.type.Table;
import org.interlis2.validator.Validator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.postgresql.PGProperty;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2pg.PgMain;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.db.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@Testcontainers
public class Ili2pgBatcherTest {
    
    static String WAIT_PATTERN = ".*database system is ready to accept connections.*\\s";
    
    // static: will be shared between test methods
    // non static: will be started before and stopped after each test method
    @Container
    public PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("sogis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"))
        .withDatabaseName("edit")
        .withUsername(DbUtil.PG_CON_DDLUSER)
        .withPassword(DbUtil.PG_CON_DDLPASS)
        //.withInitScript("init_postgresql.sql")
        .waitingFor(Wait.forLogMessage(WAIT_PATTERN, 2));

    @Test
    public void import_Ok() throws Exception {
        String dbSchema = "dummy";
        String modelName = "MyModel";

        // Prepare
        Config config = new Config();
        new PgMain().initConfig(config);

        config.setConfigReadFromDb(true);
        config.setModels(modelName);
        config.setModeldir("src/test/data/");
        config.setBasketHandling(Config.BASKET_HANDLING_READWRITE);
        config.setCreateDatasetCols(Config.CREATE_DATASET_COL);

        Properties props = org.postgresql.Driver.parseURL(postgres.getJdbcUrl(), null);

        config.setDbhost(props.getProperty(PGProperty.PG_HOST.getName()));
        config.setDbport(props.getProperty(PGProperty.PG_PORT.getName()));
        config.setDbdatabase(props.getProperty(PGProperty.PG_DBNAME.getName()));
        config.setDbschema(dbSchema);
        config.setDbusr(postgres.getUsername());
        config.setDbpwd(postgres.getPassword());
        config.setDburl(postgres.getJdbcUrl());

        // Wird später in doImport überschrieben.
        config.setFunction(Config.FC_SCHEMAIMPORT);
        Ili2db.run(config, null);
        
        // Run
        Ili2pgBatcher ili2pgBatcher = new Ili2pgBatcher();
        ili2pgBatcher.doImport(config, Paths.get("src/test/data/datasets.csv"));

        //Thread.sleep(120000);
        
        
        // Validate
        Source source = new Source(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        Table table = new Table(source, dbSchema + "." + "classa");

        assertThat(table).column("attr1")
            .value().isEqualTo("foo")
            .value().isEqualTo("bar");

        assertEquals(2, table.getRowsList().size());
    }
    
    @Test
    public void export_Ok(@TempDir Path tempDir) throws Exception {        
        String dbSchema = "dummy";
        String modelName = "MyModel";
        
        // Prepare
        DbUtil.importXtf(postgres, dbSchema, modelName, new File("src/test/data/MyData1.xtf"), true);
        DbUtil.importXtf(postgres, dbSchema, modelName, new File("src/test/data/MyData2.xtf"), false);
                
        // Run        
        Config config = new Config();
        new PgMain().initConfig(config);

        config.setConfigReadFromDb(true);
        config.setModels(modelName);
        config.setModeldir("src/test/data/");
        
        Properties props = org.postgresql.Driver.parseURL(postgres.getJdbcUrl(), null);

        config.setDbhost(props.getProperty(PGProperty.PG_HOST.getName()));
        config.setDbport(props.getProperty(PGProperty.PG_PORT.getName()));
        config.setDbdatabase(props.getProperty(PGProperty.PG_DBNAME.getName()));
        config.setDbschema(dbSchema);
        config.setDbusr(postgres.getUsername());
        config.setDbpwd(postgres.getPassword());
        config.setDburl(postgres.getJdbcUrl());
        
        Settings settings = new Settings();
        settings.setValue(Validator.SETTING_ALL_OBJECTS_ACCESSIBLE, Validator.TRUE);

        Ili2pgBatcher ili2pgBatcher = new Ili2pgBatcher();
        ili2pgBatcher.doExport(config, tempDir, settings);
        
        // Validate
        settings = new Settings();
        settings.setValue(Validator.SETTING_ILIDIRS, config.getModeldir());
        
        boolean valid = false;
        valid = Validator.runValidation(tempDir.resolve("MyData1.xtf").toString(), settings);
        assertTrue(valid);

        valid = Validator.runValidation(tempDir.resolve("MyData2.xtf").toString(), settings);
        assertTrue(valid);        
    }
}
