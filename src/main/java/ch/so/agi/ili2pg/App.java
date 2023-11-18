package ch.so.agi.ili2pg;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.interlis2.validator.Validator;

import com.google.common.collect.Maps;

import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2pg.PgMain;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
        name = "ili2pg-batcher",
        description = "Import / export transfer files dataset per dataset.",
        //version = "ili2pg-batcher version 0.0.1",
        mixinStandardHelpOptions = true,
        showDefaultValues = true,
        
        headerHeading = "%n",
        //synopsisHeading = "%n",
        descriptionHeading = "%nDescription: ",
        parameterListHeading = "%nParameters:%n",
        optionListHeading = "%nOptions:%n"
      )

public class App implements Callable<Integer> {
    
    @Option(names = { "--dbhost" }, required = false, defaultValue = "localhost", description = "The host name of the server.")
    String dbhost;
    
    @Option(names = { "--dbport" }, required = false, defaultValue = "5432", description = "The port number the server is listening on.") 
    String dbport;

    @Option(names = { "--dbdatabase" }, required = true, description = "The database name.") 
    String dbdatabase;

    @Option(names = { "--dbusr" }, required = true, description = "User name to access database.") 
    String dbusr;

    @Option(names = { "--dbpwd" }, required = true, description = "Password of user used to access database.") 
    String dbpwd;

    @Option(names = { "--dbschema" }, required = false, defaultValue = "public", description = "The name of the schema in the database.") 
    String dbschema;

    @Option(names = { "--disableValidation" }, required = false, description = "Disable validation of data.") 
    boolean disableValidation;
    
    @Option(names = { "--models" }, required = true, description = "Name(s) of ili-models.") 
    String models;

    @Option(names = { "--modeldir" }, required = false, description = "Path(s) of directories containing ili-files.") 
    String modeldir;

    @Option(names = { "--itf" }, required = false, description = "INTERLIS 1 data") 
    boolean itf;

    @Option(names = { "--allObjectsAccessible" }, required = false, description = "Assume that all objects are known to the validator.") 
    boolean allObjectsAccessible;
    
//    @Option(names = { "--log" }, required = false, description = "log messages to file.") 
//    boolean log;
    
    @Option(names = { "--export" }, required = false, description = "Output directory.") 
    File outputDirectory;
    
    private static final String DATASET_TABLE = "t_ili2db_dataset";

    @Override
    public Integer call() throws Exception {
        
        Config config = new Config();
        new PgMain().initConfig(config);

        config.setConfigReadFromDb(true);
        config.setModels(models);
        
        if (modeldir != null) {
            config.setModeldir(modeldir);
        }

        config.setDbhost(dbhost);
        config.setDbport(dbport);
        config.setDbusr(dbusr);
        config.setDbpwd(dbpwd);
        String dburl = "jdbc:postgresql://"+dbhost+":"+dbport+"/"+dbdatabase;
        config.setDburl(dburl);
        config.setDbschema(dbschema);
        
        if (itf) {
            config.setItfTransferfile(true);
        }
        
        if (disableValidation) {
            config.setValidation(false);
        }
        
        Settings settings = new Settings();
        
        if (allObjectsAccessible) {
            settings.setValue(Validator.SETTING_ALL_OBJECTS_ACCESSIBLE, Validator.TRUE);
        }
                        
        Ili2pgBatcher ili2pgBatcher = new Ili2pgBatcher();
        
        if (outputDirectory != null) {
            ili2pgBatcher.export(config, outputDirectory.toPath(), settings);            
        }
        
        
        
        
        
        // datasets erst ganz am Schluss in csv file schreiben.
        // Dann weiss man, ob alles funktioniert hat.
        
        
        
        
        return 0; //true ? 1 : 0;
    }
    
    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

}
