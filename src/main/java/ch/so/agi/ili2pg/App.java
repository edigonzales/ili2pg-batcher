package ch.so.agi.ili2pg;

import java.io.File;
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

import com.google.common.collect.Maps;

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

    @Option(names = { "--itf" }, required = false, description = "INTERLIS 1 data") 
    boolean itf;
    
    @Option(names = { "--export" }, required = true, description = "Output directory.") 
    File outputDirectory;
    
    private static final String DATASET_TABLE = "t_ili2db_dataset";

    @Override
    public Integer call() throws Exception {
        
        Config config = new Config();
        new PgMain().initConfig(config);

        config.setConfigReadFromDb(true);
        config.setModels(models);

        config.setDbhost(dbhost);
        config.setDbport(dbport);
        config.setDbusr(dbusr);
        config.setDbpwd(dbpwd);
        String dburl = "jdbc:postgresql://"+dbhost+":"+dbport+"/"+dbdatabase;
        config.setDburl(dburl);
        config.setDbschema(dbschema);
        
        System.out.println(dburl);
        String fileExtension = itf ? "itf" : "xtf";
        
        
        Ili2pgBatcher ili2pgBatcher = new Ili2pgBatcher();
        ili2pgBatcher.export(config, outputDirectory.toPath());
        
//        List<Map<String,String>> datasets = new ArrayList<>();
//        try(Connection con = DriverManager.getConnection(dburl, "XXXXX", "YYYYY"); Statement stmt = con.createStatement();) {
//           try(ResultSet rs = stmt.executeQuery("SELECT datasetname FROM " + dbschema + "." + DATASET_TABLE);) {
//              while(rs.next()) {
//                 String datasetName = rs.getString("datasetname");
//                 System.out.print(datasetName+", ");
//                 
//                 String fileName = outputDirectory.toPath().resolve(Paths.get(datasetName + "." + fileExtension)).toAbsolutePath().toString();
//                 System.out.print(fileName+", ");
//                 Map<String,String> dataset = new HashMap<>();
//                 dataset.put(datasetName, fileName);
//                 datasets.add(dataset);
//                 
//                 
//                 System.out.println();
//              }
//           } catch (SQLException e) {
//              e.printStackTrace();
//           }
//        } catch (SQLException e) {
//              e.printStackTrace();
//        }
        
        
        
        
        // datasets erst ganz am Schluss in csv file schreiben.
        // Dann weiss man, ob alles funktioniert hat.
        
        
        
        // TODO: if/else logic ??
        
//        if (initSiteXml) {
//            InputStream is = this.getClass().getClassLoader().getResourceAsStream("ilisite.xml");
//            File file = Paths.get(modelsDir.getAbsolutePath(), new File("ilisite.xml").getName()).toFile();
//            Files.copy(is, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
//        }
//
//        var failed = false;
//        if (!server) {
//            failed = new ListModels().listModels(modelsDir);
//            if (failed) return 1; 
//        }
//        
//        if (server) {
//            new Httpd(modelsDir.getAbsolutePath()).start();
//            
//            // see https://community.oracle.com/tech/developers/discussion/1541952/java-application-not-terminating-immediatly-after-ctrl-c
//            final Console console = System.console();
//            String line;
//            do {
//                System.out.println("Ctrl-C to stop server...");
//                line = console.readLine();
//            } while (line == null);
//        }
        
        return 0; //true ? 1 : 0;
    }
    
    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }

}
