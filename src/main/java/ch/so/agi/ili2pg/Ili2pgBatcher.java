package ch.so.agi.ili2pg;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

import org.apache.commons.io.FilenameUtils;
import org.interlis2.validator.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVWriter;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.logging.FileListener;
import ch.ehi.basics.settings.Settings;
import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.base.Ili2dbException;
import ch.ehi.ili2db.gui.Config;
import ch.interlis.iox_j.logging.FileLogger;

public class Ili2pgBatcher {
    private static Logger log = LoggerFactory.getLogger(Ili2pgBatcher.class);

    private static final String DATASET_TABLE = "t_ili2db_dataset";
    
    public void export(Config config, Path outputDirectory, Settings settings) throws SQLException, Ili2dbException, IOException {        
        String fileExtension = "xtf";
        if (config.isItfTransferfile()) {
            fileExtension = "itf";
        }
        
        Map<String,String> datasets = new HashMap<>();
        try(Connection con = DriverManager.getConnection(config.getDburl(), config.getDbusr(), config.getDbpwd()); Statement stmt = con.createStatement();) {
            try(ResultSet rs = stmt.executeQuery("SELECT datasetname FROM " + config.getDbschema() + "." + DATASET_TABLE + " ORDER BY datasetname ASC");) {
                while(rs.next()) {
                    String datasetName = rs.getString("datasetname");               
                    String fileName = outputDirectory.resolve(Paths.get(datasetName + "." + fileExtension)).toAbsolutePath().toString();
                    datasets.put(datasetName, fileName);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SQLException(e);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SQLException(e);
        }

        Map<String,Boolean> datasetsExportResults = new HashMap<>();
        Map<String,Boolean> datasetsValidationResults = new HashMap<>();
        
        for (var entry : datasets.entrySet()) {
            String dataset = entry.getKey();
            String fileName = entry.getValue();
            
            // Immer ohne Validierung exportieren und falls gewünscht nachträglich validieren.
            boolean disableValidation = config.isValidation();
            config.setValidation(false);
            
            config.setLogfile(fileName + ".log");
            
            config.setBasketHandling(Config.BASKET_HANDLING_READWRITE);
            config.setDatasetName(dataset);
            config.setXtffile(fileName);
            config.setFunction(Config.FC_EXPORT);
            
            try {
                Ili2db.run(config, null);
                datasetsExportResults.put(dataset, Boolean.TRUE);
            } catch (Ili2dbException e) {
                datasetsExportResults.put(dataset, Boolean.FALSE);
                continue;
            }

            if (!disableValidation) {
                settings.setValue(Validator.SETTING_LOGFILE, fileName + "_validation.log");

                if (config.getModeldir() == null) {
                    String settingIlidirs = Validator.SETTING_DEFAULT_ILIDIRS;
                    settings.setValue(Validator.SETTING_ILIDIRS, settingIlidirs);                        
                } else {
                    settings.setValue(Validator.SETTING_ILIDIRS, config.getModeldir());
                }

                boolean valid = Validator.runValidation(fileName, settings);
                if (valid) {
                    datasetsValidationResults.put(dataset, Boolean.TRUE);
                } else {
                    datasetsValidationResults.put(dataset, Boolean.FALSE);
                }
            }
        }
        
        for (var entry : datasetsExportResults.entrySet()) {
            String dataset = entry.getKey();
            Boolean result = entry.getValue();
            if (!result) {
                System.err.println("Error while exporting dataset: " + dataset);                
            }
        }
        
        for (var entry : datasetsValidationResults.entrySet()) {
            String dataset = entry.getKey();
            Boolean result = entry.getValue();
            if (!result) {
                System.err.println("Error while validating dataset: " + dataset);                
            }
        }
        
        Path csvFile = outputDirectory.resolve("datasets.csv");
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile.toString()))) {
            for (var entry : datasets.entrySet()) {
                String[] entries = new String[] {entry.getKey(), entry.getValue()};
                writer.writeNext(entries);
            }      
        }
    }
}
