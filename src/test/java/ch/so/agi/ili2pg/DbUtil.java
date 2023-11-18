package ch.so.agi.ili2pg;

import java.io.File;
import java.util.Properties;

import org.postgresql.PGProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import ch.ehi.ili2db.base.Ili2db;
import ch.ehi.ili2db.base.Ili2dbException;
import ch.ehi.ili2db.gui.Config;
import ch.ehi.ili2pg.PgMain;

import org.apache.commons.io.FilenameUtils;


public class DbUtil {
    private static Logger log = LoggerFactory.getLogger(Ili2pgBatcher.class);

    public static final String PG_CON_DDLUSER = "postgres";
    public static final String PG_CON_DDLPASS = "secret";
    
    private static final String MODEL_DIR = "src/test/data/";
    
    public static void importXtf(PostgreSQLContainer<?> postgres, String dbSchema, String modelName, File fileName, boolean doSchemaImport) throws Ili2dbException {
        Config settings = new Config();
        new PgMain().initConfig(settings);
        settings.setFunction(Config.FC_IMPORT);
        settings.setModels(modelName);
        settings.setModeldir(MODEL_DIR);
        settings.setDoImplicitSchemaImport(doSchemaImport);
        settings.setValidation(false);
        settings.setJsonTrafo(Config.JSON_TRAFO_COALESCE);
        //settings.setDeleteMode(Config.DELETE_DATA);
        settings.setBatchSize(5000);
        settings.setBasketHandling(Config.BASKET_HANDLING_READWRITE);
        settings.setCreateDatasetCols(Config.CREATE_DATASET_COL);
        settings.setDatasetName(FilenameUtils.getBaseName(fileName.getName()));
       
        Properties props = org.postgresql.Driver.parseURL(postgres.getJdbcUrl(), null);

        settings.setDbhost(props.getProperty(PGProperty.PG_HOST.getName()));
        settings.setDbport(props.getProperty(PGProperty.PG_PORT.getName()));
        settings.setDbdatabase(props.getProperty(PGProperty.PG_DBNAME.getName()));
        settings.setDbschema(dbSchema);
        settings.setDbusr(postgres.getUsername());
        settings.setDbpwd(postgres.getPassword());
        settings.setDburl(postgres.getJdbcUrl());
        settings.setItfTransferfile(false);
        settings.setXtffile(fileName.getAbsolutePath());
        
        Ili2db.run(settings, null);
    }
}
