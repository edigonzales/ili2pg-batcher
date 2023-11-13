package ch.so.agi.ili2pg;

import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.ehi.ili2db.gui.Config;

public class Ili2pgBatcher {
    
    private static Logger log = LoggerFactory.getLogger(Ili2pgBatcher.class);

    public void export(Config config, Path outputDirectory) {
        System.out.println(config.toString());
    }
}
