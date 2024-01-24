package tesis.msc.dh.Log;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * @author Abraham Castillo
 */
public class LogReaderMongoDB extends LogReader implements Serializable {

    public LogReaderMongoDB(String address) {
        super(address);
    }

    /*

    @Override
    public List<String> determineOperations(File[] archivos, String database, String table) throws Exception {

        List<String> lines = new ArrayList<>();
        //List<String> lines = Files.lines(Paths.get(patch)).

        for (int i = 0; i < archivos.length; i++) {
            lines.addAll(IOUtils.readLines(archivos[i].getAbsoluteFile()., "UTF-8"));
        }

        return lines.stream().filter(l -> l.contains(database + "." + table))
                .map(l -> {
                    return l.replaceAll("\"", "");
                })
                .collect(Collectors.toList());
    }
/*/
    @Override
    public List<String> determineOperations02(String path, String database, String table) throws Exception {
        List<String> lines = Files.lines(Paths.get(path)).collect(Collectors.toList());
        List<String> obtainedLines = null;

        obtainedLines = lines.stream().collect(Collectors.toList());

        return obtainedLines.stream().filter(l -> l.contains(database + "." + table))
                .map(l -> {
                    return l.replaceAll("\"", "");
                })
                .collect(Collectors.toList());

    }

}
