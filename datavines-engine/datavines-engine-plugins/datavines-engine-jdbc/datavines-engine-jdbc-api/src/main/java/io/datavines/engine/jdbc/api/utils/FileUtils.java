package io.datavines.engine.jdbc.api.utils;

import io.datavines.engine.jdbc.api.entity.QueryColumn;
import io.datavines.engine.jdbc.api.entity.ResultListWithColumns;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class FileUtils {

    public static void writeToLocal(ResultListWithColumns resultListWithColumns, String directory, String name,boolean needHeader) {
        //首先判断文件夹是否存在

        BufferedWriter bw = null;
        try {

            File localErrorDir = new File(directory);

            if (!localErrorDir.exists()){
                org.apache.commons.io.FileUtils.forceMkdir(localErrorDir);
            }

            bw = new BufferedWriter(new FileWriter(directory + File.separator + name +".csv",true));

            if (resultListWithColumns != null && CollectionUtils.isNotEmpty(resultListWithColumns.getResultList())) {
                List<QueryColumn> columns = resultListWithColumns.getColumns();
                List<String> headerList = new ArrayList<>();
                columns.forEach(header -> {
                    headerList.add(header.getName()+"@@"+header.getType());
                });
                if (needHeader) {
                    bw.write(String.join("\001",headerList));
                    bw.newLine();
                }

                for(Map<String, Object> row: resultListWithColumns.getResultList()) {
                    List<String> rowDataList = new ArrayList<>();
                    headerList.forEach(header -> {
                        rowDataList.add((String.valueOf(row.get(header.split("@@")[0]))));
                    });
                    bw.write(String.join("\001",rowDataList));
                    bw.newLine();
                }
                bw.flush();
                bw.close();
            }
        } catch (IOException e) {
            log.error("write data error {}", e);
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException ioe) {
                log.error("close buffer writer error {}", ioe);
            }
        } finally {
            try {
                if (bw != null) {
                    bw.close();
                }
            } catch (IOException ioe) {
                log.error("close buffer writer error {}", ioe);
            }
        }
    }

    public static List<String> readPartFileContent(String filePath,
                                             int skipLine,
                                             int limit){
        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
            return stream.skip(skipLine).limit(limit).collect(Collectors.toList());
        } catch (IOException e) {
            log.error("read file error",e);
        }
        return Collections.emptyList();
    }
}
