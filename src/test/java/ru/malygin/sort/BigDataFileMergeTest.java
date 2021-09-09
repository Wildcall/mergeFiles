package ru.malygin.sort;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BigDataFileMergeTest {

    @Test
    public void testMerge() throws IOException, InterruptedException {

        final String currentInputDir ="D:\\Skillbox\\data";

        //Thread.sleep(10_000);
        List<String> filePaths = new ArrayList<>(Files.list(Path.of(currentInputDir)).map(Path::toString).toList());
        long start = System.currentTimeMillis();
        System.out.println("Всего файлов - " + filePaths.size());
        System.out.println("Время начала - " + start);
        MergeFile.merge(filePaths);
        System.out.println("Время на работу - " + (System.currentTimeMillis() - start));
        Thread.sleep(100_000);
    }
}