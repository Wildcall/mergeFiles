package ru.malygin;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.malygin.sort.DataType;
import ru.malygin.sort.MergeFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

public class MergeFileTest {

    // Generate options
    private static final Random random = new Random();

    // Sort options
    private static final String inputDir = "data/input/";
    private static final String outputDir = "data/output/";

    @BeforeClass
    public static void beforeClass() {
        try {
            Files.createDirectories(Path.of(inputDir));
            Files.createDirectories(Path.of(outputDir));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @ParameterizedTest
    @MethodSource("variationsParamsForTest")
    public void mergeTest(int filesCount, int maxItemsInFile, DataType dataType, boolean descending, boolean preSort, boolean generate, boolean sorted) throws IOException {

        // File path options
        final String testDesc = dataType.toString() + "_" +
                (descending ? "descending" : "ascending") + "_" +
                (preSort ? "presort" : "skip") + "_" +
                (generate ? "generate" : "preset") + "_" +
                (sorted ? "sorted" : "unsorted");
        final String currentInputDir = inputDir + testDesc + "/";
        final String outputFile = outputDir + testDesc + ".txt";

        // Setup MergeFile
        MergeFile.setOutputFile(outputFile);
        MergeFile.setDescending(descending);
        MergeFile.setDataType(dataType);

        long start;
        if (generate) {
            start = System.currentTimeMillis();
            generateInputFile(currentInputDir, filesCount, maxItemsInFile, dataType, descending, sorted);
            System.out.println("Генерация файлов - " + (System.currentTimeMillis() - start) + " мс.");
        }

        start = System.currentTimeMillis();
        List<String> filePaths = new ArrayList<>(Files.list(Path.of(currentInputDir)).map(Path::toString).toList());
        System.out.println("Составление списка файлов - " + (System.currentTimeMillis() - start) + " мс.");

        if (preSort) {
            MergeFile.presortAndMerge(filePaths);
        } else {
            MergeFile.merge(filePaths);
        }
        System.out.println("Слияние - " + (System.currentTimeMillis() - start) + " мс.");

        Assertions.assertTrue(checkSort(outputFile, descending, dataType));
        Assertions.assertEquals((long) filesCount * maxItemsInFile, checkCount(outputFile));
    }

    private static Stream<Arguments> variationsParamsForTest() {
        return Stream.of(
                // generate sorted data
                //           Count  FileSize    DataType           descending  preSort     generate    sorted
                Arguments.of(10,    100_000,    DataType.INTEGER,  false,      false,      true,       true),
                Arguments.of(10,    100_000,    DataType.INTEGER,  true,       false,      true,       true),
                Arguments.of(10,    100_000,    DataType.STRING,   false,      false,      true,       true),
                Arguments.of(10,    100_000,    DataType.STRING,   true,       false,      true,       true),

                // generate sorted data and presort
                //           Count  FileSize    DataType           descending  preSort     generate    sorted
                Arguments.of(10,    100_000,    DataType.INTEGER,  false,      true,       true,       true),
                Arguments.of(10,    100_000,    DataType.INTEGER,  true,       true,       true,       true),
                Arguments.of(10,    100_000,    DataType.STRING,   false,      true,       true,       true),
                Arguments.of(10,    100_000,    DataType.STRING,   true,       true,       true,       true),

                // generate unsorted data and presort
                //           Count  FileSize    DataType           descending  preSort     generate    sorted
                Arguments.of(10,    100_000,    DataType.STRING,   false,      true,       true,       false),
                Arguments.of(10,    100_000,    DataType.STRING,   true,       true,       true,       false),
                Arguments.of(10,    100_000,    DataType.INTEGER,  false,      true,       true,       false),
                Arguments.of(10,    100_000,    DataType.INTEGER,  true,       true,       true,       false)
        );
    }

    @ParameterizedTest
    @MethodSource("variationsParamsForBigDataTest")
    public void mergeBigSizeFile(int filesCount, int maxItemsInFile, boolean descending) throws IOException {

        // File path options
        final String testDesc = "bigSize_" + DataType.INTEGER +
                (descending ? "_descending" : "_ascending");
        final String currentInputDir = inputDir + testDesc + "/";
        final String outputFile = outputDir + testDesc + ".txt";

        // Setup MergeFile
        MergeFile.setOutputFile(outputFile);
        MergeFile.setDescending(descending);
        MergeFile.setDataType(DataType.INTEGER);


        long start = System.currentTimeMillis();
        generateInputFile(currentInputDir, filesCount, maxItemsInFile, DataType.INTEGER, descending, true);
        System.out.println("Генерация файлов - " + (System.currentTimeMillis() - start) + " мс.");

        start = System.currentTimeMillis();
        List<String> filePaths = new ArrayList<>(Files.list(Path.of(currentInputDir)).map(Path::toString).toList());
        System.out.println("Составление списка файлов - " + (System.currentTimeMillis() - start) + " мс.");

        MergeFile.merge(filePaths);
        System.out.println("Слияние - " + (System.currentTimeMillis() - start) + " мс.");

        Assertions.assertTrue(checkSort(outputFile, descending, DataType.INTEGER));
        Assertions.assertEquals((long) filesCount * maxItemsInFile, checkCount(outputFile));
    }

    private static Stream<Arguments> variationsParamsForBigDataTest() {
        return Stream.of(
                // generate sorted INTEGER data
                //                             descending
                Arguments.of(2, 100_000_000,   false),
                Arguments.of(2, 100_000_000,   true)
                );
    }

    @Test
    public void wrongInputPath() throws IOException {

        // Generate options
        final int filesCount = 5;
        final int maxItemsInFile = 10;

        // File path options
        final String testDesc = "wrong_input_path";
        final String currentInputDir = inputDir + testDesc + "/";
        final String outputFile = outputDir + testDesc + ".txt";

        // Setup MergeFile
        MergeFile.setOutputFile(outputFile);
        MergeFile.setDescending(false);
        MergeFile.setDataType(DataType.INTEGER);


        long start = System.currentTimeMillis();
        generateInputFile(currentInputDir, filesCount, maxItemsInFile, DataType.INTEGER, false, true);
        System.out.println("Генерация файлов - " + (System.currentTimeMillis() - start) + " мс.");

        start = System.currentTimeMillis();
        List<String> filePaths = new ArrayList<>(Files.list(Path.of(currentInputDir)).map(Path::toString).toList());
        // Add wrong path
        filePaths.add(currentInputDir + "input5.txt");
        System.out.println("Составление списка файлов - " + (System.currentTimeMillis() - start) + " мс.");

        MergeFile.merge(filePaths);
        System.out.println("Слияние - " + (System.currentTimeMillis() - start) + " мс.");

        Assertions.assertTrue(checkSort(outputFile, false, DataType.INTEGER));
        Assertions.assertEquals((long) filesCount * maxItemsInFile, checkCount(outputFile));
    }

    @Test
    public void wrongOutputPath() throws IOException {

        // Generate options
        final int filesCount = 5;
        final int maxItemsInFile = 10;

        // File path options
        final String testDesc = "wrong_output_path";
        final String currentInputDir = inputDir + testDesc + "/";
        // Create wrong output file path
        final String outputFile = outputDir + "wrongPath/" + testDesc + ".txt";

        // Setup MergeFile
        try {
            MergeFile.setOutputFile(outputFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        MergeFile.setDescending(false);
        MergeFile.setDataType(DataType.INTEGER);


        long start = System.currentTimeMillis();
        generateInputFile(currentInputDir, filesCount, maxItemsInFile, DataType.INTEGER, false, true);
        System.out.println("Генерация файлов - " + (System.currentTimeMillis() - start) + " мс.");

        start = System.currentTimeMillis();
        List<String> filePaths = new ArrayList<>(Files.list(Path.of(currentInputDir)).map(Path::toString).toList());
        System.out.println("Составление списка файлов - " + (System.currentTimeMillis() - start) + " мс.");

        MergeFile.merge(filePaths);
        System.out.println("Слияние - " + (System.currentTimeMillis() - start) + " мс.");

        Assertions.assertTrue(checkSort(MergeFile.getOutputFile(), false, DataType.INTEGER));
        Assertions.assertEquals((long) filesCount * maxItemsInFile, checkCount(MergeFile.getOutputFile()));
    }

    private static void generateInputFile(String path, int count, int maxSize, DataType dataType, boolean descending, boolean sorted) throws IOException {
        Files.createDirectories(Path.of(path));
        for (int i = 0; i < count; i++) {
            String filePath = path + "input" + i + ".txt";
            BufferedWriter bw = getBufferedWriter(filePath);
            if (dataType.equals(DataType.INTEGER)) {
                if (sorted) {
                    writeSortedIntegerFile(bw, maxSize, descending);
                } else {
                    writeRandomIntegerFile(bw, maxSize);
                }
            } else {
                if (sorted){
                    writeSortedStringFile(bw, maxSize, descending);
                } else {
                    writeRandomStringFile(bw, maxSize);
                }
            }
        }
    }

    private static void writeSortedIntegerFile(BufferedWriter bw, int maxSize, boolean descending) throws IOException {
        int k = 0;
        int size = 0;
        while (size != maxSize) {
            if (random.nextBoolean()) {
                int result = descending ? maxSize * 3 - k : k;
                bw.write(Long.toString(result));
                bw.newLine();
                size++;
            }
            k++;
        }
        bw.close();
    }

    private static void writeRandomIntegerFile(BufferedWriter bw, int maxSize) throws IOException {
        for (int i = 0; i < maxSize; i++) {
            bw.write(Long.toString(random.nextInt(maxSize)));
            bw.newLine();
        }
        bw.close();
    }

    private static void writeSortedStringFile(BufferedWriter bw, int maxSize, boolean descending) throws IOException {
        StringBuilder tmp = new StringBuilder();
        String[] strings = new String[maxSize];
        for(int i = 0; i < maxSize; i++) {
            for (int k = 0; k < random.nextInt(7) + 4; k++){
                tmp.append(Character.toString(random.nextInt(26) + 97));
            }
            strings[i] = tmp.toString();
            tmp.delete(0, tmp.length());
        }

        if (descending) {
            Arrays.parallelSort(strings, Comparator.reverseOrder());
        } else {
            Arrays.parallelSort(strings);
        }

        Arrays.stream(strings).forEach(string -> {
            try {
                bw.write(string);
                bw.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } );
        bw.close();
    }

    private static void writeRandomStringFile(BufferedWriter bw, int maxSize) throws IOException {
        StringBuilder tmp = new StringBuilder();
        for(int i = 0; i < maxSize; i++) {
            for (int k = 0; k < random.nextInt(7) + 4; k++){
                tmp.append(Character.toString(random.nextInt(26) + 97));
            }
            bw.write(tmp.toString());
            bw.newLine();
            tmp.delete(0, tmp.length());
        }
        bw.close();
    }

    private static boolean checkSort(String file, boolean descending, DataType dataType) throws IOException {
        BufferedReader br = getBufferReader(file);

        String current = br.readLine();
        String previous = current;
        while (current != null) {
            if (compare(current, previous, dataType, descending) >= 0) {
                previous = current;
                current = br.readLine();
            } else {
                br.close();
                return false;
            }
        }
        br.close();
        return true;
    }

    private static long checkCount(String file) throws IOException {
        BufferedReader br = getBufferReader(file);
        long count = 0;

        while (br.readLine() != null) {
            count++;
        }
        br.close();
        return count;
    }

    private static int compare(String st1, String st2, DataType dataType, boolean descending) {
        int cmp;
        if (dataType.equals(DataType.INTEGER)) {
            try {
                cmp = Integer.compare(Integer.parseInt(st1), Integer.parseInt(st2));
            } catch (NumberFormatException e) {
                cmp = st1.compareTo(st2);
            }
        } else {
            cmp = st1.compareTo(st2);
        }
        return descending ? -cmp : cmp;
    }

    private static BufferedReader getBufferReader(String filePath) throws IOException {
        return new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(filePath), StandardCharsets.UTF_8));
    }

    private static BufferedWriter getBufferedWriter(String filePath) throws IOException {
        return new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(filePath),StandardCharsets.UTF_8));
    }
}