package ru.malygin.sort;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.*;
import ru.malygin.Main;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MergeFile {

    private static final Comparator<Integer> integerComparator = Integer::compareTo;
    private static final Comparator<String> stringComparator = String::compareTo;
    private static final Logger logger = LogManager.getLogger();
    private static final Marker dataError = MarkerManager.getMarker("data-error");
    private static final Random random = new Random();

    @Setter
    private static String tmpDirectory = "tmp/";
    private static final File tmpDir = new File(tmpDirectory);
    @Setter
    private static int filesCountThreshold = 16_384;
    @Setter
    private static int maxTmpFileCount = 1024;
    @Getter
    private static String outputFile = Math.abs(random.nextLong()) + "_out.txt";
    @Setter
    private static DataType dataType = DataType.STRING;
    @Setter
    private static boolean descending = false;

    static {
        try {
            Files.createDirectories(Path.of(tmpDirectory));
        } catch (IOException e) {
            e.printStackTrace();
            logger.log(Level.ERROR, e.getMessage());
        }
    }

    public static void setOutputFile(String outputFile) throws IOException {
        File file = new File(outputFile);
        if (file.exists() && file.canWrite()) {
            MergeFile.outputFile = outputFile;
        } else {
            Files.createDirectories(Path.of(file.getParent()));
            Files.createFile(Path.of(file.getAbsolutePath()));
            if (file.exists() && file.canWrite()) {
                MergeFile.outputFile = outputFile;
            } else {
            System.err.println(
                    outputFile + " (files not found, using default output file " + MergeFile.getOutputFile() + ")");}

        }
    }

    public static String merge(List<String> filePaths) throws IOException {
        int filesCount = filePaths.size();
        if (filesCount <= filesCountThreshold) {
            return mergeFiles(filePaths, outputFile);
        }

        List<String> blockOfFile = new ArrayList<>();
        int blocksCount = filesCount / filesCountThreshold
                + (filesCount % filesCountThreshold == 0 ? 0 : 1);
        for (int i = 0; i < blocksCount; i++)
        {
            blockOfFile.add(
                    mergeBlockOfFile(
                            filePaths.subList((i * filesCountThreshold),
                                    Math.min(((i + 1) * filesCountThreshold), filesCount))));
        }
        return  merge(blockOfFile);
    }

    public static String presortAndMerge(List<String> filePaths) throws IOException {
        List<String> resultList = new ArrayList<>();

        int threadsCount = Runtime.getRuntime().availableProcessors() - 1;
        ExecutorService service = Executors.newFixedThreadPool(threadsCount);

        List<Future<String>> futures = new ArrayList<>();
        for (String filePath : filePaths) {
            futures.add(CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return checkSort(filePath);
                        } catch (IOException e) {
                            e.printStackTrace();
                            logger.log(Level.ERROR, e.getMessage());
                            return filePath;
                        }
                    },
                    service
            ));
        }
        for (Future<String> future : futures) {
            try {
                resultList.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                logger.log(Level.ERROR, e.getMessage());
            }
        }
        service.shutdown();
        return merge(resultList);
    }

    private static String mergeBlockOfFile(List<String> filePaths) throws IOException {
        File tmpFile = File.createTempFile("mergeBlock-", "-file", tmpDir);
        tmpFile.deleteOnExit();
        return mergeFiles(filePaths, tmpFile.toString());
    }

    private static String mergeFiles(List<String> filePaths, String outputFile) {
        List<InputStack> isl = new ArrayList<>();

        for (String file : filePaths) {
            try{
                isl.add(new InputStack(getBufferedReader(file)));
            } catch (IOException e) {
                System.err.println(e.getMessage());
                logger.log(Level.ERROR, e.getMessage());
            }
        }

        try {
            mergeSort(getBufferedWriter(outputFile), isl);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            logger.log(Level.ERROR, e.getMessage());
        }
        return outputFile;
    }

    private static void mergeSort(BufferedWriter bw, List<InputStack> isl) throws IOException {
        PriorityQueue<InputStack> pq = new PriorityQueue<>((i, j) -> {
            int cmp;
            if (dataType.equals(DataType.INTEGER)) {
                try {
                    cmp = integerComparator.compare(Integer.parseInt(i.peek()), Integer.parseInt(j.peek()));
                } catch (NumberFormatException e) {
                    cmp = stringComparator.compare(i.peek(), j.peek());
                    logger.log(Level.INFO, dataError, e.getMessage());
                }
            } else {
                cmp = stringComparator.compare(i.peek(), j.peek());
            }
            return descending ? -cmp : cmp;
        });
        isl.forEach(item -> {
            if(!item.empty()) {
                pq.add(item);
            }
        });
        try (bw) {
            while (pq.size() > 0) {
                InputStack inputStack = pq.poll();
                String result = inputStack.pop();
                bw.write(result);
                bw.newLine();
                if (inputStack.empty()) {
                    inputStack.close();
                } else {
                    pq.add(inputStack);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.log(Level.ERROR, e.getMessage());
        } finally {
            for (InputStack item : pq) {
                item.close();
            }
        }
    }

    private static String checkSort(String filePath) throws IOException {
        try{
            BufferedReader br = getBufferedReader(filePath);
            String current = br.readLine();
            String previous = current;
            while (current != null) {
                if (compare(current, previous) >= 0) {
                    previous = current;
                    current = br.readLine();
                } else {
                    br.close();
                    return externalSort(filePath);
                }
            }
            br.close();
            return filePath;
        } catch (IOException e) {
            System.err.println(e.getMessage());
            logger.log(Level.ERROR, e.getMessage());
            return filePath;
        }
    }

    private static int compare(String st1, String st2) {
        int cmp;
        if (dataType.equals(DataType.INTEGER)) {
            try {
                cmp = Integer.compare(Integer.parseInt(st1), Integer.parseInt(st2));
            } catch (NumberFormatException e) {
                cmp = st1.compareTo(st2);
                logger.log(Level.INFO, dataError, e.getMessage());
            }
        } else {
            cmp = st1.compareTo(st2);
        }
        return descending ? -cmp : cmp;
    }

    private static String externalSort(String filePath) throws IOException {
        List<String> filePaths = new ArrayList<>();

        long blockSize = estimateBestSizeOfBlocks(new File(filePath).length());

        List<String> tmpList = new ArrayList<>();
        String line = "";
        try {
            BufferedReader br = getBufferedReader(filePath);
            while (line != null) {
                long currentBlockSize = 0;
                while ((currentBlockSize < blockSize) && ((line = br.readLine()) != null)) {
                    tmpList.add(line);
                    currentBlockSize += StringSizeCalculator.estimatedSizeOf(line);
                }
                filePaths.add(sortAndSave(tmpList));
                tmpList.clear();
            }
            br.close();
        } catch (IOException e) {
            if (tmpList.size() > 0) {
                filePaths.add(sortAndSave(tmpList));
                tmpList.clear();
            }
            System.err.println(e.getMessage());
            logger.log(Level.ERROR, e.getMessage());
        }
        File newTmpFile = File.createTempFile("sortedMerged-", "-file", tmpDir);
        newTmpFile.deleteOnExit();
        return mergeFiles(filePaths, newTmpFile.toString());
    }

    private static String sortAndSave(List<String> linesList) throws IOException {
        linesList = linesList.parallelStream().sorted((i, j) -> {
            int cmp;
            if (dataType.equals(DataType.INTEGER)) {
                try {
                    cmp = integerComparator.compare(Integer.parseInt(i), Integer.parseInt(j));
                } catch (NumberFormatException e) {
                    cmp = stringComparator.compare(i, j);
                    logger.log(Level.INFO, dataError, e.getMessage());
                }
            } else {
                cmp = stringComparator.compare(i, j);
            }
            return descending ? -cmp : cmp;
        }).collect(Collectors.toCollection(ArrayList<String>::new));

        File newTmpFile = File.createTempFile("sorted-", "-file", tmpDir);
        newTmpFile.deleteOnExit();
        try {
            BufferedWriter fbw = getBufferedWriter(newTmpFile.toString());
            for (String r : linesList) {
                fbw.write(r);
                fbw.newLine();
            }
            fbw.close();
        } catch (IOException e) {
            System.err.println(e.getMessage());
            logger.log(Level.ERROR, e.getMessage());
        }
        return newTmpFile.toString();
    }

    private static BufferedReader getBufferedReader(String filePath) throws IOException {
        return new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(filePath), StandardCharsets.UTF_8));

    }

    private static BufferedWriter getBufferedWriter(String filePath) throws IOException {
        return new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(filePath),StandardCharsets.UTF_8));
    }

    private static long estimateBestSizeOfBlocks(long sizeOfFile) {
        long maxMemory = estimateAvailableMemory();
        long blockSize = sizeOfFile / maxTmpFileCount
                + (sizeOfFile % maxTmpFileCount == 0 ? 0 : 1);

        if (blockSize < maxMemory / 2) {
            blockSize = maxMemory / 2;
        }
        return blockSize;
    }

    private static long estimateAvailableMemory() {
        Runtime r = Runtime.getRuntime();
        long allocatedMemory = r.totalMemory() - r.freeMemory();
        return r.maxMemory() - allocatedMemory;
    }
}
