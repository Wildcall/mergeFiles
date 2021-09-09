package ru.malygin;

import ru.malygin.sort.DataType;
import ru.malygin.sort.MergeFile;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static void displayHelp() {
        System.out.println("Usage: java -jar sort-it [-options] outputFile inputFile(s)");
        System.out.println("where options include:");
        System.out.println("(required) -s or -i:\tto (s)tring or (i)nteger data in input files");
        System.out.println("(optional) -a or -d:\tsort in (a)scending or (d)escending order, default usage - ascending order");
        System.out.println("(optional) -p:      \tuse pre-sorting of invalid data, default usage - pass invalid data");
        System.out.println("(optional) -h:      \tdisplay this message");
    }

    private static String outputFilePath = "";
    private static boolean descending = false;
    private static boolean presort = false;
    private static DataType dataType;
    private static final List<String> filesPath = new ArrayList<>();

    public static void main(String[] args) {

        if (args.length < 3) {
            displayHelp();
        } else {
            for (String arg : args) {
                switch (arg) {
                    case ("-s") -> dataType = DataType.STRING;
                    case ("-i") -> dataType = DataType.INTEGER;
                    case ("-a") -> descending = false;
                    case ("-d") -> descending = true;
                    case ("-p") -> presort = true;
                    case ("-h") -> displayHelp();
                    default -> {
                        if (outputFilePath.isEmpty()) {
                            outputFilePath = arg;
                            break;
                        }
                        filesPath.add(arg);
                    }
                }
            }
            if (dataType == null) {
                System.out.println("No option -s or -i specified, use -h for help");
            } else if (outputFilePath.equals("")) {
                System.out.println("No output file specified, use -h for help");
            } else if (filesPath.isEmpty()) {
                System.out.println("No input file specified, use -h for help");
            } else {
                try {
                    MergeFile.setOutputFile(outputFilePath);
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                }
                MergeFile.setDescending(descending);
                MergeFile.setDataType(dataType);
                MergeFile.setFilesCountThreshold(1024);
                try {
                    String result = presort ? MergeFile.presortAndMerge(filesPath) : MergeFile.merge(filesPath);
                    System.out.println("Output file - " + result);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Press any button to close...\n");
        new Scanner(System.in).nextLine();
    }
}
