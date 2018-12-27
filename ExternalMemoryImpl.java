package edu.hebrew.db.external;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;


public class ExternalMemoryImpl implements IExternalMemory {
    private static final int BlockSize = 32768;
    private static final int NumOfBlocks = 640;
    private static final int columnLength = 20;

    /*
     * Calculates the line size of the given file.
     */
    private int checkLineSize(String in) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(in))) {
            int lineSize = br.readLine().length() * 2;
            br.close();
            return lineSize;
        }
    }

    /*
     * Compares lines in given column number.
     */
    private class ColumnComparator implements Comparator<String> {
        private int columnTocompare;

        ColumnComparator(int columnTocompare) {
            this.columnTocompare = columnTocompare;
        }

        @Override
        public int compare(String s1, String s2) {
            int start = (columnTocompare - 1) * (columnLength + 1);
            return s1.substring(start, start + columnLength).compareTo(s2.substring(start, start + columnLength));
        }
    }

    /*
    Selects lines with given column and substring.
     */
    private class SelectPredicate implements Predicate<String> {
        private int colNum;
        private String substrSelect;

        public SelectPredicate(int colNum, String substrSelect) {
            this.colNum = colNum;
            this.substrSelect = substrSelect;
        }

        @Override
        public boolean test(String s) {
            if (colNum - 1 < s.length()) {
                int start = (colNum - 1) * (columnLength + 1);
                return s.substring(start, start + columnLength).contains(substrSelect);
            }
            return false;
        }
    }

    /*
    Merges the given File array into one file, using the comparator. If number of files < M-1 (i.e. last
    round) then write them to the output file.
     */
    private void merge(File[] tmpFiles, File tmpDir, String out, Comparator<String> comparator, boolean isLastRound) throws IOException {
        ArrayList<String> minFromEachFile = new ArrayList<>();
        ArrayList<BufferedReader> buffersEachFile = new ArrayList<>();
        // read blocksize input from at most M-1 files.
        for (File tmpFile : tmpFiles) {
            BufferedReader br = new BufferedReader(new FileReader(tmpFile), BlockSize);
            String minLine = br.readLine();

            if (minLine != null) {
                minFromEachFile.add(minLine);
                buffersEachFile.add(br);
            }
        }
        // if last round then write to the out file. else write to a temp file.
        BufferedWriter outputWriter;
        if (isLastRound) {
            outputWriter = new BufferedWriter(new FileWriter(out), BlockSize);
        } else {
            File tempFile = File.createTempFile("sortedFile_", null, tmpDir);
            tempFile.deleteOnExit();
            outputWriter = new BufferedWriter(new FileWriter(tempFile), BlockSize);
        }
        // while bufferreaders list isnot empty, keep getting lines in column order.
        while (!buffersEachFile.isEmpty()) {
            String minLine = minFromEachFile.get(0);
            int minFileIndex = 0;
            for (int j = 0; j < minFromEachFile.size(); j++) {
                if (comparator.compare(minLine, minFromEachFile.get(j)) > 0) {    // get min of all files.
                    minLine = minFromEachFile.get(j);
                    minFileIndex = j;
                }
            }
            outputWriter.write(minLine + "\n");
            String newMinLine = buffersEachFile.get(minFileIndex).readLine();
            // if reading from file is done, remove file from list, else read the next line.
            if (newMinLine != null) {
                minFromEachFile.set(minFileIndex, newMinLine);
            } else {
                buffersEachFile.get(minFileIndex).close();
                buffersEachFile.remove(minFileIndex);
                minFromEachFile.remove(minFileIndex);
            }
        }
        // flush left overs.
        outputWriter.flush();
        outputWriter.close();
    }

    /*
    Merges files from the input tmpDir, until only one file is left and it is written to the out, using
    the comparator.
     */
    private void mergeFiles(File tmpDir, String out, Comparator<String> comparator) {
        try {
            // get only files with sortedFile_ prefix.
            File[] tmpFiles = tmpDir.listFiles(x -> x.getName().startsWith("sortedFile_"));
            if (tmpFiles == null) {
                return;
            }
            int readSize;        // indicates how many files to read.
            boolean isLastRound;
            while (tmpFiles.length > 0) {
                // load first lines from files.
                readSize = NumOfBlocks - 1;
                isLastRound = false;
                if (tmpFiles.length < (NumOfBlocks - 1)) {
                    readSize = tmpFiles.length;
                    isLastRound = true;
                }
                // get subarray in the readSize range.
                File[] filesToMerge = Arrays.copyOfRange(tmpFiles, 0, readSize);
                // merge those files to one, and delete all pre-merge files.
                merge(filesToMerge, tmpDir, out, comparator, isLastRound);
                for (File f : filesToMerge) {
                    f.delete();
                }
                // get only files with sortedFile_ prefix.
                tmpFiles = tmpDir.listFiles(x -> x.getName().startsWith("sortedFile_"));
                if (tmpFiles == null) {
                    return;
                }
            }
        } catch (IOException e) {
            System.out.println(e.toString());
        }
    }

    @Override
    public void sort(String in, String out, int colNum, String tmpPath) {
        // default predicate line sent to true.
        sort(in, out, colNum, tmpPath, line -> true);
    }

    /*
    Sorts the given file into out on the given column. Selection is available by providing a predicate.
     */
    private void sort(String in, String out, int colNum, String tmpPath, Predicate<String> predicate) {
        System.out.println(String.format("Starting sort of %s", in));
        try {
            File tmpDirectory = new File(tmpPath);
            File file = new File(in);
            long fileSizeBytes = file.length() * 2;
            int lineSize = checkLineSize(in);
            int bufferSize = BlockSize * NumOfBlocks;
            if (bufferSize > fileSizeBytes) {
                bufferSize = (int) fileSizeBytes;
            }
            int linesPerFile = (int) Math.ceil(((double) bufferSize) / ((double) lineSize));
            boolean readFile = true;
            BufferedReader bufferedReader = new BufferedReader(new FileReader(in), bufferSize);
            ColumnComparator comparator = new ColumnComparator(colNum);
            while (readFile) {
                ArrayList<String> lines = new ArrayList<String>();
                String line = null;
                for (int j = 0; j < linesPerFile && (line = bufferedReader.readLine()) != null; j++) {
                    if (predicate.test(line)) {
                        lines.add(line);
                    }
                }
                if (lines.isEmpty()) {
                    readFile = false;
                    continue;
                }
                lines.sort(comparator); // sort each chunk of lines file separately
                // now write these sorted lines into a tmp file.
                File outFile = File.createTempFile("sortedFile_", null, tmpDirectory);
                outFile.deleteOnExit();
                BufferedWriter printWriter = new BufferedWriter(new FileWriter(outFile));
                for (String l : lines) {
                    printWriter.write(l + "\n");
                }
                printWriter.close();

            }
            bufferedReader.close();
            mergeFiles(tmpDirectory, out, comparator);
            System.out.println("Done.");
        } catch (IOException e) {
            System.out.print(e.toString());
        }
    }

    @Override
    public void select(String in, String out, int colNumSelect, String substrSelect, String tmpPath) {
        try {
            int bufferSize = BlockSize * NumOfBlocks;
            // read buffers in the size of bufferSize.
            BufferedReader br = new BufferedReader(new FileReader(in), bufferSize);
            BufferedWriter outw = new BufferedWriter(new FileWriter(out), bufferSize);
            SelectPredicate selector = new SelectPredicate(colNumSelect, substrSelect);
            String line;
            while ((line = br.readLine()) != null)        // read lines and select matching to substr and col.
            {
                if (selector.test(line)) {
                    outw.write(line);
                }
            }
            br.close();
            outw.close();
        } catch (IOException e) {
            System.out.println(e.toString());
        }
    }

    /*
    Provide inline selecting to the sorting algorithm, instead of the naive linear approach.
     */
    @Override
    public void sortAndSelectEfficiently(String in, String out, int colNumSort,
                                         String tmpPath, int colNumSelect, String substrSelect) {

        SelectPredicate selector = new SelectPredicate(colNumSelect, substrSelect);
        this.sort(in, out, colNumSort, tmpPath, selector);
    }

}