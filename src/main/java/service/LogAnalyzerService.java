package service;

import db.DatabaseManager;
import model.LogResult;
import thread.FileReaderTask;
import util.LogFileUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class LogAnalyzerService {
    private final DatabaseManager db;

    public LogAnalyzerService() {
        this.db = new DatabaseManager();
    }

    /* ============= Phân tích log trong thư mục bằng multithreading ============= */
    public void analyzeLogs(String folderPath) {
        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<LogResult>> futures = new ArrayList<>();

        try {
            // Lấy danh sách các file trong thư mục
            Files.list(Paths.get(folderPath))
                    .filter(Files::isRegularFile)
                    .forEach(p -> futures.add(pool.submit(new thread.FileReaderTask(p))));
        } catch (IOException e) {
            System.err.println("Không đọc được thư mục: " + e.getMessage());
        }

        // Chờ tất cả các tác vụ hoàn thành và thu thập kết quả
        List<LogResult> results = new ArrayList<>();
        for (Future<LogResult> f : futures) {
            try {
                LogResult r = f.get();
                if (r != null) results.add(r);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        pool.shutdown();

        // Lưu batch với Transaction vào bảng log_analysis
        db.saveBatch(results, "log_analysis");
        System.out.println("✅ Đã phân tích " + results.size() + " file và lưu DB (log_analysis).");

        // Ghi toàn bộ kết quả vào file ana_result.txt
        writeResultsToFile(results, "D:\\InternBE\\log-analyzer_p2\\src\\main\\java\\log_result\\ana_result.txt");
    }

    // Ghi kết quả phân tích vào file ana_result.txt
    private void writeResultsToFile(List<LogResult> results, String filePath) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-20s %-12s %-15s %-25s%n", "Filename", "Word Count", "Keyword Count", "Processed At"));
        for (LogResult r : results) {
            sb.append(String.format("%-20s %-12d %-15d %-25s%n",
                    r.getFileName(), r.getWordCount(), r.getKeywordCount(), r.getProcessedAt()));
        }
        try {
            Files.write(Path.of(filePath), sb.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println("📄 Đã ghi kết quả vào file txt");
        } catch (IOException e) {
            System.err.println("Lỗi ghi file txt: " + e.getMessage());
        }
    }

    /* ============= Concurrency: ghi 100 file + DB song song ============= */
    public void write100LogsConcurrently(int N) {

        // Chuẩn bị dữ liệu đồng bộ (dùng chung cho 2 tác vụ)
        List<LogResult> records = new ArrayList<>();
        for (int i = 1; i <= N; i++) {
            String filename = "log_" + i + ".txt";
            int wc = 100 + (int)(Math.random() * 900);
            int kc = 1 + (int)(Math.random() * 10);
            records.add(new LogResult(filename, wc, kc, LocalDateTime.now()));
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);

        Callable<Void> dbTask = () -> {
            db.saveBatch(records, "logs_batch"); // batch insert vào bảng logs_batch
            System.out.println("✅ Đã ghi " + N + " bản ghi vào DB (logs_batch).");
            return null;
        };

        Callable<Void> fileTask = () -> {
            // Transaction cho ghi file: nếu lỗi, xóa toàn bộ file đã ghi trước đó
            List<Path> writtenFiles = new ArrayList<>();
            try {
                for (LogResult r : records) {
                    String content = """
                            Filename: %s
                            Word Count: %d
                            Keyword Count: %d
                            Processed At: %s
                            """.formatted(r.getFileName(), r.getWordCount(), r.getKeywordCount(), r.getProcessedAt());
                    Path filePath = Path.of("logs", r.getFileName());
                    util.LogFileUtil.ensureFolderExists("logs");
                    Files.write(filePath, content.getBytes());
                    writtenFiles.add(filePath);
                }
                System.out.println("📄 Đã ghi " + N + " file vào thư mục ./logs/ (transaction OK)");
            } catch (IOException e) {
                // Rollback: xóa toàn bộ file đã ghi trước đó
                for (Path p : writtenFiles) {
                    try { Files.deleteIfExists(p); } catch (IOException ex) { /* ignore */ }
                }
                System.err.println("Lỗi ghi file, đã rollback toàn bộ file đã ghi trước đó: " + e.getMessage());
            }
            return null;
        };

        try {
            pool.invokeAll(List.of(dbTask, fileTask)); // chạy 2 tác vụ song song
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            pool.shutdown();
        }
    }


    /* ============= Concurrency: xem dữ liệu từ 2 bảng song song ============= */
    public void showAllConcurrentlyFromTwoTables() {
        db.showAllConcurrentlyFromTwoTables();
    }

    /* ============= CRUD tiện dụng gọi từ CLI ============= */
    public void addLog(String filename, int wordCount, int keywordCount) { db.addLog(filename, wordCount, keywordCount); }
    public void showAll() { db.showAll(); }
    public void updateLog(int id, String filename, int wordCount, int keywordCount) { db.updateLog(id, filename, wordCount, keywordCount); }
    public void deleteLog(int id) { db.deleteLog(id); }

    // Phân tích log_all.csv bằng FixedThreadPool, ghi tổng hợp ra file
    public void analyzeLargeLogWithThreadPool(String path) {
        // Đặt đường dẫn file log cần phân tích
        path = "D:\\InternBE\\log-analyzer_p2\\src\\main\\resources\\logs\\log_all.csv";
        List<String> lines;
        try {
            // Đọc toàn bộ nội dung file vào danh sách dòng
            lines = Files.readAllLines(Paths.get(path));
        } catch (IOException e) {
            // Nếu có lỗi khi đọc file, in ra lỗi và kết thúc hàm
            System.err.println("Không đọc được file: " + e.getMessage());
            return;
        }
        // Kiểm tra file rỗng
        if (lines.isEmpty()) {
            System.out.println("File rỗng.");
            return;
        }

        // Nếu có header thì bỏ qua dòng đầu
        int startIdx = lines.get(0).toLowerCase().contains("timestamp") ? 1 : 0;
        // Lấy danh sách dòng dữ liệu (bỏ qua header nếu có)
        List<String> dataLines = lines.subList(startIdx, lines.size());

        // Khởi tạo biến tổng số từ và tổng số keyword
        int totalWordCount = 0;
        int totalKeywordCount = 0;

        // Tạo thread pool với số luồng bằng số CPU
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        // Chia dữ liệu thành các batch cho từng luồng
        int batchSize = (int) Math.ceil((double) dataLines.size() / numThreads);
        List<Future<int[]>> futures = new ArrayList<>();

        // Tạo và gửi task cho mỗi batch
        for (int i = 0; i < numThreads; i++) {
            int from = i * batchSize;
            int to = Math.min(from + batchSize, dataLines.size());
            if (from >= to) break;
            List<String> batch = dataLines.subList(from, to);

            // Tạo task cho mỗi batch: trả về mảng gồm tổng số từ và tổng số keyword của batch
            Callable<int[]> task = () -> {
                int wc = 0, kc = 0;
                for (String line : batch) {
                    wc += line.trim().isEmpty() ? 0 : line.trim().split("\\s+").length; // Đếm số từ
                    kc += line.split("(?i)error", -1).length - 1; // Đếm số lần xuất hiện "error"
                }
                return new int[]{wc, kc};
            };
            futures.add(pool.submit(task));
        }

        // Thu thập kết quả từ các luồng và cộng dồn vào tổng
        for (Future<int[]> f : futures) {
            try {
                int[] res = f.get();
                totalWordCount += res[0];
                totalKeywordCount += res[1];
            } catch (InterruptedException | ExecutionException e) {
                // Nếu có lỗi khi lấy kết quả, in ra lỗi
                e.printStackTrace();
            }
        }

        // Đóng thread pool
        pool.shutdown();
        try {
            // Chờ tối đa 60 giây để tất cả các tác vụ hoàn thành
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
        }

        // Tạo kết quả tổng hợp và ghi ra file ana_result.txt
        List<LogResult> result = List.of(
            new LogResult("log_all.csv", totalWordCount, totalKeywordCount, java.time.LocalDateTime.now())
        );
        writeResultsToFile(result, "D:\\InternBE\\log-analyzer_p2\\src\\main\\java\\log_result\\log_result_fixedThreadPool.txt");
    }

    // Phân tích log_all.csv bằng ForkJoinPool, ghi tổng hợp ra file
    public void analyzeLargeLogWithForkJoin(String path) {
        // Đặt đường dẫn file log cần phân tích
        path = "D:\\InternBE\\log-analyzer_p2\\src\\main\\resources\\logs\\log_all.csv";
        List<String> lines;
        try {
            // Đọc toàn bộ nội dung file vào danh sách dòng
            lines = Files.readAllLines(Paths.get(path));
        } catch (IOException e) {
            // Nếu có lỗi khi đọc file, in ra lỗi và kết thúc hàm
            System.err.println("Không đọc được file: " + e.getMessage());
            return;
        }
        // Kiểm tra file rỗng
        if (lines.isEmpty()) {
            System.out.println("File rỗng.");
            return;
        }

        // Nếu có header thì bỏ qua dòng đầu
        int startIdx = lines.get(0).toLowerCase().contains("timestamp") ? 1 : 0;
        List<String> dataLines = lines.subList(startIdx, lines.size());

        // Sử dụng ForkJoinPool để phân tích
        ForkJoinPool pool = new ForkJoinPool();
        LogAnalyzeForkTask task = new LogAnalyzeForkTask(dataLines, 0, dataLines.size());
        int[] resultArr = pool.invoke(task); // Kết quả: [tổng số từ, tổng số keyword]
        pool.shutdown();

        int totalWordCount = resultArr[0];
        int totalKeywordCount = resultArr[1];

        // Tạo kết quả tổng hợp và ghi ra file ana_result.txt
        List<LogResult> result = List.of(
            new LogResult("log_all.csv", totalWordCount, totalKeywordCount, java.time.LocalDateTime.now())
        );
        writeResultsToFile(result, "D:\\InternBE\\log-analyzer_p2\\src\\main\\java\\log_result\\log_result_forkJoin.txt");
    }

    // Task cho ForkJoinPool: phân tích một đoạn của danh sách dòng
    private static class LogAnalyzeForkTask extends RecursiveTask<int[]> {
        private static final int THRESHOLD = 500; // ngưỡng chia nhỏ
        private final List<String> lines;
        private final int start, end;

        LogAnalyzeForkTask(List<String> lines, int start, int end) {
            this.lines = lines;
            this.start = start;
            this.end = end;
        }

        @Override
        protected int[] compute() {
            // Nếu số dòng nhỏ hơn ngưỡng, xử lý trực tiếp
            if (end - start <= THRESHOLD) {
                int wc = 0, kc = 0;
                for (int i = start; i < end; i++) {
                    String line = lines.get(i);
                    wc += line.trim().isEmpty() ? 0 : line.trim().split("\\s+").length; // Đếm số từ
                    kc += line.split("(?i)error", -1).length - 1; // Đếm số lần xuất hiện "error"
                }
                return new int[]{wc, kc};
            } else {
                // Nếu số dòng lớn, chia đôi và xử lý song song
                int mid = (start + end) / 2;
                LogAnalyzeForkTask left = new LogAnalyzeForkTask(lines, start, mid);
                LogAnalyzeForkTask right = new LogAnalyzeForkTask(lines, mid, end);
                left.fork(); // chạy nhánh trái song song
                int[] rightRes = right.compute(); // xử lý nhánh phải
                int[] leftRes = left.join(); // lấy kết quả nhánh trái
                // Cộng kết quả hai nhánh
                return new int[]{leftRes[0] + rightRes[0], leftRes[1] + rightRes[1]};
            }
        }
    }
}
