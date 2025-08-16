package service;

import db.DatabaseManager;
import model.LogResult;
import thread.FileReaderTask;
import util.LogFileUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
                    .forEach(p -> futures.add(pool.submit(new FileReaderTask(p))));
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
            for (LogResult r : records) {
                String content = """
                        Filename: %s
                        Word Count: %d
                        Keyword Count: %d
                        Processed At: %s
                        """.formatted(r.getFileName(), r.getWordCount(), r.getKeywordCount(), r.getProcessedAt());
                try {
                    LogFileUtil.writeFile("logs", r.getFileName(), content);
                } catch (IOException e) {
                    System.err.println("Lỗi ghi file " + r.getFileName() + ": " + e.getMessage());
                }
            }
            System.out.println("📄 Đã ghi " + N + " file vào thư mục ./logs/");
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
}
