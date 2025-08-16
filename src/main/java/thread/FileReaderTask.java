package thread;

import model.LogResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.Callable;

/**
 * Lớp này thực hiện việc đọc nội dung của một file log, đếm số từ và số lần xuất hiện của từ khóa "error".
 * Kết quả được trả về dưới dạng đối tượng LogResult.
 */
public class FileReaderTask implements Callable<LogResult> {
    private final Path filePath;

    public FileReaderTask(Path filePath) {
        this.filePath = filePath;
    }

    @Override
    public LogResult call() {
        try {
            String content = Files.readString(filePath);
            int wordCount = content.trim().isEmpty() ? 0 : content.trim().split("\\s+").length;
            int keywordCount = content.split("(?i)error", -1).length - 1; // đếm "error" không phân biệt hoa thường
            return new LogResult(filePath.getFileName().toString(), wordCount, keywordCount, LocalDateTime.now());
        } catch (IOException e) {
            System.err.println("Lỗi đọc file: " + filePath + " -> " + e.getMessage());
            return null;
        }
    }
}