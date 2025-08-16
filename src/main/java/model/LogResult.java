package model;

import java.time.LocalDateTime;

public class LogResult {
    private final String fileName;
    private final int wordCount;
    private final int keywordCount;
    private final LocalDateTime processedAt;

    public LogResult(String fileName, int wordCount, int keywordCount, LocalDateTime processedAt) {
        this.fileName = fileName;
        this.wordCount = wordCount;
        this.keywordCount = keywordCount;
        this.processedAt = processedAt;
    }

    public String getFileName() {
        return fileName;
    }

    public int getWordCount() {
        return wordCount;
    }

    public int getKeywordCount() {
        return keywordCount;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }
}
