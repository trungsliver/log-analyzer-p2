package db;

import model.LogResult;
import util.DbUtil;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DatabaseManager {
    public DatabaseManager() {
        createTableIfNotExists();
    }

    // Tạo bảng log_analysis và logs_batch nếu chưa tồn tại
    private void createTableIfNotExists() {
        String createMain = """
            CREATE TABLE IF NOT EXISTS log_analysis (
              id INT AUTO_INCREMENT PRIMARY KEY,
              filename VARCHAR(255) NOT NULL,
              word_count INT NOT NULL,
              keyword_count INT NOT NULL,
              processed_at DATETIME NOT NULL
            )
            """;
        String createBatch = """
            CREATE TABLE IF NOT EXISTS logs_batch (
              id INT AUTO_INCREMENT PRIMARY KEY,
              filename VARCHAR(255) NOT NULL,
              word_count INT NOT NULL,
              keyword_count INT NOT NULL,
              processed_at DATETIME NOT NULL
            )
            """;
        try (Connection c = DbUtil.getConnection(); Statement s = c.createStatement()) {
            s.execute(createMain);
            s.execute(createBatch);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // ================== CRUD =================
    public void addLog(String filename, int word_count, int keyword_count) {
        String sql = "INSERT INTO log_analysis(filename, word_count, keyword_count, processed_at) VALUES (?, ?, ?, ?)";
        try (Connection connection = DbUtil.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, filename);
            ps.setInt(2, word_count);
            ps.setInt(3, keyword_count);
            ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            ps.executeUpdate();
            System.out.println("Đã thêm bản ghi log: " + filename);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void showAll() {
        String sql = "SELECT * FROM log_analysis";
        try (Connection connection = DbUtil.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sql)) {
            // In ra tiêu đề bảng
            printHeader(false);
            // Duyệt qua kết quả và in từng dòng
            while (rs.next()){
                printRow(rs, null);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void updateLog(int id, String filename, int wordCount, int keywordCount) {
        String sql = "UPDATE log_analysis SET filename=?, word_count=?, keyword_count=?, processed_at=? WHERE id=?";
        try (Connection c = DbUtil.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, filename);
            ps.setInt(2, wordCount);
            ps.setInt(3, keywordCount);
            ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()));
            ps.setInt(5, id);
            int rows = ps.executeUpdate();
            if (rows > 0){
                System.out.println("Bản ghi ID " + id + " đã được cập nhật thành công.");
            } else {
                System.out.println("Không tìm thấy bản ghi với ID: " + id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void deleteLog(int id) {
        String sql = "DELETE FROM log_analysis WHERE id=?";
        try (Connection c = DbUtil.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setInt(1, id);
            int rows = ps.executeUpdate();
            if (rows > 0) {
                System.out.println("Bản ghi ID " + id + " đã được xóa thành công.");
            } else {
                System.out.println("Không tìm thấy bản ghi với ID: " + id);
            }
        } catch (SQLException e) { e.printStackTrace(); }
    }


    /* ===================== Batch + Transaction ===================== */
    public void saveBatch(List<LogResult> results, String table) {
        String sql = "INSERT INTO " + table + " (filename, word_count, keyword_count, processed_at) VALUES (?,?,?,?)";
        try (Connection c = DbUtil.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            c.setAutoCommit(false);
            for (LogResult r : results) {
                ps.setString(1, r.getFileName());
                ps.setInt(2, r.getWordCount());
                ps.setInt(3, r.getKeywordCount());
                ps.setTimestamp(4, Timestamp.valueOf(r.getProcessedAt()));
                ps.addBatch();
            }
            ps.executeBatch();
            c.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /* ===================== Concurrency: đọc 2 bảng song song ===================== */
    public void showAllConcurrentlyFromTwoTables() {
        // Sử dụng ExecutorService để đọc song song từ hai bảng
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        Callable<List<String[]>> readMain = () -> readTable("log_analysis", "log-analysis");
        Callable<List<String[]>> readBatch = () -> readTable("logs_batch", "logs_batch");

        try {
            // Future là một đối tượng đại diện cho kết quả của một tác vụ bất đồng bộ
            // Đọc song song từ hai bảng và kết hợp kết quả vào 1 danh sách duy nhất
            Future<List<String[]>> f1 = executorService.submit(readMain);
            Future<List<String[]>> f2 = executorService.submit(readBatch);
            List<String[]> all = new ArrayList<>();
            all.addAll(f1.get());
            all.addAll(f2.get());

            System.out.printf("%-5s %-20s %-12s %-15s %-23s %-12s%n",
                    "ID", "Filename", "Word Count", "Keyword Count", "Processed At", "Source");
            for (String[] r : all) {
                System.out.printf("%-5s %-20s %-12s %-15s %-23s %-12s%n",
                        r[0], r[1], r[2], r[3], r[4], r[5]);
            }
        }catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }

    // Phương thức đọc từ bảng và trả về danh sách các bản ghi
    private List<String[]> readTable(String table, String source) throws SQLException {
        String sql = "SELECT id, filename, word_count, keyword_count, processed_at FROM " + table + " ORDER BY id";
        try (Connection c = DbUtil.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<String[]> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new String[]{
                        String.valueOf(rs.getInt("id")),
                        rs.getString("filename"),
                        String.valueOf(rs.getInt("word_count")),
                        String.valueOf(rs.getInt("keyword_count")),
                        String.valueOf(rs.getTimestamp("processed_at")),
                        source
                });
            }
            return out;
        }
    }

    /* ===================== Helpers ===================== */

    private void printHeader(boolean withSource) {
        if (withSource) {
            System.out.printf("%-5s %-20s %-12s %-15s %-23s %-12s%n",
                    "ID", "Filename", "Word Count", "Keyword Count", "Processed At", "Source");
        } else {
            System.out.printf("%-5s %-20s %-12s %-15s %-23s%n",
                    "ID", "Filename", "Word Count", "Keyword Count", "Processed At");
        }
    }

    private void printRow(ResultSet rs, String source) throws SQLException {
        if (source == null) {
            System.out.printf("%-5d %-20s %-12d %-15d %-23s%n",
                    rs.getInt("id"),
                    rs.getString("filename"),
                    rs.getInt("word_count"),
                    rs.getInt("keyword_count"),
                    rs.getTimestamp("processed_at"));
        } else {
            System.out.printf("%-5d %-20s %-12d %-15d %-23s %-12s%n",
                    rs.getInt("id"),
                    rs.getString("filename"),
                    rs.getInt("word_count"),
                    rs.getInt("keyword_count"),
                    rs.getTimestamp("processed_at"),
                    source);
        }
    }
}
