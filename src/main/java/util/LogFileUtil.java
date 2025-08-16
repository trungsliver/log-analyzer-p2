package util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LogFileUtil {
    // Tạo thư mục mới nếu không tồn tại
    public static void ensureFolderExists(String folder) throws IOException {
        Path p = Paths.get(folder);
        if (!Files.exists(p)) Files.createDirectories(p);
    }

    // Đọc nội dung của file
    public static void writeFile(String folder, String filename, String content) throws IOException {
        ensureFolderExists(folder);
        Files.write(Paths.get(folder, filename), content.getBytes());
    }
}
