import service.LogAnalyzerService;

import java.util.Scanner;

public class App {
    public static void main(String[] args) {
        LogAnalyzerService service = new LogAnalyzerService();
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println("\n===== LOG ANALYZER CLI =====");
            System.out.println("1. Phân tích log từ thư mục (multithreading) -> lưu vào log_analysis");
            System.out.println("2. Xem tất cả (concurrent) từ 2 bảng log_analysis + logs_batch");
            System.out.println("3. Thêm bản ghi (log_analysis)");
            System.out.println("4. Cập nhật bản ghi (log_analysis)");
            System.out.println("5. Xóa bản ghi (log_analysis)");
            System.out.println("6. Ghi nhiều log (concurrency): DB (logs_batch) + file ./logs/");
            System.out.println("7. Phân tích log (log_all.csv) bằng fixedThreadPool");
            System.out.println("8. Phân tích log (log_all.csv) bằng ForkJoin");
            System.out.println("0. Thoát");
            System.out.print("Chọn: ");

            String choice = sc.nextLine().trim();
            switch (choice) {
                case "1" -> {
                    System.out.print("Nhập đường dẫn thư mục log (VD: src/main/resources/logs): ");
                    String path = sc.nextLine().trim();
                    service.analyzeLogs(path);
                }
                case "2" -> service.showAllConcurrentlyFromTwoTables();
                case "3" -> {
                    System.out.print("Filename: ");
                    String fn = sc.nextLine().trim();
                    System.out.print("Word count: ");
                    int wc = Integer.parseInt(sc.nextLine().trim());
                    System.out.print("Keyword count: ");
                    int kc = Integer.parseInt(sc.nextLine().trim());
                    service.addLog(fn, wc, kc);
                }
                case "4" -> {
                    System.out.print("ID cần cập nhật: ");
                    int id = Integer.parseInt(sc.nextLine().trim());
                    System.out.print("Filename mới: ");
                    String fn = sc.nextLine().trim();
                    System.out.print("Word count mới: ");
                    int wc = Integer.parseInt(sc.nextLine().trim());
                    System.out.print("Keyword count mới: ");
                    int kc = Integer.parseInt(sc.nextLine().trim());
                    service.updateLog(id, fn, wc, kc);
                }
                case "5" -> {
                    System.out.print("ID cần xóa: ");
                    int id = Integer.parseInt(sc.nextLine().trim());
                    service.deleteLog(id);
                }
                case "6" -> {
                    System.out.print("Số log muốn ghi: ");
                    int n = Integer.parseInt(sc.nextLine().trim());
                    service.write100LogsConcurrently(n);
                }
                case "7" -> {
                    // fixedThreadPool dùng trong trường hợp file log < 1M lines
                    System.out.println("Phân tích log (log_all.csv) bằng fixedThreadPool:");
                    service.analyzeLargeLogWithThreadPool("fixed");
                }
                case "8" -> {
                    //  forkJoin dùng trong trường hợp file log > 1M lines
                    System.out.println("Phân tích log (log_all.csv) bằng ForkJoin:");
                    service.analyzeLargeLogWithForkJoin("forkjoin");
                }
                case "0" -> {
                    System.out.println("Bye!");
                    return;
                }
                default -> System.out.println("Lựa chọn không hợp lệ!");
            }
        }
    }
}