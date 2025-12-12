## 💻 Hướng Dẫn Thiết Lập và Quy Trình Làm Việc

---

### 1. Thiết Lập Dự Án (Clone/Setup)

Thực hiện các bước sau để thiết lập môi trường lần đầu:

- **Vào thư mục dự án:**
  ```bash
  cd data-engineer-course
  ```
- **Sao chép file môi trường:**
  ```bash
  cp .env.example .env
  ```
- **Chỉnh sửa file môi trường (Tùy chọn):**
  ```bash
  nano .env
  ```

---

### 2. Khởi Động Dịch Vụ (Start Everything)

Bạn có hai tùy chọn để khởi động các dịch vụ:

- **Tùy chọn 1: Khởi động nhanh cho Tuần 1 (Week 1):**
  ```bash
  make week1
  ```
- **Tùy chọn 2: Các bước thủ công:**
  1.  **Thiết lập:**
      ```bash
      make setup
      ```
  2.  **Khởi động dịch vụ:**
      ```bash
      make start
      ```
  3.  **Đợi các dịch vụ sẵn sàng:**
      ```bash
      sleep 10 # Wait for services to be ready
      ```
  4.  **Tạo dữ liệu:**
      ```bash
      make generate-data
      ```

---

### 3. Xác Minh (Verify)

Sau khi khởi động, hãy kiểm tra để đảm bảo mọi thứ đang hoạt động:

- **Kiểm tra trạng thái dịch vụ:**
  ```bash
  make ps
  ```
- **Kiểm tra kết nối cơ sở dữ liệu:**
  ```bash
  make test-connection
  ```
- **Xem logs dịch vụ:**
  ```bash
  make logs
  ```

---

### 4. Thông Tin Truy Cập Dịch Vụ

| Dịch Vụ         | Địa Chỉ               | Chi Tiết Đăng Nhập           |
| :-------------- | :-------------------- | :--------------------------- |
| **PgAdmin**     | http://localhost:5050 | Email: `admin@dataeng.local` |
|                 |                       | Password: `admin123`         |
| **Jupyter Lab** | http://localhost:8888 | Token: `dataengineer`        |
| **PostgreSQL**  | Host: `localhost`     | Database: `data_practice`    |
|                 | Port: `5432`          | User: `dataengineer`         |
|                 |                       | Password: `dataengineer123`  |

---

### 5. Quy Trình Làm Việc Hàng Ngày

Thực hiện theo quy trình này cho công việc hàng ngày của bạn:

#### 🌤️ Buổi Sáng: Bắt đầu làm việc

- **Khởi động các dịch vụ:**
  ```bash
  make start
  ```
- **Vào thư mục bài tập và mở code:**
  ```bash
  cd week-01-02-sql-python/exercises/day-01-02-window-functions
  code my_solutions.sql
  ```
- **Kiểm tra kết quả:**
  > Test trong **PgAdmin** hoặc **Jupyter Lab**.

#### 🌙 Buổi Chiều/Tối: Kết thúc và Sao lưu

- **Dừng các dịch vụ:**
  ```bash
  make stop
  ```
- **Sao lưu công việc của bạn (Git):**
  ```bash
  git add .
  git commit -m "Completed Day 1 exercises"
  ```
