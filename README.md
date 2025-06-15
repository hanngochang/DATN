

---

# Hệ thống Lakehouse Toàn diện (Comprehensive Lakehouse System)

## 1. Giới thiệu

Dự án này xây dựng một nền tảng dữ liệu hiện đại theo kiến trúc Lakehouse, sử dụng hoàn toàn các công cụ mã nguồn mở. Hệ thống có khả năng thu thập dữ liệu từ web (cụ thể là các bài báo từ VnExpress), xử lý, lưu trữ, và cung cấp khả năng phân tích, trực quan hóa dữ liệu một cách hiệu quả.

**Mục tiêu chính:**

* **End-to-End Pipeline:** Xây dựng một quy trình dữ liệu hoàn chỉnh từ khâu thu thập (Ingestion), làm sạch (Cleansing), chuyển đổi (Transformation) đến khi sẵn sàng cho phân tích (Analytics-ready).
* **Kiến trúc Lakehouse:** Kết hợp những ưu điểm của Data Lake (lưu trữ linh hoạt, chi phí thấp) và Data Warehouse (quản lý giao dịch, chất lượng dữ liệu) bằng cách sử dụng **Apache Iceberg** làm định dạng bảng và **Project Nessie** để quản lý phiên bản dữ liệu.
* **Hệ sinh thái mã nguồn mở:** Tận dụng sức mạnh của các công cụ hàng đầu như Apache Spark, Apache Airflow, Trino, MinIO, và Selenium.
* **Tự động hóa & Giám sát:** Tự động hóa quy trình bằng Airflow và giám sát toàn bộ hệ thống bằng Prometheus & Grafana.

---

## 2. Sơ đồ Kiến trúc

Sơ đồ dưới đây mô tả luồng hoạt động và sự tương tác giữa các thành phần trong hệ thống.

![Sơ đồ kiến trúc hệ thống Lakehouse](images/architect.png)



---

## 3. Các Thành phần Công nghệ

| Lĩnh vực | Công cụ | Phiên bản | Vai trò trong dự án |
| :--- | :--- | :--- | :--- |
| **Lưu trữ (Storage)** | **MinIO** | `RELEASE.2024-07-31T09-57-19Z` | Cung cấp S3-compatible object storage để làm Data Lake, lưu trữ dữ liệu ở các tầng Raw, Clean, và Curated. |
| **Định dạng bảng (Table Format)** | **Apache Iceberg** | `1.9.0` | Quản lý cấu trúc dữ liệu dưới dạng bảng trên Data Lake, hỗ trợ các tính năng ACID transactions, time travel, schema evolution. |
| **Catalog & Versioning** | **Project Nessie** | `0.104.1` | Cung cấp catalog và "Git-for-Data", cho phép thực hiện các thao tác commit, branch, merge trên dữ liệu, đảm bảo tính nhất quán. |
| **Xử lý Dữ liệu (Processing)** | **Apache Spark** | `3.5.5` | Engine chính cho các tác vụ ETL (Extract, Transform, Load) từ Raw -> Clean và Clean -> Curated. |
| **Điều phối (Orchestration)** | **Apache Airflow** | `3.0.1` | Lập lịch và tự động hóa các pipeline dữ liệu, từ việc trigger crawl dữ liệu đến chạy các job Spark ETL. |
| **Thu thập Dữ liệu (Ingestion)** | **Selenium Grid** | `4.32.0` | Cụm Chrome node để thực hiện crawl dữ liệu từ trang web VnExpress một cách song song và ổn định. |
| **Query Engine** | **Trino (PrestoSQL)** | `475` | Cho phép thực hiện các truy vấn SQL tương tác (ad-hoc) với hiệu năng cao trực tiếp trên các bảng Iceberg trong Data Lake. |
| **Trực quan hóa (BI)** | **Metabase** | `v0.55.x` | Công cụ Business Intelligence để xây dựng dashboard, biểu đồ, trực quan hóa dữ liệu từ tầng Curated thông qua Trino. |
| **Giám sát (Monitoring)** | **Prometheus, Grafana** | `v3.3.0`, `11.4.4` | Thu thập, lưu trữ và hiển thị các chỉ số (metrics) về hiệu năng, tài nguyên của các container và các job Spark ETL. |
| **Containerization** | **Docker, Docker Compose** | - | Đóng gói và quản lý toàn bộ các dịch vụ của hệ thống, đảm bảo môi trường nhất quán và dễ dàng triển khai. |

---

## 4. Luồng Dữ liệu (Data Flow)

Luồng dữ liệu trong hệ thống của chúng tôi được tổ chức thành các giai đoạn chính sau:

### 4.1. Lưu trữ Dữ liệu Gốc (Staging Area)

* Dữ liệu thô, sau khi được thu thập từ các nguồn khác nhau (ví dụ: quá trình crawl), sẽ được lưu trữ ban đầu trong một cơ sở dữ liệu **MySQL**.
* Một database chuyên dụng, thường được gọi là `staging`, được sử dụng làm khu vực lưu trữ tạm thời cho dữ liệu gốc, chờ đợi các bước xử lý tiếp theo.

### 4.2. Quá trình ETL và Cập nhật Data Mart

* **Tự động hóa hàng ngày:** Quá trình ETL (Extract, Transform, Load) được thực hiện hàng ngày để chuyển đổi và cập nhật dữ liệu từ khu vực `staging` vào các **Data Mart**.
* **Điều phối bởi Airflow:** Việc này được điều phối và tự động hóa thông qua các **DAG (Directed Acyclic Graphs) của Apache Airflow**.
* **Xử lý bằng Procedure:** DAG trong Airflow sẽ chạy theo thứ tự các procedure đã sắp xếp sẵn (hàng ngày) để thực thi một chuỗi các **procedure xử lý** đã được định nghĩa trong cơ sở dữ liệu. Các procedure này chịu trách nhiệm:
    * **Extract:** Trích xuất dữ liệu cần thiết từ database `staging`.
    * **Transform:** Thực hiện làm sạch, chuẩn hóa, tổng hợp và biến đổi dữ liệu theo các quy tắc nghiệp vụ đã xác định.
    * **Load:** Tải dữ liệu đã qua xử lý vào các bảng phù hợp trong Data Mart, sẵn sàng cho việc phân tích.

### 4.3. Phân tích và Trực quan hóa (Analytics & BI) với Power BI

* **Công cụ BI chính:** **Power BI** được sử dụng làm công cụ Business Intelligence (BI) hàng đầu để tạo ra các phân tích và báo cáo chuyên sâu.
* **Kết nối OLAP:** Power BI kết nối trực tiếp với các mô hình dữ liệu **OLAP (Online Analytical Processing)** đã được thiết kế trong kho dữ liệu (Data Warehouse/Data Mart).
* **Refresh dữ liệu:** Một **Power BI Gateway** được triển khai để quản lý lịch trình refresh dữ liệu hàng ngày. Điều này đảm bảo rằng các báo cáo và dashboard luôn được cập nhật với thông tin mới nhất từ Data Mart.
* **Xuất bản báo cáo:** Sau khi quá trình refresh hoàn tất, các báo cáo và dashboard được phát triển trên Power BI Desktop sẽ được đẩy lên **Power BI Service**. Tại đây, chúng có thể được xuất bản thành các liên kết công khai hoặc được chia sẻ nội bộ.

### 4.4. Hiển thị Báo cáo trên Website

* **Nhúng Báo cáo:** Các liên kết báo cáo và dashboard đã được xuất bản từ Power BI Service sẽ được **nhúng (embed)** trực tiếp vào giao diện người dùng của website.
* **Trải nghiệm người dùng:** Việc này cho phép người dùng cuối truy cập và xem các báo cáo trực quan, tương tác cao mà không cần rời khỏi giao diện web của ứng dụng, mang lại trải nghiệm người dùng liền mạch và hiệu quả.

---

---

## 5. Cấu trúc Thư mục

```
src/
├── Apache_Spark/         # Cấu hình và ứng dụng Spark
│   ├── apps/             # Các kịch bản Python cho Spark (ETL, tạo model)
│   ├── docker_client/    # Dockerfile và requirements cho image Spark client (dùng trong Airflow)
│   └── spark.yml         # File docker-compose cho cụm Spark
├── MinIO/                # Cấu hình MinIO
├── Monitoring/           # Cấu hình cho Prometheus, Grafana, Alertmanager
├── Nessie/               # Cấu hình cho Nessie và DB backend (Postgres)
├── Trino/                # Cấu hình cho Trino (coordinator, worker, catalogs)
├── Web_scraping/         # Mã nguồn cho việc thu thập dữ liệu
│   ├── CrawlJob/         # Logic chính của job crawl
│   ├── CrawlPackage/     # Các hàm tiện ích để crawl (lấy link, nội dung)
│   ├── SeleniumPackage/  # Các hàm tiện ích để điều khiển Selenium
│   └── chrome.yml        # File docker-compose cho Selenium Grid
├── airflow/              # Cấu hình, DAGs và plugins cho Airflow
│   ├── dags/             # Nơi chứa các file định nghĩa DAG
│   └── config/           # Các file cấu hình của Airflow
├── metabase/             # Cấu hình cho Metabase
├── docker-compose-main.yml # File compose chính, định nghĩa network chung
└── .env.example          # File mẫu cho các biến môi trường
```

---

## 6. Hướng dẫn Cài đặt & Chạy

### 6.1. Yêu cầu
* Docker
* Docker Compose
* Hệ điều hành Linux (khuyến nghị, để dễ dàng quản lý `uid` của user)
* Git

### 6.2. Cấu hình Môi trường

1.  **Clone repository:**
    ```bash
    git clone <your-repo-url>
    cd lakehouse-project
    ```

2.  **Tạo file `.env`:**
    Sao chép file `.env.example` thành `.env` và tùy chỉnh các giá trị bên trong.
    ```bash
    cp .env.example .env
    ```
    **QUAN TRỌNG:** Trên Linux, hãy chạy lệnh sau để `AIRFLOW_UID` được gán đúng với user của bạn, tránh lỗi permission.
    ```bash
    echo "AIRFLOW_UID=$(id -u)" >> .env
    ```
    Hãy kiểm tra và đảm bảo các mật khẩu, access key trong file `.env` được đặt một cách an toàn.

### 6.3. Khởi chạy Hệ thống

Sử dụng `docker-compose` để khởi chạy toàn bộ các dịch vụ. Lệnh này sẽ đọc file `docker-compose-main.yml` và tất cả các file `*.yml` bên trong các thư mục con của `src`.

```bash
docker-compose \
  -f src/docker-compose-main.yml \
  -f src/MinIO/MinIO.yml \
  -f src/Nessie/nessie.yml \
  -f src/Apache_Spark/spark.yml \
  -f src/Trino/trino.yml \
  -f src/Web_scraping/chrome.yml \
  -f src/airflow/airflow.yml \
  -f src/Monitoring/monitoring.yml \
  -f src/metabase/metabase.yml \
  up -d
```

Để dừng hệ thống:
```bash
# (Chạy lệnh tương tự với 'down' thay vì 'up -d')
docker-compose ... down
```

### 6.4. Thiết lập Ban đầu (First-time setup)

Sau khi các container đã chạy, bạn cần thực hiện một số bước thiết lập ban đầu.

1.  **Tạo Bucket trên MinIO:**
    * Truy cập MinIO Console: `http://localhost:9001`
    * Đăng nhập với `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` trong file `.env`.
    * Tạo các buckets sau:
        * `raw-news-lakehouse`
        * `clean-news-lakehouse`
        * `curated-news-lakehouse`

2.  **Thiết lập Airflow:**
    * Truy cập Airflow UI: `http://localhost:8888`
    * Đăng nhập với user/password bạn đã cấu hình trong `.env` (`_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD`).
    * Vào **Admin -> Connections**, tạo connection cho MinIO:
        * **Conn Id:** `my_lakehouse_conn`
        * **Conn Type:** `Amazon S3`
        * **AWS Access Key ID:** (nhập access key từ file `.env`)
        * **AWS Secret Access Key:** (nhập secret key từ file `.env`)
        * **Extra:** `{"host": "http://minio1:9000"}`
    * Vào **Admin -> Pools**, tạo một pool:
        * **Pool:** `selenium_pool`
        * **Slots:** `3` (hoặc bằng số lượng chrome-node bạn chạy)

3.  **Tạo Schema cho Lakehouse và Bảng `dim_date`:**
    Chạy các script Spark sau thông qua `docker exec`.

    ```bash
    # Lấy ID của container spark-master
    SPARK_MASTER_ID=$(docker ps -qf "name=spark-master")

    # Chạy script tạo schema cho Clean Zone
    docker exec -it $SPARK_MASTER_ID /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark/apps/jars/hadoop-aws-3.3.4.jar,/opt/spark/apps/jars/aws-java-sdk-bundle-1.12.783.jar,/opt/spark/apps/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar,/opt/spark/apps/jars/nessie-spark-extensions-3.5_2.12-0.103.5.jar /opt/spark/apps/clean_data_model.py

    # Chạy script tạo schema cho Curated Zone
    docker exec -it $SPARK_MASTER_ID /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark/apps/jars/hadoop-aws-3.3.4.jar,/opt/spark/apps/jars/aws-java-sdk-bundle-1.12.783.jar,/opt/spark/apps/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar,/opt/spark/apps/jars/nessie-spark-extensions-3.5_2.12-0.103.5.jar /opt/spark/apps/curated_data_model.py

    # Chạy script điền dữ liệu cho bảng dim_date
    docker exec -it $SPARK_MASTER_ID /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark/apps/jars/hadoop-aws-3.3.4.jar,/opt/spark/apps/jars/aws-java-sdk-bundle-1.12.783.jar,/opt/spark/apps/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar,/opt/spark/apps/jars/nessie-spark-extensions-3.5_2.12-0.103.5.jar /opt/spark/apps/dim_date.py
    ```

### 6.5. Chạy Pipeline

1.  Truy cập Airflow UI (`http://localhost:8888`).
2.  Tìm DAG `vnexpress_daily_crawl_range_day_dag`.
3.  Bật (Un-pause) DAG.
4.  Trigger DAG thủ công bằng nút "Play", bạn có thể tùy chỉnh ngày bắt đầu/kết thúc trong mục "Config". Nếu không, DAG sẽ chạy theo lịch (`0 1 * * *` - 1 giờ sáng hàng ngày) để crawl dữ liệu 7 ngày gần nhất.
5.  Sau khi DAG crawl chạy xong và thành công, các DAG ETL `ETL_raw_zone_to_clean_zone` và `ETL_clean_zone_to_curated_zone` sẽ tự động được trigger.

---

## 7. Truy cập các Giao diện

| Dịch vụ | URL | Ghi chú |
| :--- | :--- | :--- |
| **Airflow** | `http://localhost:8888` | Giao diện điều phối, quản lý DAGs. |
| **MinIO Console** | `http://localhost:9001` | Quản lý buckets và objects trong Data Lake. |
| **Spark Master** | `http://localhost:8080` | Giao diện quản lý cụm Spark. |
| **Spark Worker 1** | `http://localhost:8081` | |
| **Spark Worker 2** | `http://localhost:8082` | |
| **Nessie UI** | `http://localhost:19120` | Xem các commit, branch trên catalog dữ liệu. |
| **Trino UI** | `http://localhost:8181` | Theo dõi các truy vấn, trạng thái cụm Trino. |
| **Metabase** | `http://localhost:3001` | Xây dựng và xem dashboards. |
| **Grafana** | `http://localhost:3000` | (User/Pass: `NguyenPhucLinh`/`NguyenPhucLinh`) - Dashboards giám sát. |
| **Prometheus** | `http://localhost:9090` | Xem metrics và cấu hình scrape. |
| **Alertmanager** | `http://localhost:9093` | Quản lý các cảnh báo. |
| **Pushgateway** | `http://localhost:9091` | Xem các metrics được đẩy từ batch jobs. |
| **Selenium Grid** | `http://localhost:4444` | Xem trạng thái của hub và các node Chrome. |

---

## 8. Giám sát Hệ thống

* **Grafana** (`http://localhost:3000`) là nơi tập trung để giám sát.
* **cAdvisor** thu thập metrics về tài nguyên (CPU, RAM, Network) của tất cả các container và được Prometheus scrape.
* Các job **Spark ETL** được lập trình để đẩy các metrics tùy chỉnh (thời gian chạy, số bản ghi xử lý, trạng thái thành công/thất bại) lên **Pushgateway**.
* **Prometheus** thu thập tất cả các metrics này và **Grafana** sẽ hiển thị chúng trên các dashboard, giúp theo dõi sức khỏe và hiệu năng của toàn bộ hệ thống.

---
## 9. Tác giả

* **Tên:** Nguyễn Phúc Linh
* **Email:** nguyenphuclinh1208@gmail.com
