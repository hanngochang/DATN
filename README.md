

---

# Hệ thống Phân tích dữ liệu 

## 1. Giới thiệu

Dự án này tập trung vào việc **xây dựng một hệ thống báo cáo dữ liệu hiện đại**, phục vụ nhu cầu phân tích dữ liệu kinh doanh và vận hành. Hệ thống được thiết kế để tự động hóa toàn bộ quy trình từ khâu xử lý dữ liệu đến việc tạo ra các báo cáo hàng ngày, cung cấp cái nhìn sâu sắc và kịp thời cho các hoạt động nghiệp vụ.

**Hệ thống có khả năng:**

* **Xử lý và chuyển đổi dữ liệu tự động:** Tự động hóa các bước làm sạch, chuẩn hóa và chuyển đổi dữ liệu để đảm bảo chất lượng và tính sẵn sàng cho phân tích.
* **Lưu trữ và quản lý dữ liệu hiệu quả:** Đảm bảo dữ liệu được lưu trữ một cách có tổ chức và dễ dàng truy cập cho các mục đích báo cáo.
* **Cung cấp khả năng phân tích và trực quan hóa mạnh mẽ:** Đảm bảo dữ liệu dễ dàng được truy vấn và biến thành các báo cáo trực quan, dễ hiểu.

**Mục tiêu chính của hệ thống:**

* **Tự động hóa Quy trình Báo cáo:** Xây dựng một quy trình tự động hoàn chỉnh từ khi dữ liệu sẵn sàng để xử lý, cho đến khi dữ liệu sẵn sàng cho phân tích và lên báo cáo hàng ngày.
* **Hỗ trợ Phân tích Kinh doanh & Vận hành:** Cung cấp các báo cáo và dashboard hàng ngày giúp phân tích hiệu suất, nhận diện xu hướng và hỗ trợ ra quyết định.
* **Sử dụng công cụ hiệu quả:** Tận dụng sức mạnh của các công cụ như Airflow, PowerBI, Angular,vv...


---

## 2. Sơ đồ Kiến trúc

Sơ đồ dưới đây mô tả luồng hoạt động và sự tương tác giữa các thành phần trong hệ thống.

![Sơ đồ kiến trúc hệ thống ](images/732850c2f9984ec61789.jpg)



---


## 3. Các Thành phần Công nghệ

| Lĩnh vực | Công cụ | Phiên bản | Vai trò trong dự án |
| :--- | :--- | :--- | :--- |
| **Lưu trữ (Storage)** | **MySQL** | `-` | Lưu trữ dữ liệu gốc (staging) và các Data Mart đã xử lý, là nguồn dữ liệu chính cho các báo cáo. |
| **Web Frontend** | **Angular** | `14.21.3` | Xây dựng giao diện người dùng tương tác, hiển thị các báo cáo và dashboard. |
| **Web Backend** | **.NET 6 (ASP.NET Core)** | `6.x` | Phát triển API backend cung cấp dữ liệu cho frontend và quản lý các logic nghiệp vụ. |
| **Công cụ phát triển Backend** | **Visual Studio** | `-` | Môi trường phát triển tích hợp (IDE) chính cho việc phát triển và gỡ lỗi ứng dụng .NET Core. |
| **Bộ đệm (Caching)** | **Redis** | `5.0.14.1` | Cung cấp cơ chế bộ đệm trong backend để tăng tốc độ truy xuất dữ liệu và giảm tải cho database. |
| **Tự động hóa (Orchestration & Automation)** | **Apache Airflow** | `3.0.1` | Lập lịch và tự động hóa các quy trình ETL hàng ngày để xử lý và cập nhật dữ liệu vào Data Mart. |
| **Tự động hóa Báo cáo (BI Automation)** | **Power BI Gateway** | `-` | Đảm bảo kết nối an toàn và tự động refresh dữ liệu từ Data Mart lên Power BI Service theo lịch trình hàng ngày. |
| **Trực quan hóa & BI (Visualization & BI)** | **Power BI** | `-` | Công cụ Business Intelligence để xây dựng các mô hình phân tích, báo cáo và dashboard trực quan từ Data Mart. |

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
## 5. Cấu trúc Thư mục

```
.
├── Procedure/                  # Chứa các tập lệnh SQL (Stored Procedures) cho cơ sở dữ liệu MySQL
│   ├── data_warehouse_procedures.sql # Các procedure xử lý cho Data Warehouse
│   ├── olap_procedures.sql     # Các procedure liên quan đến xử lý OLAP (nếu có)
│   ├── staging_ivy_moda_it_procedure... # Procedure cho dữ liệu staging Ivy Moda IT
│   └── staging_ivyx_procedures.sql # Procedure cho dữ liệu staging IvyX
├── airflow/                    # Cấu hình và tài nguyên cho Apache Airflow
│   ├── dags/                   # Nơi chứa các file định nghĩa DAGs (.py) để điều phối quá trình ETL
│   ├── .env                    # Các biến môi trường cho Airflow Docker Compose
│   ├── Dockerfile              # Dockerfile để xây dựng image cho Airflow
│   ├── docker-compose.yml      # File Docker Compose để triển khai Airflow
│   └── requirements.txt        # Các thư viện Python yêu cầu cho Airflow
├── source code/                # Thư mục chính chứa mã nguồn của ứng dụng
│   ├── .vs/                    # Thư mục ẩn của Visual Studio
│   ├── backend/                # Mã nguồn của ứng dụng Web Backend (.NET Core API)
│   │   └── App.API/            # Dự án API chính (ví dụ: App.API.sln)
│   │       ├── Controllers/    # Các API Controller
│   │       ├── Models/         # Các Model dữ liệu
│   │       ├── appsettings.json# File cấu hình của Backend
│   │       ├── Program.cs      # Điểm khởi đầu của ứng dụng Backend
│   │       └── ...             # Các file và thư mục khác của dự án .NET
│   ├── frontend/               # Mã nguồn của ứng dụng Web Frontend (Angular)
│   │   ├── src/                # Mã nguồn chính của Angular
│   │   ├── node_modules/       # Các thư viện phụ thuộc của Node.js
│   │   ├── angular.json        # Cấu hình dự án Angular
│   │   ├── package.json        # Danh sách dependencies của Angular
│   │   └── ...                 # Các file và thư mục khác của dự án Angular
└── README.md                   # Tệp README chính của toàn bộ dự án
```


---


## 6. Hướng dẫn Cài đặt & Chạy


### 6.1. Yêu cầu

Đảm bảo bạn đã cài đặt các công cụ sau trên hệ thống của mình:

* **Node.js** (để chạy Angular và npm)
* **.NET SDK 6** (để phát triển và chạy backend .NET Core)
* **MySQL Server** (hệ quản trị cơ sở dữ liệu)
* **Redis Server** (máy chủ bộ đệm)
* **Git** (để clone mã nguồn dự án)
* **Visual Studio** (hoặc Visual Studio Code, để phát triển backend và frontend)

### 6.2. Cấu hình Môi trường

#### 6.2.1. Cài đặt Angular 14.21.3 (qua NVM)

1.  **Tải NVM for Windows:**
    * Truy cập: [Releases · coreybutler/nvm-windows](https://github.com/coreybutler/nvm-windows/releases)
    * Tải xuống file `nvm-setup.zip` và cài đặt theo hướng dẫn.

2.  **Chạy script cài đặt Node (trong PowerShell với quyền Administrator):**
    * Mở **PowerShell với quyền Administrator**.
    * Chạy các lệnh sau:
        ```powershell
        nvm install 14.21.3
        nvm use 14.21.3
        ```
    * Kiểm tra phiên bản Node.js và npm: `node -v` và `npm -v`.

#### 6.2.2. Cài đặt Angular CLI

* Mở Command Prompt hoặc PowerShell (không cần quyền admin).
* Cài đặt Angular CLI phiên bản 12.0.1:
    ```bash
    npm install -g @angular/cli@12.0.1
    ```
    *Lưu ý: Hãy đảm bảo phiên bản Angular CLI này tương thích với phiên bản Angular bạn định sử dụng trong dự án (Angular 14.21.3).*

#### 6.2.3. Cài đặt .NET 6, ASP.NET và Visual Studio

1.  **Cài đặt .NET 6 Runtime (x64):**
    * Truy cập: [Download .NET 6.0](https://dotnet.microsoft.com/en-us/download/dotnet/6.0)
    * Tải xuống và cài đặt **.NET Desktop Runtime 6.0.x (x64)**.

2.  **Cài đặt Visual Studio:**
    * Truy cập: [Download Visual Studio Tools](https://visualstudio.microsoft.com/downloads/)
    * Tải xuống và chạy Visual Studio Installer.
    * Trong Visual Studio Installer, chọn **"Modify"** (nếu đã cài đặt) hoặc "Install" (nếu chưa).
    * Chọn workload **"ASP.NET and web development"**.
    * Nhấn "Install" hoặc "Modify" để hoàn tất.

#### 6.2.4. Cài đặt Redis

* Truy cập: [Releases · tporadowski/redis](https://github.com/tporadowski/redis/releases)
* Tải xuống phiên bản cài đặt (ví dụ: `.msi`) và cài đặt Redis Server.
* Đảm bảo dịch vụ Redis đang chạy sau khi cài đặt.

#### 6.2.5. Cài đặt Git

* Truy cập: [Git - Downloads](https://git-scm.com/downloads)
* Tải xuống và cài đặt Git cho hệ điều hành của bạn.

#### 6.2.6. Clone Repository

1.  **Lấy URL Clone:** Lấy URL clone HTTP từ kho lưu trữ GitLab của bạn (ví dụ: `http://gitlab.corp360.vn/project/hallure-suggestion/-/tree/dev_report`).

2.  **Thực hiện Clone:** Mở Command Prompt (CMD) hoặc PowerShell tại thư mục bạn muốn lưu dự án.
    ```bash
    git clone -b dev_report {dán_url_clone_của_bạn_vào_đây}
    ```
    *Lệnh này sẽ tải toàn bộ mã nguồn về máy của bạn và tự động chuyển sang nhánh `dev_report`.*

#### 6.2.7. Cấu hình VS Code (Tùy chọn, khuyến nghị)

* **Mở User Settings JSON:**
    * Trong VS Code, nhấn `Ctrl + Shift + P`.
    * Gõ và chọn `Preferences: Open User Settings (JSON)`.
* **Thêm cấu hình NODE_OPTIONS:**
    * Thêm đoạn cấu hình sau vào tệp `settings.json` của bạn để tăng bộ nhớ cho Node.js, giúp tránh lỗi khi biên dịch Angular:
        ```json
        {
            "terminal.integrated.env.windows": {
                "NODE_OPTIONS": "--max_old_space_size=4096"
            }
        }
        ```
        *Đảm bảo cú pháp JSON chính xác (dấu phẩy, ngoặc nhọn).*

### 6.3. Khởi chạy Hệ thống

#### 6.3.1. Chạy Frontend (Angular)

1.  **Điều hướng đến thư mục frontend:**
    ```bash
    cd {tên_thư_mục_dự_án}/frontend
    ```
    (Ví dụ: `cd DATN/frontend` nếu bạn đã clone vào thư mục `DATN`)

2.  **Cài đặt các gói phụ thuộc (chỉ lần đầu tiên):**
    ```bash
    npm install
    ```

3.  **Khởi động Angular Development Server:**
    ```bash
    ng s
    ```
    * Lệnh này sẽ biên dịch ứng dụng Angular và khởi chạy máy chủ phát triển. Mở trình duyệt của bạn và truy cập: `http://localhost:4200`.

#### 6.3.2. Chạy Backend (.NET Core API)

1.  **Điều hướng đến thư mục backend:**
    ```bash
    cd {tên_thư_mục_dự_án}/backend/App.API # Hoặc đường dẫn chính xác tới file .sln của bạn
    ```

2.  **Mở và Chạy dự án trong Visual Studio:**
    * Tìm file `App.API.sln` (hoặc tên tương ứng của solution file) trong thư mục backend.
    * Mở file này bằng **Visual Studio**.
    * Trong Visual Studio, nhấn **F5** hoặc nút **"Start Debugging"** (biểu tượng mũi tên xanh) để chạy dự án.

  


## 7. Tác giả

* **Tên:** Lê Thị Hằng
* **Email:** hangle08032003@gmail.com
