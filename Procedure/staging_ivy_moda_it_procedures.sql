-- MySQL dump 10.13  Distrib 9.0.1, for Win64 (x86_64)
--
-- Host: 103.141.144.236    Database: staging_ivy_moda_it
-- ------------------------------------------------------
-- Server version	8.0.41

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping routines for database 'staging_ivy_moda_it'
--
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_dat`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.4 2024/04/18*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
/*1 ETL loại đơn*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '1 ETL loại đơn', 'Start', ip_fromDate, ip_toDate, '1 ETL loại đơn');
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		loai_don = 1 /*Nếu độ dài mã ivy = 10 thì loai_don = 0, ngược lại loai_don = 1*/
	WHERE
		loai_don = 0
			AND LENGTH(ivy_invoice) != 10
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '1 ETL loại đơn', 'Finish', ip_fromDate, ip_toDate, '1 ETL loại đơn');      
  
/*2 ETL mã nhân viên bán hàng*/     
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '2 ETL mã nhân viên bán hàng', 'Start', ip_fromDate, ip_toDate, '2 ETL mã nhân viên bán hàng');   
	/*2.1 ETL mã nhân viên bán hàng đã xác định*/  
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		staging_ivy_moda_it.staffs nv ON nv.ma_nv = o.ma_nv 
	SET 
		o.ma_nhan_vien_ban = nv.admin_id
	WHERE
		/*(o.ma_nhan_vien_ban IS NULL
			OR o.ma_nhan_vien_ban = 0)
			AND*/ (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*2.2 ETL mã nhân viên bán hàng chưa xác định*/ 
	UPDATE staging_ivy_moda_it.orders o
	SET 
		o.ma_nhan_vien_ban = 0
	WHERE
		o.ma_nhan_vien_ban IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '2 ETL mã nhân viên bán hàng', 'Finish', ip_fromDate, ip_toDate, '2 ETL mã nhân viên bán hàng');    

/*3 ETL mã pttt*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '3 ETL mã pttt', 'Start', ip_fromDate, ip_toDate, '3 ETL mã pttt');
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 12
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán khi giao hàng'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 3
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng thẻ tín dụng'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 70
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng thẻ ATM'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 74
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Apple Pay'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 25
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Momo'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 72
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Vnpay'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 73
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Ví IVY'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_pttt = 0
	WHERE
		o.ma_pttt IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '3 ETL mã pttt', 'Finish', ip_fromDate, ip_toDate, '3 ETL mã pttt');    

/*5 ETL doanh số hàng đặt, chi phí chiết khấu, số lượng sản phẩm*/
	/*5.1 ETL doanh số hàng đặt, chi phí chiết khấu, số lượng sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '5 ETL doanh số hàng đặt, chi phí chiết khấu, số lượng sản phẩm', 'Start', ip_fromDate, ip_toDate, '5 ETL doanh số hàng đặt, chi phí chiết khấu');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.ngay_dat_hang = o.ngay_mua_hang
	WHERE
		od.ngay_dat_hang IS NULL 
		AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
        
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.ngay_cap_nhat = o.ngay_cap_nhat
	WHERE
		od.ngay_cap_nhat IS NULL 
		AND o.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate;
    
    /*UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT 
			p.order_id AS order_id, SUM(p.quantity) AS so_luong
		FROM
			staging_ivy_moda_it.order_products p
		WHERE
			(p.loai_don = 0) -- Comment lại để ETL cả những đơn tách (đơn ivy xử lý)
				AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
		GROUP BY p.order_id) slsp ON o.id = slsp.order_id 
	SET 
		o.so_luong = slsp.so_luong
	WHERE
		(o.so_luong IS NULL OR o.so_luong = 0)
			AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
            OR o.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate;*/
    
    /*ETL luôn cả doanh số của những đơn tách*/
    UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT 
			od.order_id AS order_id,
				SUM(od.price) AS doanh_so_dat,
				SUM(od.price - od.price_end) AS chi_phi_chiet_khau,
                SUM(od.quantity) AS so_luong
		FROM
			staging_ivy_moda_it.order_products od
		WHERE
			/*(od.price >= od.price_end)
				AND*/ (ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate)
		GROUP BY od.order_id) ds ON o.id = ds.order_id 
	SET 
		o.doanh_so_hang_ban = ds.doanh_so_dat,
		o.cp_khuyen_mai_chiet_khau = ds.chi_phi_chiet_khau,
        o.so_luong = ds.so_luong
	WHERE
		/*(o.doanh_so_hang_ban IS NULL
			OR o.cp_khuyen_mai_chiet_khau IS NULL)
			AND*/ (o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR o.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	
    UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.so_luong = 1
	WHERE
		o.so_luong IS NULL
			AND (o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR o.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.doanh_so_hang_ban = o.tong_tien
	WHERE
		o.doanh_so_hang_ban IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.cp_khuyen_mai_chiet_khau = 0
	WHERE
		o.cp_khuyen_mai_chiet_khau IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*5.2 ETL chi phí voucher*/ -- IT đã cấp cột tien_giam_gia nên không cần ETL nữa
    UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.tien_giam_gia = 0
	WHERE
		o.tien_giam_gia IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '5 ETL doanh số hàng đặt, chi phí chiết khấu, số lượng sản phẩm', 'Finish', ip_fromDate, ip_toDate, '5 ETL doanh số hàng đặt, chi phí chiết khấu');    
/*6 ETL mã trạng thái đơn hàng đặt level_id*/
-- Đã xuất thì không huỷ --> chỉ có thể hoàn thành hoặc hoàn trả
	/*6.1 ETL tiếp mã trạng thái đơn hàng đặt có level_id = '1, 2, 4, 5'*/
	/*ETL nâng cấp đơn hàng từ giao vận thành hoàn thành hoặc đã huỷ */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '6 ETL mã trạng thái đơn hàng đặt level_id', 'Start', ip_fromDate, ip_toDate, '6 ETL mã trạng thái đơn hàng đặt level_id');
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		o.level_id = 7,
		o.level_id_ivy = 5
	WHERE
		(o.level_id IS NULL OR o.level_id < 7)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*6.2 ETL đơn hàng hoàn trả'*/
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT 
			order_id
		FROM
			staging_ivy_moda_it.order_repays
		WHERE
			created_at >= ip_fromDate) oht ON o.id = oht.order_id 
	SET 
		o.level_id = 8,
        o.level_id_ivy = 4
	WHERE
		(o.level_id IS NULL
			OR o.level_id != 8)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    
	/*6.3 ETL mã trạng thái đơn hàng đặt level_id*/
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		staging_ivy_moda.mapping_level_don_hang_dat ml ON ml.trang_thai_don_hang = o.trang_thai
			AND ml.id_nguon = 2 
	SET 
		o.level_id = ml.level_id
	WHERE
		o.level_id IS NULL
			OR (o.level_id < 6 AND o.level_id != 3)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*6.4 ETL tiếp mã trạng thái đơn hàng đặt bị tách có level_id < 6 có trạng thái thành công*/
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT DISTINCT
			LEFT(ivy_invoice, 10) AS ivy_invoice
		FROM
			staging_ivy_moda_it.orders
		WHERE
			loai_don = 1 AND level_id = 7
				AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate)) ot ON o.ivy_invoice = ot.ivy_invoice 
	SET 
		o.level_id = 7
	WHERE
		loai_don = 0
			AND (o.level_id IS NULL OR o.level_id < 6)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*6.5 ETL tiếp mã trạng thái đơn hàng đặt bị tách có level_id = 7 có trạng thái thành công < 100%*/
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT DISTINCT
			LEFT(ivy_invoice, 10) AS ivy_invoice
		FROM
			staging_ivy_moda_it.orders
		WHERE
			loai_don = 1 AND level_id = 8
				AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate) ot ON o.ivy_invoice = ot.ivy_invoice 
	SET 
		o.level_id = 8
	WHERE
		loai_don = 0
			AND (o.level_id IS NULL OR o.level_id != 8)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*6.6 ETL những đơn hàng đặt và huỷ trong cùng ngày*/
	UPDATE staging_ivy_moda_it.orders 
	SET 
		level_id = 0
	WHERE
		level_id IN (3 , 6)
			AND DATE(ngay_mua_hang) = DATE(ngay_cap_nhat)
            AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*6.7 ETL những đơn hàng chưa xử lý được*/
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id = 0
	WHERE
		o.level_id IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '6 ETL mã trạng thái đơn hàng đặt level_id', 'Finish', ip_fromDate, ip_toDate, '6 ETL mã trạng thái đơn hàng đặt level_id');    

/*7 ETL team bán hàng*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '7 ETL team bán hàng', 'Start', ip_fromDate, ip_toDate, '7 ETL team bán hàng');
    UPDATE staging_ivy_moda_it.orders o
	SET 
        o.page_marketing_id = 0
	WHERE
		o.page_marketing_id IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_team_ban_hang = 1
	WHERE
		(o.ma_team_ban_hang IS NULL OR o.ma_team_ban_hang = 0)
			AND o.ma_nhan_vien_ban = 127
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    UPDATE staging_ivy_moda_it.orders o
	SET 
		ma_team_ban_hang = 5
	WHERE
		(o.ma_team_ban_hang IS NULL OR o.ma_team_ban_hang = 0)
			AND ma_nhan_vien_ban IN (228 , 230, 231)
            AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    
    UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_team_ban_hang = 3
	WHERE
		(o.ma_team_ban_hang IS NULL OR o.ma_team_ban_hang IN (0, 4))
			AND o.page_marketing_id != 0
            AND o.ma_nhan_vien_ban != 0
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_team_ban_hang = 4
	WHERE
		(o.ma_team_ban_hang IS NULL OR o.ma_team_ban_hang = 0)
			AND o.ma_nhan_vien_ban != 0
			AND o.page_marketing_id = 0
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		staging_ivy_moda_it.staffs nv ON nv.ma_nv = o.ma_nv 
	SET 
        o.ma_team_ban_hang = nv.ma_team_ban_hang
	WHERE
		(o.ma_team_ban_hang IS NULL
			OR o.ma_team_ban_hang = 0)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.orders o
	SET 
        o.ma_team_ban_hang = 0
	WHERE
		o.ma_team_ban_hang IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '7 ETL team bán hàng', 'Finish', ip_fromDate, ip_toDate, '7 ETL team bán hàng');    

/*8 ETL mã kênh marketing*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '8 ETL mã kênh marketing', 'Start', ip_fromDate, ip_toDate, '8 ETL mã kênh marketing');
    UPDATE staging_ivy_moda_it.orders o
	SET 
        o.page_marketing_id = 57
	WHERE
		o.page_marketing_id = 0
            AND o.ma_team_ban_hang = 1
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_kenh_marketing = 3
	WHERE
		o.page_marketing_id != 0
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_kenh_marketing = 5 -- Organic
	WHERE
		o.ma_kenh_marketing IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '8 ETL mã kênh marketing', 'Finish', ip_fromDate, ip_toDate, '8 ETL mã kênh marketing');    

/*9 ETL mã sàn*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '9 ETL mã sàn', 'Start', ip_fromDate, ip_toDate, '9 ETL mã sàn');
    UPDATE staging_ivy_moda_it.orders o
			JOIN
		olap_ivymoda.dim_san d ON o.nguon = d.san 
	SET 
		o.ma_san = d.ma_san
	WHERE
		(o.ma_san = 0 OR o.ma_san IS NULL)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    UPDATE staging_ivy_moda_it.orders o
	SET 
		o.ma_san = 0
	WHERE
		o.ma_san IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '9 ETL mã sàn', 'Finish', ip_fromDate, ip_toDate, '9 ETL mã sàn');    

/*10 ETL mã tỉnh thành - cập nhật những mã không mapping được */ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '10 ETL mã tỉnh thành', 'Start', ip_fromDate, ip_toDate, '10 ETL mã tỉnh thành');
	UPDATE staging_ivy_moda_it.orders o
			LEFT JOIN
		staging_ivy_moda_it.provinces pv ON o.province_id = pv.province_id 
	SET 
		o.province_id = 0
	WHERE
		pv.province_id IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
						OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', '10 ETL mã tỉnh thành', 'Finish', ip_fromDate, ip_toDate, '10 ETL mã tỉnh thành');    
/*note: chưa xử lý được đơn hàng tách, chưa cập nhật lại trạng thái của các đơn hàng quá khứ*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_dat_hoan` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_dat_hoan`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.3 2024/04/06*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat_hoan', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		(SELECT 
			order_id
		FROM
			staging_ivy_moda_it.order_repays
		WHERE
			created_at >= ip_fromDate) oht ON o.id = oht.order_id 
	SET 
		o.level_id = 8
	WHERE
		o.level_id IS NULL
			OR o.level_id != 8
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
    
    UPDATE staging_ivy_moda_it.order_repays orp
			JOIN
		(SELECT 
			id, ivy_invoice
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate) o ON orp.order_id = o.id 
	SET 
		orp.ivy_invoice = o.ivy_invoice,
		orp.ivy_invoice_goc = LEFT(o.ivy_invoice, 10)
	WHERE
		orp.ivy_invoice IS NULL
			AND orp.created_at >= ip_fromDate;

	UPDATE staging_ivy_moda_it.order_repays orp
			JOIN
		(SELECT 
			order_id AS order_id,
				SUM(quantity_repay) AS so_luong_sp_hoan
		FROM
			staging_ivy_moda_it.order_products
		WHERE
			quantity_repay != 0
				AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
		GROUP BY order_id
		HAVING so_luong_sp_hoan > 1) od ON orp.order_id = od.order_id 
	SET 
		orp.so_luong_sp_hoan = od.so_luong_sp_hoan
	WHERE
		orp.so_luong_sp_hoan = 1
			AND orp.created_at >= ip_fromDate;
			
	UPDATE olap_ivymoda.fact_don_hang_dat fo
			JOIN
		staging_ivy_moda_it.order_repays orp ON fo.ma_don_hang = orp.ivy_invoice_goc 
	SET 
		fo.so_luong_sp_hoan = orp.so_luong_sp_hoan,
		fo.doanh_so_hoan_sau_ck_voucher = orp.price_total_end,
        fo.level_id = 8
	WHERE
		ma_kenh_ban = 2
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_dat_hoan', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_ineco_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_ineco_dat`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.0 2023/12/24*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
/*1 ETL loại đơn*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '1 ETL loại đơn', 'Start', ip_fromDate, ip_toDate, '1 ETL loại đơn');
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		loai_don = 1 /*Nếu độ dài mã ivy = 10 thì loai_don = 0, ngược lại loai_don = 1*/
	WHERE
		loai_don = 0
			AND ivy_invoice LIKE '%-%'
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '1 ETL loại đơn', 'Finish', ip_fromDate, ip_toDate, '1 ETL loại đơn');      
  
/*2 ETL mã nhân viên bán hàng*/     
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '2 ETL mã nhân viên bán hàng', 'Start', ip_fromDate, ip_toDate, '2 ETL mã nhân viên bán hàng');   
	/*2.2 ETL mã nhân viên bán hàng chưa xác định*/ 
	UPDATE staging_ivy_moda_it.order_ineco o
	SET 
		o.order_id = 0
	WHERE
		o.order_id IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '2 ETL mã nhân viên bán hàng', 'Finish', ip_fromDate, ip_toDate, '2 ETL mã nhân viên bán hàng');    

/*3 ETL mã pttt*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '3 ETL mã pttt', 'Start', ip_fromDate, ip_toDate, '3 ETL mã pttt');
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 12
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán khi giao hàng'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 3
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng thẻ tín dụng'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 70
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng thẻ ATM'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 74
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Apple Pay'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 25
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Momo'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 72
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Vnpay'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 73
	WHERE
		phuong_thuc_thanh_toan = 'Thanh toán bằng Ví IVY'
			AND (o.ma_pttt IS NULL OR o.ma_pttt = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_pttt = 0
	WHERE
		o.ma_pttt IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '3 ETL mã pttt', 'Finish', ip_fromDate, ip_toDate, '3 ETL mã pttt');    

/*4 ETL số lượng sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '4 ETL số lượng sản phẩm', 'Start', ip_fromDate, ip_toDate, '4 ETL số lượng sản phẩm');
	
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.order_ineco o ON od.order_ineco_id = o.order_id 
	SET 
		od.ngay_dat_hang = o.ngay_mua_hang
	WHERE
		od.ngay_dat_hang IS NULL 
		AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
    
    UPDATE staging_ivy_moda_it.order_ineco o
			JOIN
		(SELECT 
			p.order_ineco_id AS order_ineco_id, SUM(p.quantity) AS so_luong
		FROM
			staging_ivy_moda_it.order_ineco_products p
		WHERE
			/*(p.loai_don = 0) -- Comment lại để ETL cả những đơn tách (đơn ivy xử lý)
				AND*/ ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
		GROUP BY p.order_ineco_id) slsp ON o.order_id = slsp.order_ineco_id 
	SET 
		o.so_luong = slsp.so_luong
	WHERE
		/*(o.so_luong IS NULL OR o.so_luong = 0)
			AND*/ o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
            OR o.updated_at BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.so_luong = 1
	WHERE
		o.so_luong IS NULL
			AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '4 ETL số lượng sản phẩm', 'Finish', ip_fromDate, ip_toDate, '4 ETL số lượng sản phẩm');    

/*5 ETL doanh số hàng đặt, chi phí chiết khấu*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '5 ETL doanh số hàng đặt, chi phí chiết khấu', 'Start', ip_fromDate, ip_toDate, '5 ETL doanh số hàng đặt, chi phí chiết khấu');
    UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.tong_tien_chiet_khau = 0
	WHERE
		o.tong_tien_chiet_khau IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;  
    UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.tong_tien_nguyen_gia = 0
	WHERE
		o.tong_tien_nguyen_gia IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;  
    UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.gia_tri_voucher = 0
	WHERE
		o.gia_tri_voucher IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '5 ETL doanh số hàng đặt, chi phí chiết khấu', 'Finish', ip_fromDate, ip_toDate, '5 ETL doanh số hàng đặt, chi phí chiết khấu');    
/*6 ETL mã trạng thái đơn hàng đặt level_id*/
-- Đã xuất thì không huỷ --> chỉ có thể hoàn thành hoặc hoàn trả
	/*6.1 ETL tiếp mã trạng thái đơn hàng đặt có level_id = '1, 2, 4, 5'*/
	/*ETL nâng cấp đơn hàng từ giao vận thành hoàn thành hoặc đã huỷ */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '6 ETL mã trạng thái đơn hàng đặt level_id', 'Start', ip_fromDate, ip_toDate, '6 ETL mã trạng thái đơn hàng đặt level_id');
	UPDATE staging_ivy_moda_it.order_ineco o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		o.level_id = 7,
		o.level_id_ivy = 5
	WHERE
		(o.level_id IS NULL OR o.level_id < 7)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	/*6.2 ETL đơn hàng hoàn trả'*/
    
	/*6.3 ETL mã trạng thái đơn hàng đặt level_id*/
	UPDATE staging_ivy_moda_it.order_ineco o
			JOIN
		staging_ivy_moda.mapping_level_don_hang_dat ml ON ml.trang_thai_don_hang = o.trang_thai
			AND ml.id_nguon = 2 
	SET 
		o.level_id = ml.level_id
	WHERE
		o.level_id IS NULL
			OR (o.level_id < 6 AND o.level_id != 3)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	/*6.4 ETL tiếp mã trạng thái đơn hàng đặt bị tách có level_id < 6 có trạng thái thành công*/
	UPDATE staging_ivy_moda_it.order_ineco o
			JOIN
		(SELECT DISTINCT
			SUBSTRING_INDEX(ivy_invoice, '-', 1) AS ivy_invoice
		FROM
			staging_ivy_moda_it.order_ineco
		WHERE
			loai_don = 1 AND level_id = 7
				AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR updated_at BETWEEN ip_fromDate AND ip_toDate)) ot ON o.ivy_invoice = ot.ivy_invoice 
	SET 
		o.level_id = 7
	WHERE
		loai_don = 0
			AND (o.level_id IS NULL OR o.level_id < 6)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	/*6.5 ETL tiếp mã trạng thái đơn hàng đặt bị tách có level_id = 7 có trạng thái thành công < 100%*/
	UPDATE staging_ivy_moda_it.order_ineco o
			JOIN
		(SELECT DISTINCT
			SUBSTRING_INDEX(ivy_invoice, '-', 1) AS ivy_invoice
		FROM
			staging_ivy_moda_it.order_ineco
		WHERE
			loai_don = 1 AND level_id = 8
				AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate) ot ON o.ivy_invoice = ot.ivy_invoice 
	SET 
		o.level_id = 8
	WHERE
		loai_don = 0
			AND (o.level_id IS NULL OR o.level_id != 8)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	/*6.6 ETL những đơn hàng đặt và huỷ trong cùng ngày*/
	UPDATE staging_ivy_moda_it.order_ineco 
	SET 
		level_id = 0
	WHERE
		level_id IN (3 , 6)
			AND DATE(ngay_mua_hang) = DATE(updated_at)
            AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	/*6.7 ETL những đơn hàng chưa xử lý được*/
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id = 0
	WHERE
		o.level_id IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '6 ETL mã trạng thái đơn hàng đặt level_id', 'Finish', ip_fromDate, ip_toDate, '6 ETL mã trạng thái đơn hàng đặt level_id');    

/*10 ETL mã tỉnh thành - cập nhật những mã không mapping được */ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '10 ETL mã tỉnh thành', 'Start', ip_fromDate, ip_toDate, '10 ETL mã tỉnh thành');
	UPDATE staging_ivy_moda_it.order_ineco o
			LEFT JOIN
		staging_ivy_moda_it.provinces pv ON o.region_id = pv.province_id 
	SET 
		o.region_id = 0
	WHERE
		pv.province_id IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', '10 ETL mã tỉnh thành', 'Finish', ip_fromDate, ip_toDate, '10 ETL mã tỉnh thành');    
/*note: chưa xử lý được đơn hàng tách, chưa cập nhật lại trạng thái của các đơn hàng quá khứ*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_ineco_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_ineco_ivy`( ip_fromDate date, ip_toDate date)
BEGIN
/*Ver1.5 2024/04/10*/ -- QuocNV 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
/*1. ETL mã kho*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '1. ETL mã kho', 'Start', ip_fromDate, ip_toDate, '1. ETL mã kho');
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		ma_kho = CASE kho
			WHEN 'ON1' THEN 1
			WHEN 'ON3' THEN 2
			WHEN 'ON4' THEN 1
			ELSE 0
		END
	WHERE
		o.ma_kho IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '1. ETL mã kho', 'Finish', ip_fromDate, ip_toDate, '1. ETL mã kho');    
/*2. ETL mã đơn vị vận chuyển*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '2. ETL mã đơn vị vận chuyển', 'Start', ip_fromDate, ip_toDate, '2. ETL mã đơn vị vận chuyển');
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.ma_dvvc = o.ma_kho
	WHERE
		o.ma_dvvc IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '2. ETL mã đơn vị vận chuyển', 'Finish', ip_fromDate, ip_toDate, '2. ETL mã đơn vị vận chuyển');

/*3. ETL mã phương thức thanh toán*/ 
-- Đã xử lý ở ETL đơn hàng đặt
/*4. ETL phân loại đơn bị tách*/
-- 0 là đơn bị hủy, 1 là đơn bình thường (bao gồm bị tách không huỷ, không bị tách không huỷ), 

CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '4. ETL phân loại đơn bị tách', 'Start', ip_fromDate, ip_toDate, ' 4. ETL phân loại đơn bị tách');
	UPDATE staging_ivy_moda_it.order_ineco o
	SET 
		o.loai_don_ivy = 0
	WHERE
		o.loai_don_ivy != 0
			AND o.trang_thai = 'Đã hủy đơn hàng'
            AND o.level_id_ivy < 4
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o
	SET 
		o.loai_don_ivy = 1
	WHERE
		o.loai_don_ivy = 0
			AND o.trang_thai != 'Đã hủy đơn hàng'
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ineco 
	SET 
		loai_don_ivy = 1
	WHERE
		loai_don_ivy IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '4. ETL phân loại đơn bị tách', 'Finish', ip_fromDate, ip_toDate, ' 4. ETL phân loại đơn bị tách');

    
/*5. ETL level_id trạng thái đơn hàng*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '5. ETL level_id trạng thái đơn hàng', 'Start', ip_fromDate, ip_toDate, '5. ETL level_id trạng thái đơn hàng');
	/*5.1 ETL level đơn hàng ivy (đã thanh toán???) - xuất hàng thành công và hoàn trả*/
    UPDATE staging_ivy_moda_it.order_ineco o
	SET 
		o.level_id_ivy = 5
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
            AND loai_don_ivy = 1
            AND level_id = 7
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ineco o
	SET 
		o.level_id_ivy = 4
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
            AND loai_don_ivy = 1
            AND level_id = 8
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	/*5.2 ETL level đơn hàng ivy trên AMS3*/
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 1
	WHERE
		trang_thai IN ('ON1 xử lý', 'Chờ chia đơn', 'Chờ giao vận', 'Chờ xác nhận', 'Đã in đơn', 'Đơn CH', 'Đơn CHCTD', 'Đơn vào kho')
			AND (o.level_id_ivy IS NULL
			OR (o.level_id_ivy < 4
			AND o.level_id_ivy != 1))
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 2
	WHERE
		trang_thai IN ('Kho check không đủ tồn' , 'Kho check lỗi')
			AND (o.level_id_ivy IS NULL
			OR (o.level_id_ivy < 4
            AND o.level_id_ivy != 2))
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 3
	WHERE
		trang_thai IN ('CH đã giao' , 'Đã giao vận chuyển')
			AND (o.level_id_ivy IS NULL
			OR (o.level_id_ivy < 4
            AND o.level_id_ivy != 3))
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 4
	WHERE
		trang_thai IN ('Đã Trả hàng/Đổi hàng', 'Đã Trả hàng/Hủy COD')
			AND (o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 5
	WHERE
		trang_thai IN ('Hoàn Thành')
			AND (o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
   /*5.3 ETL level đơn hàng ivy trả bằng COD*/         
	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 6
	WHERE
		o.level_id_ivy = 5
			AND o.ma_pttt IN (1 , 12)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco o 
	SET 
		o.level_id_ivy = 0
	WHERE
		o.level_id_ivy IS NULL
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', '5. ETL level_id trạng thái đơn hàng', 'Finish', ip_fromDate, ip_toDate, '5. ETL level_id trạng thái đơn hàng');

/*6. ETL doanh số, số lượng sản phẩm*/
/*Đã ETL ở đơn hàng đặt*/

CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ineco_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_ivy`( ip_fromDate date, ip_toDate date)
BEGIN
/*Ver3.1 2024/03/07*/ -- QuocNV 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
/*1. ETL mã kho*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '1. ETL mã kho', 'Start', ip_fromDate, ip_toDate, '1. ETL mã kho');
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		ma_kho = CASE cua_hang
			WHEN 'ON1' THEN 1
			WHEN 'ON3' THEN 2
			WHEN 'ON4' THEN 1
			ELSE 0
		END
	WHERE
		o.ma_kho IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '1. ETL mã kho', 'Finish', ip_fromDate, ip_toDate, '1. ETL mã kho');    
/*2. ETL mã đơn vị vận chuyển*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '2. ETL mã đơn vị vận chuyển', 'Start', ip_fromDate, ip_toDate, '2. ETL mã đơn vị vận chuyển');
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.ma_dvvc = o.ma_kho
	WHERE
		o.ma_dvvc IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '2. ETL mã đơn vị vận chuyển', 'Finish', ip_fromDate, ip_toDate, '2. ETL mã đơn vị vận chuyển');

/*3. ETL mã phương thức thanh toán*/ 
-- Đã xử lý ở ETL đơn hàng đặt
/*4. ETL phân loại đơn bị tách*/
-- 0 là đơn bị hủy, 1 là đơn bình thường (bao gồm bị tách không huỷ, không bị tách không huỷ), 

CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '4. ETL phân loại đơn bị tách', 'Start', ip_fromDate, ip_toDate, ' 4. ETL phân loại đơn bị tách');
	UPDATE staging_ivy_moda_it.orders o
	SET 
		o.loai_don_ivy = 0
	WHERE
		o.loai_don_ivy != 0
			AND o.trang_thai = 'Đã hủy đơn hàng'
            AND o.level_id_ivy < 4
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o
	SET 
		o.loai_don_ivy = 1
	WHERE
		o.loai_don_ivy = 0
			AND o.trang_thai != 'Đã hủy đơn hàng'
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders 
	SET 
		loai_don_ivy = 1
	WHERE
		loai_don_ivy IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '4. ETL phân loại đơn bị tách', 'Finish', ip_fromDate, ip_toDate, ' 4. ETL phân loại đơn bị tách');

    
/*5. ETL level_id trạng thái đơn hàng*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '5. ETL level_id trạng thái đơn hàng', 'Start', ip_fromDate, ip_toDate, '5. ETL level_id trạng thái đơn hàng');
	/*5.1 ETL level đơn hàng ivy (đã thanh toán???) - xuất hàng thành công và hoàn trả*/
    UPDATE staging_ivy_moda_it.orders o
	SET 
		o.level_id_ivy = 5
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
            AND loai_don_ivy = 1
            AND level_id = 7
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o
	SET 
		o.level_id_ivy = 4
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
            AND loai_don_ivy = 1
            AND level_id = 8
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
			OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	/*5.2 ETL level đơn hàng ivy trên AMS3*/
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 1
	WHERE
		trang_thai IN ('ON1 xử lý', 'Chờ chia đơn', 'Chờ giao vận', 'Chờ xác nhận', 'Đã in đơn', 'Đơn CH', 'Đơn CHCTD', 'Đơn vào kho')
			AND (o.level_id_ivy IS NULL
			OR (o.level_id_ivy < 4
			AND o.level_id_ivy != 1))
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 2
	WHERE
		trang_thai IN ('Kho check không đủ tồn' , 'Kho check lỗi')
			AND (o.level_id_ivy IS NULL
			OR (o.level_id_ivy < 4
            AND o.level_id_ivy != 2))
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 3
	WHERE
		trang_thai IN ('CH đã giao' , 'Đã giao vận chuyển')
			AND (o.level_id_ivy IS NULL
			OR (o.level_id_ivy < 4
            AND o.level_id_ivy != 3))
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 4
	WHERE
		trang_thai IN ('Đã Trả hàng/Đổi hàng', 'Đã Trả hàng/Hủy COD')
			AND (o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 5
	WHERE
		trang_thai IN ('Hoàn Thành')
			AND (o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
   /*5.3 ETL level đơn hàng ivy trả bằng COD*/         
	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 6
	WHERE
		o.level_id_ivy = 5
			AND o.ma_pttt IN (1 , 12)
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.orders o 
	SET 
		o.level_id_ivy = 0
	WHERE
		o.level_id_ivy IS NULL
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', '5. ETL level_id trạng thái đơn hàng', 'Finish', ip_fromDate, ip_toDate, '5. ETL level_id trạng thái đơn hàng');

/*6. ETL doanh số, số lượng sản phẩm*/
/*Đã ETL ở đơn hàng đặt*/

CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_ivy_hoan` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_ivy_hoan`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.3 2024/04/06*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy_hoan', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');			
	
    UPDATE staging_ivy_moda_it.orders o
	SET 
		o.level_id_ivy = 4
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
            AND loai_don_ivy = 1
            AND level_id = 8
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    
    UPDATE olap_ivymoda.fact_don_hang_ivy fo
			JOIN
		staging_ivy_moda_it.order_repays orp ON fo.ma_don_hang = orp.ivy_invoice 
	SET 
		fo.so_luong_sp_hoan = orp.so_luong_sp_hoan,
		fo.doanh_so_hoan_sau_ck_voucher = orp.price_total_end,
        fo.ngay_hoan_tra_hang = DATE(orp.created_at),
        fo.level_id = 4
	WHERE
		ma_kenh_ban = 2
			AND fo.so_luong_sp_hoan = 0
			AND (ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
				OR fo.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.orders_repay_bonus orp
			JOIN
		(SELECT 
			IVM, Stt_HD
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) inf ON orp.Stt_HBTL = inf.Stt_HD 
	SET 
		orp.ivy_invoice = inf.IVM
	WHERE
		orp.ivy_invoice IS NULL
			AND orp.Ngay_Ct >= ip_fromDate;
            
	UPDATE staging_ivy_moda_it.orders o
			JOIN
		staging_ivy_moda_it.orders_repay_bonus orp ON o.ivy_invoice = orp.ivy_invoice 
	SET 
        o.level_id = 8,
        o.level_id_ivy = 4
	WHERE
		/*ma_kenh_ban = 2
			AND*/ o.level_id_ivy != 4
			/*AND fo.so_luong_sp_hoan = 0*/
			AND (ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate
				OR ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
    
	UPDATE olap_ivymoda.fact_don_hang_ivy fo
			JOIN
		staging_ivy_moda_it.orders_repay_bonus orp ON fo.ma_don_hang = orp.ivy_invoice 
	SET 
		fo.so_luong_sp_hoan = orp.Tong_SL,
		fo.doanh_so_hoan_sau_ck_voucher = orp.TTien2 - orp.TTien4 - fo.cp_van_chuyen_ivy,
        fo.ngay_hoan_tra_hang = DATE(orp.Ngay_Ct),
        fo.level_id = 4
	WHERE
		/*ma_kenh_ban = 2
			AND*/ fo.ngay_hoan_tra_hang IS NULL
			/*AND fo.so_luong_sp_hoan = 0*/
			AND (ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
				OR fo.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);

	UPDATE olap_ivymoda.fact_don_hang_ivy fo
			JOIN
		(SELECT 
			IVM, MIN(Ngay_Ct) Ngay_Ct
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate
		GROUP BY IVM) ivm ON fo.ma_don_hang = ivm.IVM 
	SET 
		fo.ngay_xuat_bang_ke = DATE(ivm.Ngay_Ct)
	WHERE
		ma_kenh_ban = 2
			AND fo.ngay_xuat_bang_ke IS NULL
			AND (ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
				OR fo.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_ivy_hoan', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_product_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_product_dat`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.0 2023/12/24*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
/*1 ETL ngày đặt hàng*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '1 ETL ngày đặt hàng', 'Start', ip_fromDate, ip_toDate, '1 ETL ngày đặt hàng');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.ngay_dat_hang = o.ngay_mua_hang
	WHERE
		od.ngay_dat_hang IS NULL
		AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
	
	/*UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.ngay_dat_hang = '2000-01-01'
	WHERE
		od.ngay_dat_hang IS NULL;*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '1 ETL ngày đặt hàng', 'Finish', ip_fromDate, ip_toDate, '1 ETL ngày đặt hàng');    
 
/*2 ETL sản phẩm */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '2 ETL sản phẩm', 'Start', ip_fromDate, ip_toDate, '2 ETL sản phẩm');
	/*2.1 ETL mã sản phẩm */
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.product_subs p ON od.product_sub_id = p.product_sub_id 
	SET 
		od.product_sub_sku = p.product_sub_sku
	WHERE
		(od.product_sub_sku IS NULL
			OR od.product_sub_sku = '00ZZZ00000000000')
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	/*2.2 ETL mã 16 của những đơn hàng có mã 16 không đúng cấu trúc*/
	UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.product_sub_sku = '00ZZZ00000000000'
	WHERE
		(LENGTH(od.product_sub_sku) != 16
			OR od.product_sub_sku IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	/*2.3 ETL tách các thông số của sản phẩm từ mã 16*/
	UPDATE staging_ivy_moda_it.order_products 
	SET 
		ma_7 = CONCAT(SUBSTRING(product_sub_sku, 1, 2),
				SUBSTRING(product_sub_sku, 5, 1),
				SUBSTRING(product_sub_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(product_sub_sku, 1, 2),
		ma_size = SUBSTRING(product_sub_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(product_sub_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(product_sub_sku, 5, 1),
		ma_san_xuat = SUBSTRING(product_sub_sku, 6, 4),
		ma_mau_sac = SUBSTRING(product_sub_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '2 ETL sản phẩm', 'Finish', ip_fromDate, ip_toDate, '2 ETL sản phẩm');    
 
/*3 ETL mã trạng thái đơn hàng đặt */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '3 ETL mã trạng thái đơn hàng đặt', 'Start', ip_fromDate, ip_toDate, '3 ETL mã trạng thái đơn hàng đặt');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.level_id = o.level_id,
        od.level_id_ivy = o.level_id_ivy
	WHERE
		(od.level_id IS NULL
			OR (od.level_id < 6 AND od.level_id != 3))
			AND (od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
				OR od.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.level_id = 0
	WHERE
		od.level_id IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '3 ETL mã trạng thái đơn hàng đặt', 'Finish', ip_fromDate, ip_toDate, '3 ETL mã trạng thái đơn hàng đặt');    
 
/*4 ETL mã nhân viên bán hàng */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '4 ETL mã nhân viên bán hàng', 'Start', ip_fromDate, ip_toDate, '4 ETL mã nhân viên bán hàng');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.ma_nv_ban_hang = o.ma_nhan_vien_ban
	WHERE
		(od.ma_nv_ban_hang IS NULL
			OR od.ma_nv_ban_hang = 0)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.ma_nv_ban_hang = 0
	WHERE
		od.ma_nv_ban_hang IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '4 ETL mã nhân viên bán hàng', 'Finish', ip_fromDate, ip_toDate, '4 ETL mã nhân viên bán hàng');    
 
/*5 ETL mã sàn */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '5 ETL mã sàn', 'Start', ip_fromDate, ip_toDate, '5 ETL mã sàn');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.ma_san = o.ma_san
	WHERE
		(od.ma_san = 0 OR od.ma_san IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.ma_san = 0
	WHERE
		od.ma_san IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '5 ETL mã sàn', 'Finish', ip_fromDate, ip_toDate, '5 ETL mã sàn');    
 
/*6 ETL mã khu vực */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '6 ETL mã khu vực ', 'Start', ip_fromDate, ip_toDate, '6 ETL mã khu vực ');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.ma_khu_vuc = o.province_id
	WHERE
		od.ma_khu_vuc = 0
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.ma_khu_vuc = 0
	WHERE
		od.ma_khu_vuc IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '6 ETL mã khu vực ', 'Finish', ip_fromDate, ip_toDate, '6 ETL mã khu vực ');    
 
/*7 ETL loại đơn */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '7 ETL loại đơn', 'Start', ip_fromDate, ip_toDate, '7 ETL loại đơn');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.loai_don = o.loai_don
	WHERE
			(od.loai_don = 10 OR od.loai_don IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.loai_don = 10
	WHERE
		od.loai_don IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '7 ETL loại đơn', 'Finish', ip_fromDate, ip_toDate, '7 ETL loại đơn');    

/*8 ETL mã kênh marketing, mã page marketing, mã team bán hàng*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '8 ETL mã kênh marketing, mã page marketing, mã team bán hàng', 'Start', ip_fromDate, ip_toDate, '8 ETL mã kênh marketing, mã page marketing, mã team bán hàng');
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.orders o ON od.order_id = o.id 
	SET 
		od.page_marketing_id = o.page_marketing_id,
        od.ma_kenh_marketing = o.ma_kenh_marketing,
        od.ma_team_ban_hang = o.ma_team_ban_hang
	WHERE
		(od.page_marketing_id IS NULL
			OR od.page_marketing_id = 0)
			AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;
    UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.page_marketing_id = 0
	WHERE
		od.page_marketing_id IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
    UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.ma_team_ban_hang = 1
	WHERE
		(od.ma_team_ban_hang IS NULL OR od.ma_team_ban_hang = 0)
			AND od.ma_nv_ban_hang = 127
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    UPDATE staging_ivy_moda_it.order_products od 
	SET 
		ma_team_ban_hang = 5
	WHERE
		(od.ma_team_ban_hang IS NULL OR od.ma_team_ban_hang = 0)
			AND ma_nv_ban_hang IN (228 , 230, 231)
            AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    
    UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.ma_team_ban_hang = 3
	WHERE
		(od.ma_team_ban_hang IS NULL OR od.ma_team_ban_hang = 0)
			AND od.page_marketing_id != 0
            AND od.ma_nv_ban_hang != 0
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.ma_team_ban_hang = 4
	WHERE
		(od.ma_team_ban_hang IS NULL OR od.ma_team_ban_hang = 0)
			AND od.ma_nv_ban_hang != 0
			AND od.page_marketing_id = 0
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_products od 
	SET 
        od.ma_team_ban_hang = 0
	WHERE
		od.ma_team_ban_hang IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
      
	UPDATE staging_ivy_moda_it.order_products od
	SET 
        od.page_marketing_id = 57
	WHERE
		od.page_marketing_id = 0
            AND od.ma_team_ban_hang = 1
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
            
    UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.ma_kenh_marketing = 3
	WHERE
		od.page_marketing_id != 0
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.ma_kenh_marketing = 6
	WHERE
		od.ma_kenh_marketing IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;        
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '8 ETL mã kênh marketing, mã page marketing, mã team bán hàng', 'Finish', ip_fromDate, ip_toDate, '8 ETL mã kênh marketing, mã page marketing, mã team bán hàng');    

/*9 ETL giá gốc của sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '9 ETL giá gốc của sản phẩm', 'Start', ip_fromDate, ip_toDate, '9 ETL giá gốc của sản phẩm');
	UPDATE staging_ivy_moda_it.order_products od
	SET 
		od.price = od.price_end
	WHERE
		od.price < od.price_end 
		AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '9 ETL giá gốc của sản phẩm', 'Finish', ip_fromDate, ip_toDate, '9 ETL giá gốc của sản phẩm');    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_product_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_product_ivy`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver2.1 2024/03/07*/ -- QuocNV
/* Do ETL etl_orders_product_dat đã có mã sp, mã khu vực nên chỉ cần etl thêm mã dvvc và mã kho*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');
/*1. ETL mã dvvc*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '1. ETL mã dvvc', 'Start', ip_fromDate, ip_toDate,'1. ETL mã dvvc');
	UPDATE staging_ivy_moda_it.order_products ct
			LEFT JOIN
		staging_ivy_moda_it.orders s ON ct.order_id = s.id 
	SET 
		ct.ma_dvvc = IFNULL(s.ma_dvvc, 0)
	WHERE
		(ct.ma_dvvc IS NULL
			OR ct.ma_dvvc = 0)
			AND ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '1. ETL mã dvvc', 'Finish', ip_fromDate, ip_toDate,'1. ETL mã dvvc');  

/*2. ETL mã kho*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '2. ETL mã kho', 'Start', ip_fromDate, ip_toDate,'2. ETL mã kho');
	UPDATE staging_ivy_moda_it.order_products ct
			LEFT JOIN
		staging_ivy_moda_it.orders s ON ct.order_id = s.id 
	SET 
		ct.ma_kho = IFNULL(s.ma_kho, 0)
	WHERE
		(ct.ma_kho IS NULL
			OR ct.ma_kho = 0)
			AND ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '2. ETL mã kho', 'Finish', ip_fromDate, ip_toDate,'2. ETL mã kho');  

/*3. ETL mã trạng thái ivy*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '3. ETL mã trạng thái ivy', 'Start', ip_fromDate, ip_toDate,'3. ETL mã trạng thái ivy');
	UPDATE staging_ivy_moda_it.order_products ct
			LEFT JOIN
		staging_ivy_moda_it.orders s ON ct.order_id = s.id 
	SET 
		ct.level_id_ivy = IFNULL(s.level_id_ivy, 0)
	WHERE
		(ct.level_id_ivy IS NULL
			OR ct.level_id_ivy < 4)
			AND (ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
				OR ct.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
-- Xử lý các đơn hoàn
    UPDATE staging_ivy_moda_it.order_products ct
			JOIN
		staging_ivy_moda_it.orders s ON ct.order_id = s.id 
	SET 
		ct.level_id_ivy = 4
	WHERE
		ct.level_id_ivy > 4
			AND s.level_id_ivy = 4
			AND (ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
				OR ct.ngay_cap_nhat BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '3. ETL mã trạng thái ivy', 'Finish', ip_fromDate, ip_toDate,'3. ETL mã trạng thái ivy');    

/*4. ETL loại đơn ivy*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '4. ETL loại đơn ivy', 'Start', ip_fromDate, ip_toDate,'4. ETL loại đơn ivy');
	UPDATE staging_ivy_moda_it.order_products ct
			LEFT JOIN
		staging_ivy_moda_it.orders s ON ct.order_id = s.id 
	SET 
		ct.loai_don_ivy = IFNULL(s.loai_don_ivy, 0)
	WHERE
		ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', '4. ETL loại đơn ivy', 'Finish', ip_fromDate, ip_toDate,'4. ETL loại đơn ivy');    

/*5 ETL sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '5 ETL sản phẩm', 'Start', ip_fromDate, ip_toDate, '5 ETL sản phẩm');
	/*5.1 ETL mã sản phẩm*/
	UPDATE staging_ivy_moda_it.order_products od
			JOIN
		staging_ivy_moda_it.product_subs p ON od.product_sub_id = p.product_sub_id 
	SET 
		od.product_sub_sku = p.product_sub_sku
	WHERE
		(od.product_sub_sku IS NULL
			OR od.product_sub_sku = '00ZZZ00000000000')
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	/*5.2 ETL mã 16 của những đơn hàng có mã 16 không đúng cấu trúc*/
	UPDATE staging_ivy_moda_it.order_products od 
	SET 
		od.product_sub_sku = '00ZZZ00000000000'
	WHERE
		(LENGTH(od.product_sub_sku) != 16
			OR od.product_sub_sku IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	/*5.3 ETL tách các thông số của sản phẩm từ mã 16*/
	UPDATE staging_ivy_moda_it.order_products 
	SET 
		ma_7 = CONCAT(SUBSTRING(product_sub_sku, 1, 2),
				SUBSTRING(product_sub_sku, 5, 1),
				SUBSTRING(product_sub_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(product_sub_sku, 1, 2),
		ma_size = SUBSTRING(product_sub_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(product_sub_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(product_sub_sku, 5, 1),
		ma_san_xuat = SUBSTRING(product_sub_sku, 6, 4),
		ma_mau_sac = SUBSTRING(product_sub_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_dat', '5 ETL sản phẩm', 'Finish', ip_fromDate, ip_toDate, '5 ETL sản phẩm');    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_orders_product_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_orders_web_app` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_orders_web_app`()
BEGIN
 /*1. etl số điện thoại dựa vào customer_id*/
 update staging_ivy_moda_it.orders od
 left join staging_ivy_moda_it.customers cm on  od.customer_id = cm.customer_id
 set od.dien_thoai = cm.customer_phone
 where od.dien_thoai like '%x%';
 
 update staging_ivy_moda_it.orders od
 left join staging_ivy_moda_it.customers cm on  od.customer_id = cm.customer_id
 set od.dien_thoai = cm.customer_phone
 where od.dien_thoai is null;
 /*2. etl số điện thọai dựa vào user name là sđt*/
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_ecommerces_dat_hoan` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_ecommerces_dat_hoan`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.0 2023/12/25*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_dat_hoan', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
	UPDATE staging_ivy_moda_it.order_ecommerce_repays orp
			JOIN
		(SELECT 
			id, tmdt_invoice, tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate) o ON orp.order_ecommerce_id = o.id 
	SET 
		orp.tmdt_invoice = o.tmdt_invoice,
		orp.tmdt_invoice_goc = o.tmdt_invoice_goc
	WHERE
		orp.tmdt_invoice IS NULL
			AND orp.created_at >= ip_fromDate;

	UPDATE staging_ivy_moda_it.order_ecommerce_repays orp
			JOIN
		(SELECT 
			order_ecommerce_id AS order_ecommerce_id,
				SUM(quantity_repay) AS so_luong_sp_hoan
		FROM
			staging_ivy_moda_it.order_ecommerce_products
		WHERE
			quantity_repay != 0
			   AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
		GROUP BY order_ecommerce_id
		HAVING so_luong_sp_hoan > 1) od ON orp.order_ecommerce_id = od.order_ecommerce_id 
	SET 
		orp.so_luong_sp_hoan = od.so_luong_sp_hoan
	WHERE
		orp.so_luong_sp_hoan = 1
			AND orp.created_at >= ip_fromDate;
			
	UPDATE olap_ivymoda.fact_don_hang_dat fo
			JOIN
		staging_ivy_moda_it.order_ecommerce_repays orp ON fo.ma_don_hang = orp.tmdt_invoice_goc 
	SET 
		fo.so_luong_sp_hoan = orp.so_luong_sp_hoan,
		fo.doanh_so_hoan_sau_ck_voucher = orp.price_total_end
	WHERE
		ma_kenh_ban != 2
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_dat_hoan', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_ecommerces_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_ecommerces_ivy`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.0 2024/06/06*/ -- QuocNV	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');
/*
-- 1 ETL số điện thoại, tên người nhận 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '1 ETL số điện thoại, tên người nhận', 'Start', ip_fromDate, ip_toDate,'1 ETL số điện thoại, tên người nhận');
	UPDATE staging_ivy_moda_it.order_ecommerces t
			LEFT JOIN
		(SELECT DISTINCT
			tmdt_type, nguoi_mua, so_dien_thoai, ten_nguoi_nhan
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			so_dien_thoai NOT LIKE '%*%'
				AND so_dien_thoai IS NOT NULL
				AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate) g ON t.nguoi_mua = g.nguoi_mua
			AND t.tmdt_type = g.tmdt_type 
	SET 
		t.so_dien_thoai = g.so_dien_thoai,
		t.ten_nguoi_nhan = g.ten_nguoi_nhan
	WHERE
		t.so_dien_thoai LIKE '%*%'
			OR t.so_dien_thoai IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '1 ETL số điện thoại, tên người nhận', 'Finish', ip_fromDate, ip_toDate,'1 ETL số điện thoại, tên người nhận');    
        
 -- 2 ETL sdt, với những khách hàng có sdt ở tên người mua        
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '2 ETL sdt, với những khách hàng có sdt ở tên người mua', 'Start', ip_fromDate, ip_toDate,'2 ETL sdt, với những khách hàng có sdt ở tên người mua');
	UPDATE staging_ivy_moda_it.order_ecommerces 
	SET 
		so_dien_thoai = nguoi_mua
	WHERE
		nguoi_mua REGEXP '^-?[0-9]+$'
			AND so_dien_thoai IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '2 ETL sdt, với những khách hàng có sdt ở tên người mua', 'Finish', ip_fromDate, ip_toDate,'2 ETL sdt, với những khách hàng có sdt ở tên người mua');    
*/ -- Không biết ETL để làm gì??? ETL dữ liệu để đổ vào DataMart, dữ liệu không dùng đến thì không ETL

/*1 ETL mã kênh bán*/	

CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '1 ETL mã kênh bán', 'Start', ip_fromDate, ip_toDate,'1 ETL mã kênh bán');
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 3
	WHERE
		o.tmdt_type = 'Shopee'
			AND (o.ma_kenh_ban IS NULL OR o.ma_kenh_ban = 1)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 4
	WHERE
		o.tmdt_type = 'Lazada'
			AND (o.ma_kenh_ban IS NULL OR o.ma_kenh_ban = 1)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 5
	WHERE
		o.tmdt_type = 'Tiktok'
			AND (o.ma_kenh_ban IS NULL OR o.ma_kenh_ban = 1)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 1
	WHERE
		o.ma_kenh_ban IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '1 ETL mã kênh bán', 'Finish', ip_fromDate, ip_toDate,'1 ETL mã kênh bán');

	UPDATE staging_ivy_moda_it.order_ecommerces 
	SET 
		ngay_dat_hang = DATE(updated_at)
	WHERE
		ngay_dat_hang IS NULL;

	UPDATE staging_ivy_moda_it.order_ecommerce_products od
			JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.order_ecommerce_id = o.id 
	SET 
		od.ngay_dat_hang = o.ngay_dat_hang
	WHERE
		od.ngay_dat_hang IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);

/*1 ETL số lượng sản phẩm bán*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '1 ETL số lượng sản phẩm bán', 'Start', ip_fromDate, ip_toDate,'1 ETL số lượng sản phẩm bán');
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id, SUM(quantity) so_luong_sp
		FROM
			staging_ivy_moda_it.order_ecommerce_products
		WHERE
			ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
		GROUP BY order_ecommerce_id
		HAVING so_luong_sp < 10000) od ON o.id = od.order_ecommerce_id 
	SET 
		o.so_luong_sp = od.so_luong_sp
	WHERE
		o.so_luong_sp IS NULL
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '1 ETL số lượng sản phẩm bán', 'Finish', ip_fromDate, ip_toDate,'1 ETL số lượng sản phẩm bán');

/*2 ETL mã page marketing, mã kênh marketing, mã team bán hàng chi tiết, mã sàn, chi phí sàn*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '2 ETL mã page marketing, mã kênh marketing, mã team bán hàng chi tiết, mã sàn, chi phí sàn', 'Start', ip_fromDate, ip_toDate,'2 ETL mã page marketing, mã kênh marketing, mã team bán hàng chi tiết, mã sàn, chi phí sàn');
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_marketing = 1,
        o.ma_page_marketings = 51,
        o.ma_team_ban_hang_chi_tiet = 6,
        o.ma_san = 3,
        o.chi_phi_san_tmdt = o.tong_gia_cuoi * 0.1005
 	WHERE
		o.ma_kenh_ban = 3
			AND o.ma_kenh_marketing = 0
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_marketing = 2,
        o.ma_page_marketings = 52,
        o.ma_team_ban_hang_chi_tiet = 7,
        o.ma_san = 4,
        o.chi_phi_san_tmdt = o.tong_gia_cuoi * 0.1005
 	WHERE
		o.ma_kenh_ban = 4
			AND o.ma_kenh_marketing = 0
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_marketing = 8,
        o.ma_page_marketings = 56,
        o.ma_team_ban_hang_chi_tiet = 8,
        o.ma_san = 5,
        o.chi_phi_san_tmdt = o.tong_gia_cuoi * 0.04
 	WHERE
		o.ma_kenh_ban = 5
			AND o.ma_kenh_marketing = 0
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '2 ETL mã page marketing, mã kênh marketing, mã team bán hàng chi tiết, mã sàn, chi phí sàn', 'Finish', ip_fromDate, ip_toDate,'2 ETL mã page marketing, mã kênh marketing, mã team bán hàng chi tiết, mã sàn, chi phí sàn');
/*3 ETL mã tỉnh thành*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '3 ETL mã tỉnh thành', 'Start', ip_fromDate, ip_toDate,'3 ETL mã tỉnh thành');
	/*3.1 ETL mã tỉnh thành của Shopee*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			ma_don_hang, ma_tinh_thanh
		FROM
			staging_ivy_moda.gd_shopee_don_hang_chi_tiet
		WHERE
			ngay_dat_hang >= ip_fromDate) od ON o.tmdt_invoice_goc = od.ma_don_hang 
	SET 
		o.ma_tinh_thanh = od.ma_tinh_thanh
	WHERE
		(o.ma_tinh_thanh IS NULL
			OR o.ma_tinh_thanh = 0)
			AND ma_kenh_ban = 3
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	/*3.2 ETL mã tỉnh thành Lazada*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_number, ma_tinh_thanh
		FROM
			staging_ivy_moda.gd_lazada_don_hang_chi_tiet
		WHERE
			create_time >= ip_fromDate) od ON o.tmdt_invoice_goc = od.order_number 
	SET 
		o.ma_tinh_thanh = od.ma_tinh_thanh
	WHERE
		(o.ma_tinh_thanh IS NULL
			OR o.ma_tinh_thanh = 0)
			AND ma_kenh_ban = 4
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	/*3.3 ETL mã tỉnh thành Tiktok*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_id, ma_tinh_thanh
		FROM
			staging_ivy_moda.gd_tiktok_don_hang_chi_tiet
		WHERE
			created_time >= ip_fromDate) od ON o.tmdt_invoice_goc = od.order_id 
	SET 
		o.ma_tinh_thanh = od.ma_tinh_thanh
	WHERE
		(o.ma_tinh_thanh IS NULL
			OR o.ma_tinh_thanh = 0)
			AND ma_kenh_ban = 5
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.order_ecommerces
	SET 
		ma_tinh_thanh = 0
	WHERE
		ma_tinh_thanh IS NULL
			AND (ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '3 ETL mã tỉnh thành', 'Finish', ip_fromDate, ip_toDate,'3 ETL mã tỉnh thành');    
 
/*4 ETL mã phương thức thanh toán*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '4 ETL mã phương thức thanh toán', 'Start', ip_fromDate, ip_toDate,'4 ETL mã phương thức thanh toán');
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		olap_ivymoda.dim_pttt pttt ON pttt.ten_pttt = o.phuong_thuc_thanh_toan 
	SET 
		o.ma_pttt = pttt.ma_pttt
	WHERE
		o.ma_pttt IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	UPDATE staging_ivy_moda_it.order_ecommerces o
	SET 
		o.ma_pttt = 0
	WHERE
		o.ma_pttt IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '4 ETL mã phương thức thanh toán', 'Finish', ip_fromDate, ip_toDate,'4 ETL mã phương thức thanh toán');	
/*5 ETL trạng thái ivy*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '5 ETL trạng thái ivy', 'Start', ip_fromDate, ip_toDate,'5 ETL trạng thái ivy');
	/*5.0 ETL đơn hàng hoàn trả'*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		o.level_id_ivy = 5
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);

	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_ivy_moda_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		o.level_id_ivy = 4
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);

    /*5.1 ETL level đơn hàng thành công và đã thanh toán - xuất thành công và hoàn trả*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
	SET 
		o.level_id_ivy = 5
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
			AND o.check_trang_thai_don_hang = 1
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
            
	UPDATE staging_ivy_moda_it.order_ecommerces o
	SET 
		o.level_id_ivy = 4
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy != 4)
			AND o.check_trang_thai_don_hang = 2
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
    /*5.2 ETL level đơn hàng trong ams3*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		staging_ivy_moda.mapping_level_don_hang_ivy mp ON o.trang_thai = mp.trang_thai_ivy 
	SET 
		o.level_id_ivy = mp.level_id
	WHERE
		(o.level_id_ivy IS NULL
			OR o.level_id_ivy < 4)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
   /*5.3 ETL level đơn hàng thanh toán COD*/     
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.level_id_ivy = 6
	WHERE
		o.level_id_ivy = 5
			AND (ma_pttt = 1 OR ma_pttt = 12)
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
	/*5.4 ETL những đơn hàng chưa xử lý được*/
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.level_id_ivy = 0
	WHERE
		o.level_id_ivy IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '5 ETL trạng thái ivy', 'Finish', ip_fromDate, ip_toDate,'5 ETL trạng thái ivy');

/*7 ETL giá gốc và chi phí voucher của sản phẩm*/
	UPDATE staging_ivy_moda_it.order_ecommerces o
	SET 
		o.ma_giam_gia_shop = 0
	WHERE
		o.ma_giam_gia_shop IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	UPDATE staging_ivy_moda_it.order_ecommerce_products od 
	SET 
		od.gia_mot_san_pham = 0
	WHERE
		od.gia_mot_san_pham IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    
    UPDATE staging_ivy_moda_it.order_ecommerce_products od
			JOIN
		staging_ivy_moda_it.product_subs sp ON (od.product_sub_id = sp.product_sub_id) 
	SET 
		od.gia_goc = sp.product_sub_price
	WHERE
		(od.gia_goc IS NULL OR od.gia_goc = 0) 
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
    UPDATE staging_ivy_moda_it.order_ecommerce_products od 
	SET 
		od.gia_goc = od.gia_mot_san_pham
	WHERE
		(od.gia_goc IS NULL OR od.gia_goc = 0) 
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id AS order_ecommerce_id,
				SUM(gia_goc * quantity) AS gia_goc
		FROM
			staging_ivy_moda_it.order_ecommerce_products
		WHERE
			ngay_dat_hang >= ip_fromDate
		GROUP BY order_ecommerce_id) od ON o.id = od.order_ecommerce_id 
	SET 
		o.gia_goc = od.gia_goc
	WHERE
		/*od.gia_goc IS NULL
			AND*/ ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.gia_goc = o.tong_gia_cuoi
	WHERE
		o.gia_goc IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
/*8 ETL số lượng sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '8 ETL số lượng sản phẩm', 'Start', ip_fromDate, ip_toDate,'8 ETL số lượng sản phẩm');
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id AS order_ecommerce_id,
				SUM(quantity) AS so_luong_sp
		FROM
			staging_ivy_moda_it.order_ecommerce_products
		WHERE
			ngay_dat_hang >= ip_fromDate
		GROUP BY order_ecommerce_id) od ON o.id = od.order_ecommerce_id 
	SET 
		o.so_luong_sp = od.so_luong_sp
	WHERE
		od.so_luong_sp IS NULL
			AND (o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
            OR updated_at BETWEEN ip_fromDate AND ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', '8 ETL số lượng sản phẩm', 'Finish', ip_fromDate, ip_toDate,'8 ETL số lượng sản phẩm');
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_ecommerces_ivy_hoan` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_ecommerces_ivy_hoan`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.3 2024/04/06*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy_hoan', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_ivy_moda_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		o.check_trang_thai_don_hang = 2,
        o.level_id_ivy = 4
	WHERE
		o.check_trang_thai_don_hang < 2
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    
    UPDATE staging_ivy_moda_it.order_ecommerce_repays orp
			JOIN
		(SELECT 
			id, ivy_invoice
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate) o ON orp.order_ecommerce_id = o.id 
	SET 
		orp.ivy_invoice = o.ivy_invoice
	WHERE
		orp.ivy_invoice IS NULL
			AND orp.created_at >= ip_fromDate;
    
	UPDATE olap_ivymoda.fact_don_hang_ivy fo
			JOIN
		staging_ivy_moda_it.order_ecommerce_repays orp ON fo.ma_don_hang = orp.ivy_invoice 
	SET 
		fo.so_luong_sp_hoan = orp.so_luong_sp_hoan,
		fo.doanh_so_hoan_sau_ck_voucher = orp.price_total_end,
        fo.ngay_hoan_tra_hang = DATE(orp.created_at),
        fo.level_id = 4
	WHERE
		ma_kenh_ban != 2
			/*AND fo.so_luong_sp_hoan = 0*/
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	UPDATE olap_ivymoda.fact_don_hang_ivy fo
			JOIN
		(SELECT 
			IVM, MIN(Ngay_Ct) Ngay_Ct
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate
		GROUP BY IVM) ivm ON fo.ma_don_hang = ivm.IVM 
	SET 
		fo.ngay_xuat_bang_ke = DATE(ivm.Ngay_Ct)
	WHERE
		ma_kenh_ban != 2
			AND fo.ngay_xuat_bang_ke IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerces_ivy_hoan', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_ecommerce_products` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_ecommerce_products`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver2.0 2023/12/24*/ -- QuocNV
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');
/*1. ETL ngày đặt hàng, mã kênh bán*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '1. ETL ngày đặt hàng, mã kênh bán', 'Start', ip_fromDate, ip_toDate,'1. ETL ngày đặt hàng, mã kênh bán');
	UPDATE staging_ivy_moda_it.order_ecommerce_products od
			JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.order_ecommerce_id = o.id 
	SET 
		od.ngay_dat_hang = o.ngay_dat_hang
	WHERE
		od.ngay_dat_hang IS NULL
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ecommerce_products od
			JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.order_ecommerce_id = o.id 
	SET 
		od.ma_kenh_ban = o.ma_kenh_ban
	WHERE
		(od.ma_kenh_ban IS NULL
			OR od.ma_kenh_ban = 1)
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ecommerce_products od
	SET 
        od.ma_kenh_ban = 1
	WHERE
		od.ma_kenh_ban IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '1. ETL ngày đặt hàng, mã kênh bán', 'Finish', ip_fromDate, ip_toDate,'1. ETL ngày đặt hàng, mã kênh bán');    
 
/*2. ETL mã sản phẩm */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '2. ETL mã sản phẩm', 'Start', ip_fromDate, ip_toDate,'2. ETL mã sản phẩm');
	UPDATE staging_ivy_moda_it.order_ecommerce_products o
			LEFT JOIN
		staging_ivy_moda_it.product_subs p ON o.product_sub_id = p.product_sub_id 
	SET 
		o.product_sub_sku = IFNULL(p.product_sub_sku, 0)
	WHERE
		(o.product_sub_sku IS NULL
			OR o.product_sub_sku = '00ZZZ00000000000')
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate; 
	/*2.2 ETL thông tin sản phẩm*/
	/*2.2.1 ETL mã 16 của những đơn hàng có mã 16 không đúng cấu trúc*/
	UPDATE staging_ivy_moda_it.order_ecommerce_products o 
	SET 
		product_sub_sku = '00ZZZ00000000000'
	WHERE
		LENGTH(product_sub_sku) != 16
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	/*2.2.2 ETL tách các thông số của sản phẩm từ mã 16*/
	UPDATE staging_ivy_moda_it.order_ecommerce_products 
	SET 
		ma_7 = CONCAT(SUBSTRING(product_sub_sku, 1, 2),
				SUBSTRING(product_sub_sku, 5, 1),
				SUBSTRING(product_sub_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(product_sub_sku, 1, 2),
		ma_size = SUBSTRING(product_sub_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(product_sub_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(product_sub_sku, 5, 1),
		ma_san_xuat = SUBSTRING(product_sub_sku, 6, 4),
		ma_mau_sac = SUBSTRING(product_sub_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '2. ETL mã sản phẩm', 'Finish', ip_fromDate, ip_toDate,'2. ETL mã sản phẩm');    
 
-- 3. ETL mã trạng thái đơn hàng ivy 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '3. ETL mã trạng thái đơn hàng ivy', 'Start', ip_fromDate, ip_toDate,'3. ETL mã trạng thái đơn hàng ivy');
	UPDATE staging_ivy_moda_it.order_ecommerce_products od
			LEFT JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.order_ecommerce_id = o.id 
	SET 
		od.level_id_ivy = IFNULL(o.level_id_ivy, 0)
	WHERE
		(od.level_id_ivy IS NULL
			OR od.level_id_ivy < 4)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
-- Xử lý các đơn hoàn
	UPDATE staging_ivy_moda_it.order_ecommerce_products od
			LEFT JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.order_ecommerce_id = o.id 
	SET 
		od.level_id_ivy = 4
	WHERE
		(od.level_id_ivy != 4
			OR o.level_id_ivy = 4)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '3. ETL mã trạng thái đơn hàng ivy', 'Finish', ip_fromDate, ip_toDate,'3. ETL mã trạng thái đơn hàng ivy');    
 
-- 4. ETL mã dvvc  mặc định là 3 - dvvc TMDT
 
-- 5. ETL mã khu vực
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '5. ETL mã khu vực', 'Start', ip_fromDate, ip_toDate,'5. ETL mã khu vực');
	UPDATE staging_ivy_moda_it.order_ecommerce_products od
			LEFT JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.order_ecommerce_id = o.id 
	SET 
		od.ma_tinh_thanh = IFNULL(o.ma_tinh_thanh, 0),
        od.ma_page_marketings = IFNULL(o.ma_page_marketings, 0),
        od.ma_kenh_marketing = IFNULL(o.ma_kenh_marketing, 0),
        od.ma_team_ban_hang_chi_tiet = IFNULL(o.ma_team_ban_hang_chi_tiet, 0),
        od.ma_san = IFNULL(o.ma_san, 0)
	WHERE
		(od.ma_tinh_thanh IS NULL
			OR od.ma_tinh_thanh = 0)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', '5. ETL mã khu vực', 'Finish', ip_fromDate, ip_toDate,'5. ETL mã khu vực');    

CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ecommerce_products', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_ineco_products_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_ineco_products_dat`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver1.5 2024/04/10*/ -- QuocNV 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');
/*1 ETL ngày đặt hàng*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '1 ETL ngày đặt hàng', 'Start', ip_fromDate, ip_toDate, '1 ETL ngày đặt hàng');
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.order_ineco o ON od.order_ineco_id = o.order_id 
	SET 
		od.ngay_dat_hang = o.ngay_mua_hang
	WHERE
		od.ngay_dat_hang IS NULL 
		AND o.ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ineco_products od
	SET 
		od.ngay_dat_hang = od.updated_at
	WHERE
		od.ngay_dat_hang IS NULL;
	
	UPDATE staging_ivy_moda_it.order_ineco_products od
	SET 
		od.ngay_dat_hang = '2000-01-01'
	WHERE
		od.ngay_dat_hang IS NULL;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '1 ETL ngày đặt hàng', 'Finish', ip_fromDate, ip_toDate, '1 ETL ngày đặt hàng');    
 
/*2 ETL sản phẩm */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '2 ETL sản phẩm', 'Start', ip_fromDate, ip_toDate, '2 ETL sản phẩm');
	/*2.1 ETL mã sản phẩm */
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.product_subs p ON od.product_sub_id = p.product_sub_id 
	SET 
		od.product_sub_sku = p.product_sub_sku
	WHERE
		(od.product_sub_sku IS NULL
			OR od.product_sub_sku = '00ZZZ00000000000')
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	/*2.2 ETL mã 16 của những đơn hàng có mã 16 không đúng cấu trúc*/
	UPDATE staging_ivy_moda_it.order_ineco_products od 
	SET 
		od.product_sub_sku = '00ZZZ00000000000'
	WHERE
		(LENGTH(od.product_sub_sku) != 16
			OR od.product_sub_sku IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	/*2.3 ETL tách các thông số của sản phẩm từ mã 16*/
	UPDATE staging_ivy_moda_it.order_ineco_products 
	SET 
		ma_7 = CONCAT(SUBSTRING(product_sub_sku, 1, 2),
				SUBSTRING(product_sub_sku, 5, 1),
				SUBSTRING(product_sub_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(product_sub_sku, 1, 2),
		ma_size = SUBSTRING(product_sub_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(product_sub_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(product_sub_sku, 5, 1),
		ma_san_xuat = SUBSTRING(product_sub_sku, 6, 4),
		ma_mau_sac = SUBSTRING(product_sub_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '2 ETL sản phẩm', 'Finish', ip_fromDate, ip_toDate, '2 ETL sản phẩm');    
 
/*3 ETL mã trạng thái đơn hàng đặt */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '3 ETL mã trạng thái đơn hàng đặt', 'Start', ip_fromDate, ip_toDate, '3 ETL mã trạng thái đơn hàng đặt');
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.order_ineco o ON od.order_ineco_id = o.order_id 
	SET 
		od.level_id = o.level_id
	WHERE
		(od.level_id IS NULL
			OR (od.level_id < 6 AND od.level_id != 3))
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco_products od
	SET 
		od.level_id = 0
	WHERE
		od.level_id IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '3 ETL mã trạng thái đơn hàng đặt', 'Finish', ip_fromDate, ip_toDate, '3 ETL mã trạng thái đơn hàng đặt');    
 
/*4 ETL mã nhân viên bán hàng */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '4 ETL mã nhân viên bán hàng', 'Start', ip_fromDate, ip_toDate, '4 ETL mã nhân viên bán hàng');
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.order_ineco o ON od.order_ineco_id = o.order_id 
	SET 
		od.ma_nv_ban_hang = o.id_nv
	WHERE
		(od.ma_nv_ban_hang IS NULL
			OR od.ma_nv_ban_hang = 0)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco_products od
	SET 
		od.ma_nv_ban_hang = 0
	WHERE
		od.ma_nv_ban_hang IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '4 ETL mã nhân viên bán hàng', 'Finish', ip_fromDate, ip_toDate, '4 ETL mã nhân viên bán hàng');    

/*6 ETL mã khu vực */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '6 ETL mã khu vực ', 'Start', ip_fromDate, ip_toDate, '6 ETL mã khu vực ');
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.order_ineco o ON od.order_ineco_id = o.order_id 
	SET 
		od.ma_khu_vuc = o.region_id
	WHERE
		od.ma_khu_vuc = 0
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco_products od 
	SET 
		od.ma_khu_vuc = 0
	WHERE
		od.ma_khu_vuc IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '6 ETL mã khu vực ', 'Finish', ip_fromDate, ip_toDate, '6 ETL mã khu vực ');    
 
/*7 ETL loại đơn */
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '7 ETL loại đơn', 'Start', ip_fromDate, ip_toDate, '7 ETL loại đơn');
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.order_ineco o ON od.order_ineco_id = o.order_id 
	SET 
		od.loai_don = o.loai_don
	WHERE
			(od.loai_don = 10 OR od.loai_don IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ineco_products od
	SET 
		od.loai_don = 10
	WHERE
		od.loai_don IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '7 ETL loại đơn', 'Finish', ip_fromDate, ip_toDate, '7 ETL loại đơn');    

/*9 ETL giá gốc của sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '9 ETL giá gốc của sản phẩm', 'Start', ip_fromDate, ip_toDate, '9 ETL giá gốc của sản phẩm');
	UPDATE staging_ivy_moda_it.order_ineco_products od
	SET 
		od.price = od.price_end
	WHERE
		od.price < od.price_end 
		AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', '9 ETL giá gốc của sản phẩm', 'Finish', ip_fromDate, ip_toDate, '9 ETL giá gốc của sản phẩm');    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_ineco_products_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_ineco_products_ivy`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver1.5 2024/04/10*/ -- QuocNV 
/* Do ETL etl_order_ineco_product_dat đã có mã sp, mã khu vực nên chỉ cần etl thêm mã dvvc và mã kho*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');
/*1. ETL mã dvvc*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '1. ETL mã dvvc', 'Start', ip_fromDate, ip_toDate,'1. ETL mã dvvc');
	UPDATE staging_ivy_moda_it.order_ineco_products ct
			LEFT JOIN
		staging_ivy_moda_it.order_ineco s ON s.order_id = ct.order_ineco_id
	SET 
		ct.ma_dvvc = IFNULL(s.ma_dvvc, 0)
	WHERE
		(ct.ma_dvvc IS NULL
			OR ct.ma_dvvc = 0)
			AND ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '1. ETL mã dvvc', 'Finish', ip_fromDate, ip_toDate,'1. ETL mã dvvc');  

/*2. ETL mã kho*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '2. ETL mã kho', 'Start', ip_fromDate, ip_toDate,'2. ETL mã kho');
	UPDATE staging_ivy_moda_it.order_ineco_products ct
			LEFT JOIN
		staging_ivy_moda_it.order_ineco s ON s.order_id = ct.order_ineco_id 
	SET 
		ct.ma_kho = IFNULL(s.ma_kho, 0)
	WHERE
		(ct.ma_kho IS NULL
			OR ct.ma_kho = 0)
			AND ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '2. ETL mã kho', 'Finish', ip_fromDate, ip_toDate,'2. ETL mã kho');  

/*3. ETL mã trạng thái ivy*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '3. ETL mã trạng thái ivy', 'Start', ip_fromDate, ip_toDate,'3. ETL mã trạng thái ivy');
	UPDATE staging_ivy_moda_it.order_ineco_products ct
			LEFT JOIN
		staging_ivy_moda_it.order_ineco s ON s.order_id = ct.order_ineco_id 
	SET 
		ct.level_id_ivy = IFNULL(s.level_id_ivy, 0)
	WHERE
		(ct.level_id_ivy IS NULL
			OR ct.level_id_ivy < 4)
			AND ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '3. ETL mã trạng thái ivy', 'Finish', ip_fromDate, ip_toDate,'3. ETL mã trạng thái ivy');    

/*4. ETL loại đơn ivy*/ 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '4. ETL loại đơn ivy', 'Start', ip_fromDate, ip_toDate,'4. ETL loại đơn ivy');
	UPDATE staging_ivy_moda_it.order_ineco_products ct
			LEFT JOIN
		staging_ivy_moda_it.order_ineco s ON s.order_id = ct.order_ineco_id 
	SET 
		ct.loai_don_ivy = IFNULL(s.loai_don_ivy, 0)
	WHERE
		ct.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', '4. ETL loại đơn ivy', 'Finish', ip_fromDate, ip_toDate,'4. ETL loại đơn ivy');    

/*5 ETL sản phẩm*/
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_product_dat', '5 ETL sản phẩm', 'Start', ip_fromDate, ip_toDate, '5 ETL sản phẩm');
	/*5.1 ETL mã sản phẩm*/
	UPDATE staging_ivy_moda_it.order_ineco_products od
			JOIN
		staging_ivy_moda_it.product_subs p ON od.product_sub_id = p.product_sub_id 
	SET 
		od.product_sub_sku = p.product_sub_sku
	WHERE
		(od.product_sub_sku IS NULL
			OR od.product_sub_sku = '00ZZZ00000000000')
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	/*5.2 ETL mã 16 của những đơn hàng có mã 16 không đúng cấu trúc*/
	UPDATE staging_ivy_moda_it.order_ineco_products od 
	SET 
		od.product_sub_sku = '00ZZZ00000000000'
	WHERE
		(LENGTH(od.product_sub_sku) != 16
			OR od.product_sub_sku IS NULL)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	/*5.3 ETL tách các thông số của sản phẩm từ mã 16*/
	UPDATE staging_ivy_moda_it.order_ineco_products 
	SET 
		ma_7 = CONCAT(SUBSTRING(product_sub_sku, 1, 2),
				SUBSTRING(product_sub_sku, 5, 1),
				SUBSTRING(product_sub_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(product_sub_sku, 1, 2),
		ma_size = SUBSTRING(product_sub_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(product_sub_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(product_sub_sku, 5, 1),
		ma_san_xuat = SUBSTRING(product_sub_sku, 6, 4),
		ma_mau_sac = SUBSTRING(product_sub_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_product_dat', '5 ETL sản phẩm', 'Finish', ip_fromDate, ip_toDate, '5 ETL sản phẩm');    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda_it', 'etl_order_ineco_products_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_offline` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_offline`(fromDate date, toDate date)
BEGIN

	#Routine body goes here...

	

-- 	INSERT INTO staging_ivy_moda_it.order_offline 

	INSERT INTO `staging_ivy_moda_it`.`order_offline` (    `stt`,    `ma_ct`,    `Ma_DvCs`,    `Ngay_Ct`,    `Ma_Dt`,    `Ma_The`,    `TTien2`,    `TTien4`,    `TTien_Nt41`,

    `TTien_Nt42`,    `Ma_Vou1`,    `Ma_Vou2`,    `Ma_Bp`,    `Ma_Bp1`,    `Tong_SL`,    `So_Ct`,    `stt_returned`,    `so_dien_thoai`,    `ds4`,    `province_id`) 

SELECT

    o.stt,    o.ma_ct,    o.Ma_DvCs,    o.Ngay_Ct,    o.Ma_Dt,    o.Ma_The,    o.TTien2,    o.TTien4,    o.TTien_Nt41,    o.TTien_Nt42,    o.Ma_Vou1,    o.Ma_Vou2,    o.Ma_Bp,

    o.Ma_Bp1,    o.Tong_SL,    o.So_Ct,    o.Stt_HBTL,

    c.Phone ,

    (o.TTien2 - IFNULL(o.TTien4, 0) - IFNULL(o.TTien_Nt41, 0)) AS ds4,

    ch.tinh_code AS province_id

FROM

    staging_ivy_moda_it.orders_dau_phieu AS o

    LEFT JOIN staging_ivy_moda_it.dm_dvcs_w ch ON o.Ma_DvCs = ch.Dvcs

    LEFT JOIN staging_ivy_moda_it.customers_offline c ON o.Ma_Dt = c.Ma_Dt 

WHERE

    o.Ngay_Ct BETWEEN fromDate AND toDate

    AND o.Ma_DvCs NOT IN ('ON', 'O3', 'O4', 'O5') 

    AND o.ma_ct = 'HD' 

    AND o.Ma_Dt IS NOT NULL 

    AND o.Ma_The IS NOT NULL 

    AND o.Stt_HBTL IS NULL 

    AND c.Phone IS NOT NULL

ON DUPLICATE KEY UPDATE

		ma_ct = o.ma_ct,

		Ma_DvCs = o.Ma_DvCs,

		Ngay_Ct = o.Ngay_Ct,

    TTien2 = o.TTien2,

    TTien4 = o.TTien4,

    TTien_Nt41 = o.TTien_Nt41,

    TTien_Nt42 = o.TTien_Nt42,

    Ma_Vou1 = o.Ma_Vou1,

    Ma_Vou2 = o.Ma_Vou2,

    Ma_Bp = o.Ma_Bp,

    Ma_Bp1 = o.Ma_Bp1,

    Tong_SL = o.Tong_SL,

    So_Ct = o.So_Ct,

    stt_returned = o.Stt_HBTL,

    ds4 = (o.TTien2 - IFNULL(o.TTien4, 0) - IFNULL(o.TTien_Nt41, 0)),

    province_id = ch.tinh_code,

    so_dien_thoai = c.Phone

		;





INSERT INTO `staging_ivy_moda_it`.`order_offline` (	`stt`,	`ma_ct`,	`Ma_DvCs`,	`Ngay_Ct`,	`Ma_Dt`,	`Ma_The`,	`TTien2`,	`TTien4`,	`TTien_Nt41`,	`TTien_Nt42`,	`Ma_Vou1`,	`Ma_Vou2`,	`Ma_Bp`,	`Ma_Bp1`,	`Tong_SL`,	`So_Ct`,	`stt_returned` ) 

SELECT

o.Stt_HBTL, o.ma_ct, o.Ma_DvCs,o.Ngay_Ct,o.Ma_Dt,o.Ma_The,o.TTien2,o.TTien4,o.TTien_Nt41,o.TTien_Nt42,o.Ma_Vou1,o.Ma_Vou2,o.Ma_Bp,o.Ma_Bp1,o.Tong_SL,o.So_Ct,o.stt

FROM

	staging_ivy_moda_it.orders_dau_phieu AS o

WHERE

	o.Ngay_Ct BETWEEN fromDate AND toDate

	AND o.Ma_DvCs NOT IN ( 'ON', 'O3', 'O4', 'O5' ) #bỏ qua đơn ONLINE

	AND o.ma_ct = 'TL' 

	AND o.Ma_Dt IS NOT NULL 

	AND o.Ma_The IS NOT NULL 

	AND o.Stt_HBTL IS NOT NULL # chỉ lấy đơn hoàn





ON DUPLICATE KEY UPDATE

	TTien2 = (`staging_ivy_moda_it`.`order_offline`.TTien2 - o.TTien2),

	TTien4 = (`staging_ivy_moda_it`.`order_offline`.TTien4 - IFNULL( o.TTien4, 0 )),

	ds4 = (`staging_ivy_moda_it`.`order_offline`.TTien2 - IFNULL(`staging_ivy_moda_it`.`order_offline`.TTien4,0) - IFNULL(`staging_ivy_moda_it`.`order_offline`.TTien_Nt41,0)),

	Tong_SL = (`staging_ivy_moda_it`.`order_offline`.Tong_SL - o.Tong_SL),

	

-- 		ma_ct = o.ma_ct,

-- 		Ma_DvCs = o.Ma_DvCs,

		Ngay_Ct = o.Ngay_Ct,

    stt_returned = o.stt--     ds4 = (o.TTien2 - IFNULL(o.TTien4, 0) - IFNULL(o.TTien_Nt41, 0)),

;

	



END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_order_offline_products` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_order_offline_products`(fromDate date, toDate date)
BEGIN



INSERT INTO `order_offline_products` (	`Stt0`,	`Stt`,	`Ma_DvCs`,	`Ngay_Ct`,	`Ma_Ct`,	`Ma_Kho`,	`Ma_Vt`,

	`Kind`,	`Colors`,	`Ma_Nhom`,	`Ma_Dong`,	`Ma_SX`,	`Ma7`,	`So_Luong`,

	`Tien2`,	`Tien4`,	`Tien41`,	`Tien42`,	`So_Ct`,	`Ma_The`,	`Ma_Dt`,	`Tien`,	`CK` ) 

SELECT

o.Stt0, o.stt, o.Ma_DvCs, o.Ngay_Ct, o.ma_ct AS Ma_Ct, o.Ma_Kho, o.Ma_Vt, o.Kind, o.Colors,

o.Ma_Nhom, o.Ma_Dong, o.Ma_SX, o.Ma7, o.So_Luong, o.Tien2, o.Tien4, o.Tien41, o.Tien42, o.So_Ct,

o.Ma_The, o.Ma_Dt, o.Tien, o.CK 

FROM

	orders_than_phieu AS o 

WHERE

   o.Ngay_Ct BETWEEN fromDate AND toDate

   AND o.Ma_DvCs NOT IN ('ON', 'O3', 'O4', 'O5') 

   AND o.ma_ct = 'HD' 

	 AND o.Colors != '---'

	ON DUPLICATE KEY UPDATE 

	Stt = o.Stt, 	Ma_DvCs = o.Ma_DvCs, 	Ngay_Ct = o.Ngay_Ct, 	Ma_Ct = o.Ma_Ct, 	Ma_Kho = o.Ma_Kho, 	Ma_Vt = o.Ma_Vt,

	Kind = o.Kind, 	Colors = o.Colors, 	Ma_Nhom = o.Ma_Nhom, 	Ma_Dong = o.Ma_Dong, 	Ma_SX = o.Ma_SX, 	Ma7 = o.Ma7,

	So_Luong = o.So_Luong, 	Tien2 = o.Tien2, 	Tien4 = o.Tien4, 	Tien41 = o.Tien41, 	Tien42 = o.Tien42, 	So_Ct = o.So_Ct,

	Ma_The = o.Ma_The, 	Ma_Dt = o.Ma_Dt, 	Tien = o.Tien, 	CK = o.CK

	;





INSERT INTO `order_offline_products` (	`Stt0`,	`Stt`,`Ngay_Ct`,`Ma_Ct`, `order_returned`)

SELECT

	p.Stt0, 

	o.Stt_HBTL, 

	o.Ngay_Ct,

	o.ma_ct,

	p.Stt

FROM

	orders_dau_phieu AS o

	INNER JOIN

	orders_than_phieu AS p

	ON 

		o.Stt_HBTL = p.Stt

WHERE

   o.Ngay_Ct BETWEEN fromDate AND toDate

   AND o.Ma_DvCs NOT IN ('ON', 'O3', 'O4', 'O5') 

   AND o.ma_ct = 'TL' 

	 AND p.Colors != '---'

	ON DUPLICATE KEY UPDATE order_returned = p.stt, Ma_Ct = o.Ma_Ct, Ngay_Ct = o.Ngay_Ct

;



DELETE FROM staging_ivy_moda_it.order_offline_products p

WHERE p.order_returned IS NOT NULL

AND p.Colors = '---'

;





END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_redundant_data` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_redundant_data`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver2.0 2024/04/10*/ -- QuocNV

	DELETE FROM staging_ivy_moda_it.order_products od
	WHERE od.order_product_id IN
		(SELECT 
			du_lieu_thua.order_product_id
		FROM
			(SELECT 
				od.order_product_id
			FROM
				staging_ivy_moda_it.order_products od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON od.order_product_id = c.ma_don_hang_chi_tiet
			JOIN
				(SELECT DISTINCT
					ma_don_hang
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua
				WHERE
					ma_nguon_du_lieu = 1) c2 ON od.order_id = c2.ma_don_hang
			WHERE
				c.ma_don_hang_chi_tiet IS NULL) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	DELETE FROM staging_metagent_it.order_products od
	WHERE od.order_product_id IN
		(SELECT 
			du_lieu_thua.order_product_id
		FROM
			(SELECT 
				od.order_product_id
			FROM
				staging_metagent_it.order_products od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON od.order_product_id = c.ma_don_hang_chi_tiet
			JOIN
				(SELECT DISTINCT
					ma_don_hang
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua
				WHERE
					ma_nguon_du_lieu = 2) c2 ON od.order_id = c2.ma_don_hang
			WHERE
				c.ma_don_hang_chi_tiet IS NULL) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	DELETE FROM staging_ivy_moda_it.order_ineco_products od
	WHERE od.id IN
		(SELECT 
			du_lieu_thua.order_ineco_product_id
		FROM
			(SELECT 
				od.id order_ineco_product_id
			FROM
				staging_ivy_moda_it.order_ineco_products od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON od.id = c.ma_don_hang_chi_tiet
			JOIN
				(SELECT DISTINCT
					ma_don_hang
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua
				WHERE
					ma_nguon_du_lieu = 3) c2 ON od.order_ineco_id = c2.ma_don_hang
			WHERE
				c.ma_don_hang_chi_tiet IS NULL) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;


	DELETE FROM staging_ivy_moda_it.order_ecommerce_products od
	WHERE od.id IN
		(SELECT 
			du_lieu_thua.order_ecommerce_product_id
		FROM
			(SELECT 
				od.id order_ecommerce_product_id
			FROM
				staging_ivy_moda_it.order_ecommerce_products od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON od.id = c.ma_don_hang_chi_tiet
            JOIN
				(SELECT DISTINCT
					ma_don_hang
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua
				WHERE
					ma_nguon_du_lieu = 4) c2 ON od.order_ecommerce_id = c2.ma_don_hang
			WHERE
				c.ma_don_hang_chi_tiet IS NULL) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
				
	DELETE FROM staging_metagent_it.order_ecommerce_products od
	WHERE od.id IN
		(SELECT 
			du_lieu_thua.order_ecommerce_product_id
		FROM
			(SELECT 
				od.id order_ecommerce_product_id
			FROM
				staging_metagent_it.order_ecommerce_products od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON od.id = c.ma_don_hang_chi_tiet
            JOIN
				(SELECT DISTINCT
					ma_don_hang
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua
				WHERE
					ma_nguon_du_lieu = 5) c2 ON od.order_ecommerce_id = c2.ma_don_hang
			WHERE
				c.ma_don_hang_chi_tiet IS NULL) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	DELETE FROM olap_ivymoda.fact_so_luong_sp_ivy od
	WHERE od.ma_don_hang_chi_tiet IN
		(SELECT 
			du_lieu_thua.ma_don_hang_chi_tiet
		FROM
			(SELECT 
				od.ma_don_hang_chi_tiet
			FROM
				olap_ivymoda.fact_so_luong_sp_ivy od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON (od.ma_don_hang_chi_tiet = c.ma_don_hang_chi_tiet
				AND od.ma_nguon_du_lieu = c.ma_nguon_du_lieu)
			JOIN
				(SELECT DISTINCT
					ma_don_hang, ma_nguon_du_lieu
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua) c2 
				ON (od.ma_don_hang = c2.ma_don_hang AND od.ma_nguon_du_lieu = c2.ma_nguon_du_lieu)
			WHERE
				c.ma_don_hang_chi_tiet IS NULL) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
				
	DELETE FROM olap_ivymoda.fact_so_luong_sp_dat od
	WHERE od.ma_don_hang_chi_tiet IN
		(SELECT 
			du_lieu_thua.ma_don_hang_chi_tiet
		FROM
			(SELECT 
				od.ma_don_hang_chi_tiet
			FROM
				olap_ivymoda.fact_so_luong_sp_dat od
			LEFT JOIN staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua c ON (od.ma_don_hang_chi_tiet = c.ma_don_hang_chi_tiet
				AND od.ma_nguon_du_lieu = c.ma_nguon_du_lieu)
			JOIN
				(SELECT DISTINCT
					ma_don_hang, ma_nguon_du_lieu
				FROM
					staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua) c2 
				ON (od.ma_don_hang = c2.ma_don_hang AND od.ma_nguon_du_lieu = c2.ma_nguon_du_lieu)
			WHERE
				c.ma_don_hang_chi_tiet IS NULL
					AND od.ma_kenh_ban = 2) AS du_lieu_thua)
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
-- Xử lý đơn thừa offline        
	-- Xoá bảng tạm nếu đã tồn tại
	DROP TABLE IF EXISTS staging_ivy_moda_it.orders_than_phieu_offline;

	-- Tạo bảng tạm mới
	CREATE TEMPORARY TABLE staging_ivy_moda_it.orders_than_phieu_offline AS
	SELECT 
		Stt0, Stt, DATE(Ngay_Ct) Ngay_Ct
	FROM
		staging_ivy_moda_it.orders_than_phieu
	WHERE
		Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5')
			AND Ngay_Ct BETWEEN ip_fromDate AND ip_toDate;

	-- Thêm khóa chính
	ALTER TABLE staging_ivy_moda_it.orders_than_phieu_offline
	ADD PRIMARY KEY (Stt0);

	-- Thêm chỉ mục
	CREATE INDEX index_than_phieu_offline_stt ON staging_ivy_moda_it.orders_than_phieu_offline (Stt);

	DELETE FROM staging_ivy_moda_it.orders_than_phieu 
	WHERE
		stt0 IN (SELECT 
			stt0
		FROM
			staging_ivy_moda_it.orders_than_phieu_offline tp
				LEFT JOIN
			staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua_offline ctp ON (tp.stt0 = ctp.ma_don_hang_chi_tiet)
				JOIN
			(SELECT DISTINCT
				ma_don_hang
			FROM
				staging_ivy_moda_it.check_du_lieu_don_hang_chi_tiet_thua_offline) cdp ON (tp.stt = cdp.ma_don_hang)
		
		WHERE
			tp.ngay_Ct BETWEEN ip_fromDate AND ip_toDate
			AND ctp.ma_don_hang_chi_tiet IS NULL)
	AND ngay_Ct BETWEEN ip_fromDate AND ip_toDate;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `import_dim_lien_lac` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `import_dim_lien_lac`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
	INSERT INTO user_core.dim_lien_lac (customer_index, loai_lien_lac, thong_tin_lien_lac)
SELECT
    customer_index,
    CASE
        WHEN Email IS NOT NULL THEN 'Email'
        ELSE NULL
    END AS loai_lien_lac_email,
    Email AS thong_tin_lien_lac_email
FROM user_core.dim_customers
WHERE Email IS NOT NULL and created_at between ip_fromDate and ip_toDate
UNION ALL
SELECT
    customer_index,
    CASE
        WHEN Phone IS NOT NULL THEN 'Phone'
        ELSE NULL
    END AS loai_lien_lac_dien_thoai,
    Phone AS thong_tin_lien_lac_dien_thoai
FROM user_core.dim_customers
WHERE Phone IS NOT NULL and created_at between ip_fromDate and ip_toDate; 
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `import_user_core` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `import_user_core`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
	/*version 1 08/01/2024*/
    /*1. Import dữ liệu khách hàng offline vào dim_customers*/
    INSERT IGNORE INTO user_core.dim_customers(
        Phone,
        FullName,
        Ma_The,
        Ma_Dt,
        Gioi_tinh,
        Ngay_sinh,
        Email,
        Dia_chi,
        ID_offline,
        province_name,
        district_name,
        customer_type,
        Create_at
    )
    SELECT
        Phone,
        Ten_Khach,
        Ma_The,
        Ma_Dt,
        Gioi_Tinh,
        CASE WHEN Ngay_Sinh >= "1900-01-01" THEN Ngay_Sinh ELSE NULL END,
        Email,
        Dia_Chi,
        Phone,
        TinhTP,
        QuanHuyen,
        Loai_The,
        Ngay_Tao
    FROM
        staging_ivy_moda_it.customers_offline
    WHERE
        Phone is not null and Phone not like "%*%" and Ngay_sinh >= "1900-01-01" and Ngay_Tao BETWEEN ip_fromDate AND ip_toDate;

    /*2. Import dữ liệu khách hàng web-app vào dim_customers*/
    INSERT INTO user_core.dim_customers (
        Phone,
        Ngay_sinh,
        FullName,
        Gioi_tinh,
        Email,
        ID_web_app,
        province_id,
        district_id,
        ward_id,
        customer_type,
        Create_at
    )
    SELECT
        customer_phone,
        CASE WHEN customer_birthday >= "1900-01-01" THEN customer_birthday ELSE NULL END,
        customer_name,
        customer_gender,
        customer_email,
        customer_id,
        province_id,
        district_id,
        ward_id,
        customer_type,
        created_at
    FROM
        staging_ivy_moda_it.customers
    WHERE
        customer_phone is not null and customer_phone not like "%*%" and customer_birthday >= "1900-01-01" and created_at BETWEEN ip_fromDate AND ip_toDate
    ON DUPLICATE KEY UPDATE
        Phone = VALUES(Phone),
        Ngay_sinh = VALUES(Ngay_sinh),
        FullName = VALUES(FullName),
        Gioi_tinh = VALUES(Gioi_tinh),
        Email = VALUES(Email),
        province_id = VALUES(province_id),
        district_id = VALUES(district_id),
        ward_id = VALUES(ward_id),
        customer_type = VALUES(customer_type),
        Create_at = VALUES(Create_at);

    /*3. Import dữ liệu khách hàng Shopee vào dim_customers*/
    INSERT INTO user_core.dim_customers (
        Phone,
        Dia_chi,
        Email,
        FullName,
        ID_Shopee,
        Create_at,
        province_name,
        district_name,
        ward_name
    )
    SELECT
        so_dien_thoai,
        address,
        email,
        ten_nguoi_nhan,
        so_dien_thoai,
        created_at,
        province,
        district,
        ward
    FROM
        staging_ivy_moda_it.order_ecommerces
    WHERE
        tmdt_type = "Shopee"
        AND so_dien_thoai IS NOT NULL and so_dien_thoai not like "%*%" and created_at BETWEEN ip_fromDate AND ip_toDate
    ON DUPLICATE KEY UPDATE
        Dia_chi = VALUES(Dia_chi),
        Email = VALUES(Email),
        FullName = VALUES(FullName),
        ID_Shopee = VALUES(ID_Shopee),
        Create_at = VALUES(Create_at),
        province_name = VALUES(province_name),
        district_name = VALUES(district_name),
        ward_name = VALUES(ward_name);

    /*4. Import dữ liệu khách hàng Lazada vào dim_customers*/
    INSERT INTO user_core.dim_customers (
        Phone,
        Dia_chi,
        Email,
        FullName,
        ID_Lazada,
        Create_at,
        province_name,
        district_name,
        ward_name
    )
    SELECT
        so_dien_thoai,
        address,
        email,
        ten_nguoi_nhan,
        so_dien_thoai,
        created_at,
        province,
        district,
        ward
    FROM
        staging_ivy_moda_it.order_ecommerces
    WHERE
        tmdt_type = "Lazada"
        AND so_dien_thoai IS NOT NULL and so_dien_thoai not like "%*%" and created_at BETWEEN ip_fromDate AND ip_toDate
    ON DUPLICATE KEY UPDATE
        Dia_chi = VALUES(Dia_chi),
        Email = VALUES(Email),
        FullName = VALUES(FullName),
        ID_Lazada = VALUES(ID_Lazada),
        Create_at = VALUES(Create_at),
        province_name = VALUES(province_name),
        district_name = VALUES(district_name),
        ward_name = VALUES(ward_name);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `summary_data_report` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `summary_data_report`()
BEGIN
	


select 'Tổng đơn hàng TMĐT' as NoiDung, count(*) as tongDonHangTMĐT from order_ecommerces -- Số lượng: 88624;
union
select 'Tổng đơn hàng TMĐT không có SĐT sau tiền xử lý' as NoiDung, count(*)  from order_ecommerces where so_dien_thoai is null -- Số lượng: 20415
union
select 'Tổng đơn hàng TMĐT Tiktok không có SĐT sau tiền xử lý' as NoiDung, count(*)  from order_ecommerces where so_dien_thoai is null and tmdt_type = 'tiktok'
union
select 'Tổng đơn hàng TMĐT shopee không có SĐT sau tiền xử lý' as NoiDung, count(*)  from order_ecommerces where so_dien_thoai is null and tmdt_type = 'Shopee'
union
select 'Tổng đơn hàng TMĐT lazada không có SĐT sau tiền xử lý' as NoiDung, count(*)  from order_ecommerces where so_dien_thoai is null and tmdt_type = 'lazada'
union
select 'Tổng đơn hàng TMĐT còn SĐT bị che sau ETL' as NoiDung, count(*)    from order_ecommerces where so_dien_thoai like '%*%'-- số lượng: 0
union 
SELECT 'Tổng đơn hàng TMĐT có sđt ở người mua mà SĐT null' as NoiDung, count(*)  FROM order_ecommerces WHERE nguoi_mua REGEXP '^-?[0-9]+$' and so_dien_thoai is null;


select 'Tổng khách hàng TMĐT' as NoiDung, count(distinct nguoi_mua) as tongDonHangTMĐT from order_ecommerces -- số lượng kh: 34K
union
select 'Tổng khách hàng TMĐT không có SĐT  sau tiền xử lý' as NoiDung, count(distinct nguoi_mua)  from order_ecommerces where so_dien_thoai is null  -- số lượng KH: 12.6K
union
select 'Tổng khách hàng TMĐT TKTOK không có SĐT  sau tiền xử lý' as NoiDung, count(distinct nguoi_mua)  from order_ecommerces where so_dien_thoai is null and tmdt_type = 'tiktok' -- số lượng KH: 12.6K
union
select 'Tổng khách hàng TMĐT Shopee không có SĐT  sau tiền xử lý' as NoiDung, count(distinct nguoi_mua)  from order_ecommerces where so_dien_thoai is null and tmdt_type = 'Shopee' -- số lượng KH: 12.6K
union
select 'Tổng khách hàng TMĐT lazada không có SĐT  sau tiền xử lý' as NoiDung, count(distinct nguoi_mua)  from order_ecommerces where so_dien_thoai is null and tmdt_type = 'lazada' -- số lượng KH: 12.6K
union
select 'Tổng khách hàng TMĐT còn SĐT bị che sau ETL' as NoiDung, count(distinct nguoi_mua)    from order_ecommerces where so_dien_thoai like '%*%'-- số lượng: 0
union 
SELECT 'Tổng khách hàng TMĐT có sđt ở người mua mà SĐT null' as NoiDung, count(distinct nguoi_mua)  FROM order_ecommerces WHERE nguoi_mua REGEXP '^-?[0-9]+$' and so_dien_thoai is null;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_repay_bonus` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_repay_bonus`(ip_fromDate DATE, ip_toDate DATE)
BEGIN

INSERT INTO staging_ivy_moda_it.orders_repay_bonus (`stt`, `ma_ct`, `Ma_DvCs`, `Ngay_Ct`, `Nh_Ct`, `Ma_Dt`, `Ma_The`, `TTien2`, `TTien4`,

 `TTien_Nt41`, `TTien_Nt42`, `TTien_Nt43`, `Ma_Vou1`, `Ma_Vou2`, `Ma_Vou3`, `Ma_Loai1`, `Ma_Loai2`, `Ma_Loai3`, `Ma_Bp`, `Ma_Bp1`, `Tong_SL`,

 `So_Ct`, `Ck_The_Dc`, `CrtTime`, `So_Ct0`, `Ngay_Ct0`, `Stt_HBTL`)

SELECT 
    `stt`,
    `ma_ct`,
    `Ma_DvCs`,
    `Ngay_Ct`,
    `Nh_Ct`,
    `Ma_Dt`,
    `Ma_The`,
    `TTien2`,
    `TTien4`,
    `TTien_Nt41`,
    `TTien_Nt42`,
    `TTien_Nt43`,
    `Ma_Vou1`,
    `Ma_Vou2`,
    `Ma_Vou3`,
    `Ma_Loai1`,
    `Ma_Loai2`,
    `Ma_Loai3`,
    `Ma_Bp`,
    `Ma_Bp1`,
    `Tong_SL`,
    `So_Ct`,
    `Ck_The_Dc`,
    `CrtTime`,
    `So_Ct0`,
    `Ngay_Ct0`,
    `Stt_HBTL`
FROM
    staging_ivy_moda_it.orders_dau_phieu o
WHERE
    ma_ct = 'TL' /*AND Ma_DvCs != 'ON'
        AND Ma_Loai2 IS NOT NULL*/
        AND Ma_Loai2 IN ('ON', 'O3', 'O4', 'O5')
        AND Ngay_Ct BETWEEN ip_fromDate AND ip_toDate



ON DUPLICATE KEY UPDATE

TTien2 = o.TTien2,
TTien4 = o.TTien4,
Tong_SL = o.Tong_SL

;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `thong_ke_bi_ma_hoa` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `thong_ke_bi_ma_hoa`()
BEGIN
	SELECT tmdt_type, count(distinct s.so_dien_thoai) FROM staging_ivy_moda_it.order_ecommerces s where so_dien_thoai like "%*%" 
    group by tmdt_type;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `thong_ke_so_bo` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `thong_ke_so_bo`()
BEGIN
	/*Thong ke web-app*/
select * from orders where customer_id is null;

select count(*) from orders where customer_id =  0; -- 525821   /666148
select count(*) from orders; -- 666148 

select count(customer_phone) from customers;
select count(*) from customers;

select count(distinct  dien_thoai) from (
select DISTINCT dien_thoai as dien_thoai from orders
union select distinct customer_phone as dien_thoai from customers) t; -- 243946
	/*Thong ke offline*/
select count(*) from orders_offline_dau_phieu o
, customers_offline c
where o.Ma_Dt = c.Ma_Dt
and Ngay_Ct >'2019-12-31';  -- 1071098

select count(*) from orders_offline_dau_phieu o where Ngay_Ct >'2019-12-31';  -- 2271383


select count(Ma_dt) from orders_offline_dau_phieu where Ngay_Ct >'2019-12-31';  -- 2271383
select count(Phone) from  customers_offline where Ngay_tao >'2019-12-31'; -- 211039
select count(*) from  customers_offline ;

select * from customers_offline where Ngay_tao >'2019-12-31';



END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-06-14 23:57:59
