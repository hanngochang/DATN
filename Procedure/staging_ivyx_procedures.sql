-- MySQL dump 10.13  Distrib 9.0.1, for Win64 (x86_64)
--
-- Host: 103.141.144.236    Database: staging_ivy_moda
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
-- Dumping routines for database 'staging_ivy_moda'
--
/*!50003 DROP PROCEDURE IF EXISTS `etl_don_hang_dat_chi_tiet_lazada` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_don_hang_dat_chi_tiet_lazada`( ip_fromDate date, ip_toDate date)
BEGIN
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '1 ETL đơn hàng có trong bảng kê xuất và hoàn trả', 'Start', ip_fromDate, ip_toDate,  '1 ETL đơn hàng có trong bảng kê xuất và hoàn trả');
	
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 4
	WHERE
		o.tmdt_type = 'Lazada'
			AND o.ma_kenh_ban IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ecommerces 
	SET 
		tmdt_invoice_goc = LEFT(tmdt_invoice, 15)
	WHERE
		tmdt_invoice_goc IS NULL
			AND ma_kenh_ban = 4
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	UPDATE staging_metagent_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 4
	WHERE
		o.tmdt_type = 'Lazada'
			AND o.ma_kenh_ban IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_metagent_it.order_ecommerces 
	SET 
		tmdt_invoice_goc = LEFT(tmdt_invoice, 15)
	WHERE
		tmdt_invoice_goc IS NULL
			AND ma_kenh_ban = 4
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		check_trang_thai_don_hang = 1
	WHERE
		check_trang_thai_don_hang = 0
			AND o.ma_kenh_ban = 4
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_ivy_moda_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		check_trang_thai_don_hang = 2
	WHERE
		check_trang_thai_don_hang < 2
			AND o.ma_kenh_ban = 4
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_metagent_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		check_trang_thai_don_hang = 1
	WHERE
		check_trang_thai_don_hang = 0
			AND o.ma_kenh_ban = 4
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_metagent_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_metagent_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		check_trang_thai_don_hang = 2
	WHERE
		check_trang_thai_don_hang < 2
			AND o.ma_kenh_ban = 4
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '1 ETL đơn hàng có trong bảng kê xuất và hoàn trả', 'Finish', ip_fromDate, ip_toDate,  '1 ETL đơn hàng có trong bảng kê xuất và hoàn trả');



CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '2 ETL mã trạng thái đơn hàng đặt level_id', 'Start',ip_fromDate, ip_toDate, '2 ETL mã trạng thái đơn hàng đặt level_id');
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 1
				AND ma_kenh_ban = 4
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_number = o.tmdt_invoice_goc 
	SET 
		od.level_id = 7
	WHERE
		od.level_id < 7
			AND od.create_time BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 1
				AND ma_kenh_ban = 4
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_number = o.tmdt_invoice_goc 
	SET 
		od.level_id = 7
	WHERE
		od.level_id < 7
			AND od.create_time BETWEEN ip_fromDate AND ip_toDate;
	
    UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 2 
				AND ma_kenh_ban = 4
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_number = o.tmdt_invoice_goc 
	SET 
		od.level_id = 8 
	WHERE
		od.level_id < 8 
			AND od.create_time BETWEEN ip_fromDate AND ip_toDate;
    UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 2 
				AND ma_kenh_ban = 4
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_number = o.tmdt_invoice_goc 
	SET 
		od.level_id = 8 
	WHERE
		od.level_id < 8 
			AND od.create_time BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet l
			LEFT JOIN
		staging_ivy_moda.mapping_level_don_hang_dat ml ON ml.trang_thai_don_hang = l.status
			AND ml.id_nguon = 4 
	SET 
		l.level_id = IFNULL(ml.level_id, 0)
	WHERE
		l.level_id < 6 AND l.level_id <> 3
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
            
	
    

    
	
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ma_kenh_ban = 4
				AND ngay_dat_hang >= ip_fromDate UNION SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ma_kenh_ban = 4
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_number = o.tmdt_invoice_goc 
	SET 
		od.level_id = 0
	WHERE
		od.level_id IN (3 , 6)
			AND o.tmdt_invoice_goc IS NULL
			AND od.create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '2 ETL mã trạng thái đơn hàng đặt level_id', 'Finish',ip_fromDate, ip_toDate, '2 ETL mã trạng thái đơn hàng đặt level_id');       
        

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '3 ETL giá gốc sản phẩm hàng đặt', 'Start',ip_fromDate, ip_toDate, '3 ETL giá gốc sản phẩm hàng đặt');
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			JOIN staging_ivy_moda_it.product_subs sp ON od.seller_sku = sp.product_sub_sku
	SET 
		od.gia_goc = sp.product_sub_price
	WHERE
		gia_goc = 0
			AND create_time BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet 
	SET 
		gia_goc = unit_price
	WHERE
		gia_goc = 0
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '3 ETL giá gốc sản phẩm hàng đặt', 'Finish',ip_fromDate, ip_toDate, '3 ETL giá gốc sản phẩm hàng đặt');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '4, 5 ETL doanh số hàng đặt, chi phí ck', 'Start',ip_fromDate, ip_toDate, '4, 5 ETL doanh số hàng đặt, chi phí ck');     
        
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet
	SET 
		doanh_so_hang_ban = gia_goc
	WHERE
		doanh_so_hang_ban IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
	

	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet
	SET 
        chi_phi_chiet_khau = gia_goc - unit_price
	WHERE
		chi_phi_chiet_khau IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
   CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '4, 5 ETL doanh số hàng đặt, chi phí ck', 'Finish',ip_fromDate, ip_toDate, '4, 5 ETL doanh số hàng đặt, chi phí ck');       


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '6 ETL mã tỉnh thành', 'Start',ip_fromDate, ip_toDate, '6 ETL mã tỉnh thành');
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet l
			JOIN
		olap_ivymoda.dim_khu_vuc kv ON l.shipping_address3 = kv.tinh_thanh 
	SET 
		l.ma_tinh_thanh = kv.ma_tinh_thanh
	WHERE
		l.ma_tinh_thanh = 0
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '6 ETL mã tỉnh thành', 'Finish',ip_fromDate, ip_toDate, '6 ETL mã tỉnh thành');	


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '7 ETL mã phương thức thanh toán', 'Start', ip_fromDate, ip_toDate,'7 ETL mã phương thức thanh toán');
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			JOIN
		olap_ivymoda.dim_pttt pttt ON pttt.ten_pttt = od.pay_method 
	SET 
		od.ma_pttt = pttt.ma_pttt
	WHERE
		od.ma_pttt = 0
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', '7 ETL mã phương thức thanh toán', 'Finish',ip_fromDate, ip_toDate, '7 ETL mã phương thức thanh toán');	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_lazada', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_don_hang_dat_chi_tiet_shopee` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_don_hang_dat_chi_tiet_shopee`(ip_fromDate date, ip_toDate date)
BEGIN
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '1 ETL mã tỉnh thành', 'Start', ip_fromDate, ip_toDate,  '1 ETL mã tỉnh thành');
    UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet 
	SET 
		ma_tinh_thanh = 507 
	WHERE
		tinh_thanh_pho = 'TP. Hồ Chí Minh'
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
        
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		olap_ivymoda.dim_khu_vuc p ON od.tinh_thanh_pho = p.tinh_thanh 
	SET 
		od.ma_tinh_thanh = p.ma_tinh_thanh
	WHERE
		od.ma_tinh_thanh = 0
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '1 ETL mã tỉnh thành', 'Finish', ip_fromDate, ip_toDate,  '1 ETL mã tỉnh thành');


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '2 ETL mã phương thức thanh toán', 'Start', ip_fromDate, ip_toDate,  '2 ETL mã phương thức thanh toán');
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		olap_ivymoda.dim_pttt p ON od.phuong_thuc_thanh_toan = p.ten_pttt 
	SET 
		od.ma_pttt = p.ma_pttt 
	WHERE
		od.ma_pttt = 0
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '2 ETL mã phương thức thanh toán', 'Finish', ip_fromDate, ip_toDate,  '2 ETL mã phương thức thanh toán');


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả', 'Start', ip_fromDate, ip_toDate,  '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả');
	
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 3
	WHERE
		o.tmdt_type = 'Shopee'
			AND o.ma_kenh_ban IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ecommerces 
	SET 
		tmdt_invoice_goc = LEFT(tmdt_invoice, 14)
	WHERE
		tmdt_invoice_goc IS NULL
			AND ma_kenh_ban = 3
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_metagent_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 3
	WHERE
		o.tmdt_type = 'Shopee'
			AND o.ma_kenh_ban IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_metagent_it.order_ecommerces 
	SET 
		tmdt_invoice_goc = LEFT(tmdt_invoice, 14)
	WHERE
		tmdt_invoice_goc IS NULL
			AND ma_kenh_ban = 3
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		o.check_trang_thai_don_hang = 1
	WHERE
		o.check_trang_thai_don_hang = 0
			AND o.ma_kenh_ban = 3
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_ivy_moda_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		o.check_trang_thai_don_hang = 2
	WHERE
		o.check_trang_thai_don_hang < 2
			AND o.ma_kenh_ban = 3
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_metagent_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		o.check_trang_thai_don_hang = 1
	WHERE
		o.check_trang_thai_don_hang = 0
			AND o.ma_kenh_ban = 3
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_metagent_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_metagent_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		o.check_trang_thai_don_hang = 2
	WHERE
		o.check_trang_thai_don_hang < 2
			AND o.ma_kenh_ban = 3
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
		
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả', 'Finish', ip_fromDate, ip_toDate,  '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả');



CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '4 ETL mã trạng thái đơn hàng đặt', 'Start', ip_fromDate, ip_toDate,  '4 ETL mã trạng thái đơn hàng đặt');
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 1
				AND ma_kenh_ban = 3
				AND ngay_dat_hang >= ip_fromDate) o ON od.ma_don_hang = o.tmdt_invoice_goc 
	SET 
		od.level_id = 7
	WHERE
		od.level_id < 7
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 1
				AND ma_kenh_ban = 3
				AND ngay_dat_hang >= ip_fromDate) o ON od.ma_don_hang = o.tmdt_invoice_goc 
	SET 
		od.level_id = 7
	WHERE
		od.level_id < 7
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

    
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 2 
				AND ma_kenh_ban = 3
				AND ngay_dat_hang >= ip_fromDate) o ON od.ma_don_hang = o.tmdt_invoice_goc 
	SET 
		od.level_id = 8 
	WHERE
		od.level_id < 8 
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 2 
				AND ma_kenh_ban = 3
				AND ngay_dat_hang >= ip_fromDate) o ON od.ma_don_hang = o.tmdt_invoice_goc 
	SET 
		od.level_id = 8 
	WHERE
		od.level_id < 8 
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		staging_ivy_moda.mapping_level_don_hang_dat ml ON ml.trang_thai_don_hang = od.trang_thai_don_hang
			AND ml.id_nguon = 3 
	SET 
		od.level_id = ml.level_id
	WHERE
		od.level_id < 6 AND od.level_id <> 3
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    
	
    
	
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ma_kenh_ban = 3
				AND ngay_dat_hang >= ip_fromDate UNION SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ma_kenh_ban = 3
				AND ngay_dat_hang >= ip_fromDate) o ON od.ma_don_hang = o.tmdt_invoice_goc 
	SET 
		od.level_id = 0
	WHERE
		od.level_id IN (3 , 6)
			AND o.tmdt_invoice_goc IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
            
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', '4 ETL mã trạng thái đơn hàng đặt', 'Finish', ip_fromDate, ip_toDate,  '4 ETL mã trạng thái đơn hàng đặt');
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_shopee', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_don_hang_dat_chi_tiet_tiktok` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_don_hang_dat_chi_tiet_tiktok`( ip_fromDate date, ip_toDate date)
BEGIN
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '1 ETL mã tỉnh thành', 'Start', ip_fromDate, ip_toDate,  '1 ETL mã tỉnh thành');
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet t
	SET t.ma_tinh_thanh = 
		CASE
			WHEN province LIKE '%Hà Nội%' THEN 511
			WHEN province LIKE '%Quảng Ninh%' THEN 533
			WHEN province LIKE '%Thái Bình%' THEN 537
			WHEN province LIKE '%Son La%' THEN 535
			WHEN province LIKE '%Thanh Hoá%' THEN 539
			WHEN province LIKE '%Long An%' THEN 519
			WHEN province LIKE '%Ninh Binh%' THEN 525
			WHEN province LIKE '%Bắc Ninh%' THEN 491
			WHEN province LIKE '%Hải Dương%' THEN 508
			WHEN province LIKE '%Hải Phòng%' THEN 512
			WHEN province LIKE '%Nam Định%' THEN 526
			WHEN province LIKE '%Hồ Chí Minh%' THEN 507
			WHEN province LIKE '%Bac Giang%' THEN 487
			WHEN province LIKE '%Binh Phuoc%' THEN 492
			WHEN province LIKE '%Khanh Hoa%' THEN 517
			WHEN province LIKE '%Lang Son%' THEN 523
			WHEN province LIKE '%Lào Cai%' THEN 522
			WHEN province LIKE '%Thái Nguyên%' THEN 544
			WHEN province LIKE '%Tuyen Quang%' THEN 541
			WHEN province LIKE '%Quang Tri%' THEN 534
			WHEN province LIKE '%Gia Lai%' THEN 505
			WHEN province LIKE '%Vinh Phuc%' THEN 546
			WHEN province LIKE '%Soc Trang%' THEN 536
			WHEN province LIKE '%Yên Bái%' THEN 547
			WHEN province LIKE '%Bac Lieu%' THEN 490
			WHEN province LIKE '%Bến Tre%' THEN 495
			WHEN province LIKE '%Đồng Nai%' THEN 502
			WHEN province LIKE '%Phú Thọ%' THEN 528
			WHEN province LIKE '%Dak Lak%' THEN 551
			WHEN province LIKE '%Hà Nam%' THEN 510
			WHEN province LIKE '%Hà Tĩnh%' THEN 513
			WHEN province LIKE '%Kien Giang%' THEN 516
			WHEN province LIKE '%Bình Dương%' THEN 486
			WHEN province LIKE '%Bình Định%' THEN 488
			WHEN province LIKE '%Quảng Bình%' THEN 530
			WHEN province LIKE '%Lai Châu%' THEN 521
			WHEN province LIKE '%Binh Thuan%' THEN 494
			WHEN province LIKE '%Quảng Ngãi%' THEN 531
			WHEN province LIKE '%Hoa Binh%' THEN 506
			WHEN province LIKE '%Vĩnh Long%' THEN 545
			WHEN province LIKE '%Lâm Đồng%' THEN 520
			WHEN province LIKE '%Cần Thơ%' THEN 498
			WHEN province LIKE '%Hưng Yên%' THEN 515
			WHEN province LIKE '%Kon Tum%' THEN 518
			WHEN province LIKE '%Đồng Tháp%' THEN 504
			WHEN province LIKE '%Nghe An%' THEN 524
			WHEN province LIKE '%Quảng Nam%' THEN 532
			WHEN province LIKE '%Ha Giang%' THEN 509
			WHEN province LIKE '%Cà Mau%' THEN 497
			WHEN province LIKE '%Bắc Kạn%' THEN 549
			WHEN province LIKE '%Đà Nẵng%' THEN 499
			WHEN province LIKE '%An Giang%' THEN 485
			WHEN province LIKE '%Hậu Giang%' THEN 514
			WHEN province LIKE '%Trà Vinh%' THEN 543
			WHEN province LIKE '%Cao Bằng%' THEN 496
			WHEN province LIKE '%Dien Bien%' THEN 500
			WHEN province LIKE '%Phú Yên%' THEN 529
			WHEN province LIKE '%Tây Ninh%' THEN 540
			WHEN province LIKE '%Tiền Giang%' THEN 538
			WHEN province LIKE '%Dak Nong%' THEN 548
			WHEN province LIKE '%Ninh Thuan%' THEN 527
			WHEN province LIKE '%Lak%' THEN 551
			WHEN province LIKE '%Ba Ria%' THEN 552
			WHEN province LIKE '%Hue%' THEN 550
            ELSE 0
		END
	WHERE
		(t.ma_tinh_thanh IS NULL
			OR t.ma_tinh_thanh = 0)
			AND created_time BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet t
			JOIN
		olap_ivymoda.dim_khu_vuc p ON t.province = p.tinh_thanh 
	SET 
		t.ma_tinh_thanh = p.ma_tinh_thanh
	WHERE
		t.ma_tinh_thanh = 0
			AND created_time BETWEEN ip_fromDate AND ip_toDate;
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '1 ETL mã tỉnh thành', 'Finish', ip_fromDate, ip_toDate, '1 ETL mã tỉnh thành');
        

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '2 ETL mã phương thức thanh toán đơn hàng đặt', 'Start', ip_fromDate, ip_toDate, '2 ETL mã phương thức thanh toán đơn hàng đặt');
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet t
			JOIN
		olap_ivymoda.dim_pttt p ON t.payment_method = p.ten_pttt 
	SET 
		t.ma_pttt = p.ma_pttt
	WHERE
		t.ma_pttt = 0
			AND created_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '2 ETL mã phương thức thanh toán đơn hàng đặt', 'Finish', ip_fromDate, ip_toDate,  '2 ETL mã phương thức thanh toán đơn hàng đặt');


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả', 'Start', ip_fromDate, ip_toDate,  '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả');
	
	UPDATE staging_ivy_moda_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 5
	WHERE
		o.tmdt_type = 'Tiktok'
			AND o.ma_kenh_ban IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda_it.order_ecommerces 
	SET 
		tmdt_invoice_goc = LEFT(tmdt_invoice, 18)
	WHERE
		tmdt_invoice_goc IS NULL
			AND ma_kenh_ban = 5
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_metagent_it.order_ecommerces o 
	SET 
		o.ma_kenh_ban = 5
	WHERE
		o.tmdt_type = 'Tiktok'
			AND o.ma_kenh_ban IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_metagent_it.order_ecommerces 
	SET 
		tmdt_invoice_goc = LEFT(tmdt_invoice, 18)
	WHERE
		tmdt_invoice_goc IS NULL
			AND ma_kenh_ban = 5
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
    
	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		check_trang_thai_don_hang = 1
	WHERE
		check_trang_thai_don_hang = 0
			AND o.ma_kenh_ban = 5
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_ivy_moda_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		check_trang_thai_don_hang = 2
	WHERE
		check_trang_thai_don_hang < 2
			AND o.ma_kenh_ban = 5
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_metagent_it.order_ecommerces o
			JOIN
		(SELECT 
			IVM
		FROM
			staging_ivy_moda_it.ivm_info
		WHERE
			Ngay_Ct >= ip_fromDate) oht ON o.ivy_invoice = oht.IVM 
	SET 
		check_trang_thai_don_hang = 1
	WHERE
		check_trang_thai_don_hang = 0
			AND o.ma_kenh_ban = 5
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_metagent_it.order_ecommerces o
			JOIN
		(SELECT 
			order_ecommerce_id
		FROM
			staging_metagent_it.order_ecommerce_repays
		WHERE
			created_at >= ip_fromDate) orp ON o.id = orp.order_ecommerce_id 
	SET 
		check_trang_thai_don_hang = 2
	WHERE
		check_trang_thai_don_hang < 2
			AND o.ma_kenh_ban = 5
			AND o.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả', 'Finish', ip_fromDate, ip_toDate,  '3 ETL đơn hàng có trong bảng kê xuất và hoàn trả');



CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '4 ETL mã trạng thái đơn hàng đặt level_id', 'Start', ip_fromDate, ip_toDate,'4 ETL mã trạng thái đơn hàng đặt level_id');
	
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 1
				AND ma_kenh_ban = 5
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_id = o.tmdt_invoice_goc 
	SET 
		od.level_id = 7
	WHERE
		od.level_id < 7
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;	
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 1
				AND ma_kenh_ban = 5
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_id = o.tmdt_invoice_goc 
	SET 
		od.level_id = 7
	WHERE
		od.level_id < 7
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;	
    
    UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 2 
				AND ma_kenh_ban = 5
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_id = o.tmdt_invoice_goc 
	SET 
		od.level_id = 8 
	WHERE
		od.level_id < 8 
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
    UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			check_trang_thai_don_hang = 2 
				AND ma_kenh_ban = 5
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_id = o.tmdt_invoice_goc 
	SET 
		od.level_id = 8 
	WHERE
		od.level_id < 8 
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
	
    UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet t
			JOIN
		staging_ivy_moda.mapping_level_don_hang_dat ml ON ml.trang_thai_don_hang = t.order_status
			AND ml.id_nguon = 5 
	SET 
		t.level_id = ml.level_id
	WHERE
		t.level_id < 6 AND t.level_id <> 3
			AND created_time BETWEEN ip_fromDate AND ip_toDate;
	
    

    
	
	
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		(SELECT 
			tmdt_invoice_goc
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ma_kenh_ban = 5
				AND ngay_dat_hang >= ip_fromDate UNION SELECT 
			tmdt_invoice_goc
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ma_kenh_ban = 5
				AND ngay_dat_hang >= ip_fromDate) o ON od.order_id = o.tmdt_invoice_goc 
	SET 
		od.level_id = 0
	WHERE
		od.level_id IN (3 , 6)
			AND o.tmdt_invoice_goc IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', '4 ETL mã trạng thái đơn hàng đặt level_id', 'Finish', ip_fromDate, ip_toDate,'4 ETL mã trạng thái đơn hàng đặt level_id');
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_don_hang_dat_chi_tiet_tiktok', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_facebook_chi_phi_marketing` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_facebook_chi_phi_marketing`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
 
	
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_muc_dich = 1
	WHERE
		fb.ma_muc_dich IS NULL
			AND campaign_name LIKE '%Mess%'
            AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_muc_dich = 2
	WHERE
		fb.ma_muc_dich IS NULL
			AND campaign_name LIKE '%TTac%'
            AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_muc_dich = 0
	WHERE
		fb.ma_muc_dich IS NULL
        AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
    

	
    
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 2
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_Nhung\_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 1
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_Khoa\_%' AND campaign_name NOT LIKE '%_KhoaND_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 3
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_KhoaND\_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 4
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_Toàn\_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 5
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_Lethihuong\_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 6
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_Thượng\_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 7
	WHERE
		fb.ma_marketer IS NULL
			AND campaign_name LIKE '%\_hường\_%'
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_marketer = 0
	WHERE
		fb.ma_marketer IS NULL
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb
			JOIN
		olap_ivymoda.dim_page_marketings dm ON fb.account_id = dm.ma_tai_khoan 
	SET 
		fb.ma_page_marketings = dm.id
	WHERE
		fb.ma_page_marketings IS NULL
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb
	SET 
		fb.ma_page_marketings = 0
	WHERE
		fb.ma_page_marketings IS NULL
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
    
	UPDATE staging_ivy_moda.gd_facebook_chien_dich fb 
	SET 
		ma_team_ban_hang_chi_tiet = 1
	WHERE
		account_id = 1992553484308787
			AND ma_team_ban_hang_chi_tiet != 1
			AND fb.date_start BETWEEN ip_fromDate AND ip_toDate;
 
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_hoi_thoai_pancake` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_hoi_thoai_pancake`(ip_fromDate DATE, ip_toDate DATE)
BEGIN    
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_hoi_thoai_pancake', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

	UPDATE staging_ivy_moda.gd_pancake_hoi_thoai ht
			JOIN
		staging_ivy_moda.dm_nhan_vien_pancake nv ON ht.assign_user_id = nv.id_facebook 
	SET 
		ht.ma_nhan_vien_ban = nv.admin_id
	WHERE
		(ht.ma_nhan_vien_ban IS NULL
			OR ht.ma_nhan_vien_ban = 0)
			AND ht.updated_at BETWEEN ip_fromDate AND ip_toDate;

	UPDATE staging_ivy_moda.gd_pancake_hoi_thoai ht 
	SET 
		ht.ma_nhan_vien_ban = 0
	WHERE
		ht.ma_nhan_vien_ban IS NULL
			AND ht.updated_at BETWEEN ip_fromDate AND ip_toDate;
	
    UPDATE staging_ivy_moda.gd_pancake_hoi_thoai ht
			JOIN
		staging_ivy_moda.dm_pancake_page p ON ht.page_id = p.id 
	SET 
		ht.ma_page_marketings = p.ma_page_marketing
	WHERE
		(ht.ma_page_marketings IS NULL
			OR ht.ma_page_marketings = 0)
			AND ht.updated_at BETWEEN ip_fromDate AND ip_toDate;
            
	UPDATE staging_ivy_moda.gd_pancake_hoi_thoai ht 
	SET 
		ht.ma_page_marketings = 0
	WHERE
		ht.ma_page_marketings IS NULL
			AND ht.updated_at BETWEEN ip_fromDate AND ip_toDate;
            
	INSERT INTO staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai ( conversation_id, inserted_date, so_luot_nhan_tin ) 
	SELECT 
		conversation_id, inserted_date, COUNT(1) so_luot_nhan_tin
	FROM
		staging_ivy_moda.gd_pancake_hoi_thoai_chi_tiet
	WHERE
		role = 1
			AND inserted_date BETWEEN ip_fromDate AND ip_toDate
	GROUP BY conversation_id , inserted_date
		ON DUPLICATE KEY UPDATE so_luot_nhan_tin = so_luot_nhan_tin;
	
	UPDATE staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai tkht 
	SET 
		tkht.ma_page_marketings = 0
	WHERE
		tkht.ma_page_marketings IS NULL
			AND tkht.inserted_date BETWEEN ip_fromDate AND ip_toDate;
				
	UPDATE staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai tkht 
	SET 
		tkht.ma_nhan_vien_ban = 0
	WHERE
		tkht.ma_nhan_vien_ban IS NULL
			AND tkht.inserted_date BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_hoi_thoai_pancake', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');
PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_chi_phi_marketing` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_chi_phi_marketing`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
 

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_chi_phi_marketing', 'ETL all', 'Start', ip_fromDate, ip_toDate,'etl_job_chi_phi_marketing');
    
    CALL staging_ivy_moda.etl_facebook_chi_phi_marketing(ip_fromDate, ip_toDate);
    CALL olap_ivymoda.them_fact_chi_phi_marketing(ip_fromDate, ip_toDate);
    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_chi_phi_marketing', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'etl_job_chi_phi_marketing');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_don_hang_hoan` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_don_hang_hoan`(ip_fromDate date, ip_toDate date)
BEGIN    
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_hoan', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');
    CALL staging_ivy_moda_it.etl_orders_dat_hoan(ip_fromDate, ip_toDate);
    CALL staging_metagent_it.etl_orders_dat_hoan(ip_fromDate, ip_toDate);
	CALL staging_ivy_moda_it.etl_order_ecommerces_dat_hoan(ip_fromDate, ip_toDate);
    CALL staging_metagent_it.etl_order_ecommerces_dat_hoan(ip_fromDate, ip_toDate);
    CALL staging_ivy_moda_it.etl_orders_ivy_hoan(ip_fromDate, ip_toDate);
    CALL staging_metagent_it.etl_orders_ivy_hoan(ip_fromDate, ip_toDate);
	CALL staging_ivy_moda_it.etl_order_ecommerces_ivy_hoan(ip_fromDate, ip_toDate);
    CALL staging_metagent_it.etl_order_ecommerces_ivy_hoan(ip_fromDate, ip_toDate);

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_hoan', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_don_hang_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_don_hang_ivy`(ip_fromDate date, ip_toDate date)
BEGIN  
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_ivy', '1 ETL và đổ vào fact đơn hàng ivy Webapp', 'Start', ip_fromDate, ip_toDate,'procedure:etl_orders_ivy,them_fact_don_hang_ivy_webapp');
    call staging_ivy_moda_it.etl_orders_ivy(ip_fromDate,ip_toDate);
    call staging_ivy_moda_it.etl_orders_ineco_ivy(ip_fromDate,ip_toDate);
    call staging_metagent_it.etl_orders_ivy(ip_fromDate,ip_toDate);
	call olap_ivymoda.them_fact_don_hang_ivy_webapp(ip_fromDate,ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_ivy', '1 ETL và đổ vào fact đơn hàng ivy Webapp', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_orders_ivy,them_fact_don_hang_ivy_webapp');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_ivy', '2 ETL và đổ vào fact đơn hàng ivy TMDT', 'Start', ip_fromDate, ip_toDate,'procedure:etl_order_ecommerces_ivy;them_fact_don_hang_ivy_TMDT');
    call staging_ivy_moda_it.etl_order_ecommerces_ivy(ip_fromDate,ip_toDate);
    call staging_metagent_it.etl_order_ecommerces_ivy(ip_fromDate,ip_toDate);
    call olap_ivymoda.them_fact_don_hang_ivy_TMDT(ip_fromDate,ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_ivy', '2 ETL và đổ vào fact đơn hàng ivy TMDT', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_order_ecommerces_ivy;them_fact_don_hang_ivy_TMDT');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_don_hang_tmdt_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_don_hang_tmdt_dat`(ip_fromDate date, ip_toDate date)
BEGIN    
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', '1 ETL và đổ vào fact đơn hàng đặt Lazada', 'Start', ip_fromDate, ip_toDate,'procedure:etl_don_hang_dat_chi_tiet_lazada,them_fact_don_hang_dat_lazada');
	CALL staging_ivy_moda.etl_don_hang_dat_chi_tiet_lazada(ip_fromDate,ip_toDate);
	CALL olap_ivymoda.them_fact_don_hang_dat_lazada(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', '1 ETL và đổ vào fact đơn hàng đặt Lazada', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_don_hang_dat_chi_tiet_lazada,them_fact_don_hang_dat_lazada');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', '2 ETL và đổ vào fact đơn hàng đặt Shopee', 'Start', ip_fromDate, ip_toDate,'procedure:etl_don_hang_dat_chi_tiet_shopee,them_fact_don_hang_dat_shopee');
    CALL staging_ivy_moda.etl_don_hang_dat_chi_tiet_shopee(ip_fromDate,ip_toDate);
	CALL olap_ivymoda.them_fact_don_hang_dat_shopee(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', '2 ETL và đổ vào fact đơn hàng đặt Shopee', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_don_hang_dat_chi_tiet_shopee,them_fact_don_hang_dat_shopee');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', '3 ETL và đổ vào fact đơn hàng đặt Tiktok', 'Start', ip_fromDate, ip_toDate,'procedure:etl_don_hang_dat_chi_tiet_tiktok,them_fact_don_hang_dat_tiktok');
    CALL staging_ivy_moda.etl_don_hang_dat_chi_tiet_tiktok(ip_fromDate,ip_toDate);
    CALL olap_ivymoda.them_fact_don_hang_dat_tiktok(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', '3 ETL và đổ vào fact đơn hàng đặt Tiktok', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_don_hang_dat_chi_tiet_tiktok,them_fact_don_hang_dat_tiktok');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_tmdt_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_don_hang_webapp_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_don_hang_webapp_dat`(ip_fromDate date, ip_toDate date)
BEGIN    
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_webapp_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_webapp_dat', '4 ETL và đổ vào fact đơn hàng đặt Webapp', 'Start', ip_fromDate, ip_toDate,'procedure:etl_orders_dat;them_fact_don_hang_dat_webapp');
    CALL staging_ivy_moda_it.etl_orders_dat(ip_fromDate, ip_toDate);
    CALL staging_ivy_moda_it.etl_orders_ineco_dat(ip_fromDate, ip_toDate);
    CALL staging_metagent_it.etl_orders_dat(ip_fromDate, ip_toDate);
    CALL olap_ivymoda.them_fact_don_hang_dat_webapp(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_webapp_dat', '4 ETL và đổ vào fact đơn hàng đặt Webapp', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_orders_dat;them_fact_don_hang_dat_webapp');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_don_hang_webapp_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_hoi_thoai_pancake` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_hoi_thoai_pancake`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
 
	CALL staging_ivy_moda.etl_hoi_thoai_pancake(ip_fromDate, ip_toDate);
	CALL staging_ivy_moda.etl_thong_ke_hoi_thoai_pancake(ip_fromDate, ip_toDate);
	CALL olap_ivymoda.them_fact_hoi_thoai_pancake(ip_fromDate, ip_toDate);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_redundant_data` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_redundant_data`(ip_fromDate date, ip_toDate date)
BEGIN    
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_redundant_data', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

CALL staging_ivy_moda_it.etl_redundant_data(ip_fromDate, ip_toDate);

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_redundant_data', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_so_luong_san_pham_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_so_luong_san_pham_dat`(ip_fromDate date, ip_toDate date)
BEGIN
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Lazada', 'Start', ip_fromDate, ip_toDate,'procedure:etl_so_luong_san_pham_dat_lazada,them_fact_so_luong_sp_dat_lazada');
	call staging_ivy_moda.etl_so_luong_san_pham_dat_lazada(ip_fromDate,ip_toDate);
	call olap_ivymoda.them_fact_so_luong_sp_dat_lazada(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Lazada', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_so_luong_san_pham_dat_lazada,them_fact_so_luong_sp_dat_lazada');
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Shopee', 'Start', ip_fromDate, ip_toDate,'procedure:etl_so_luong_san_pham_dat_shopee;them_fact_so_luong_sp_dat_shopee');
    call staging_ivy_moda.etl_so_luong_san_pham_dat_shopee(ip_fromDate,ip_toDate);
	call olap_ivymoda.them_fact_so_luong_sp_dat_shopee(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Shopee', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_so_luong_san_pham_dat_shopee;them_fact_so_luong_sp_dat_shopee');
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Tiktok', 'Start', ip_fromDate, ip_toDate,'procedure:etl_so_luong_san_pham_dat_tiktok;them_fact_so_luong_sp_dat_tiktok');
    call staging_ivy_moda.etl_so_luong_san_pham_dat_tiktok(ip_fromDate,ip_toDate);
    call olap_ivymoda.them_fact_so_luong_sp_dat_tiktok(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Tiktok', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_so_luong_san_pham_dat_tiktok;them_fact_so_luong_sp_dat_tiktok');
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Webapp', 'Start', ip_fromDate, ip_toDate,'procedure:etl_orders_product_dat;them_fact_so_luong_sp_dat_webapp');
    call staging_ivy_moda_it.etl_orders_product_dat(ip_fromDate, ip_toDate);
    call staging_ivy_moda_it.etl_order_ineco_products_dat(ip_fromDate, ip_toDate);
    call staging_metagent_it.etl_orders_product_dat(ip_fromDate, ip_toDate);
	call olap_ivymoda.them_fact_so_luong_sp_dat_webapp(ip_fromDate, ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', '1 ETL và đổ vào fact số lượng sản phẩm đặt Webapp', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_orders_product_dat;them_fact_so_luong_sp_dat_webapp');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_dat', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_so_luong_san_pham_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_so_luong_san_pham_ivy`(ip_fromDate date, ip_toDate date)
BEGIN
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_ivy', 'ETL all', 'Start', ip_fromDate, ip_toDate,'ETL all');

	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_ivy', '1 ETL và đổ vào fact số lượng sản phẩm ivy Webapp', 'Start', ip_fromDate, ip_toDate,'procedure:etl_orders_product_ivy;them_fact_so_luong_sp_ivy_webapp');
    call staging_ivy_moda_it.etl_orders_product_ivy(ip_fromDate,ip_toDate);
    call staging_ivy_moda_it.etl_order_ineco_products_ivy(ip_fromDate,ip_toDate);
    call staging_metagent_it.etl_orders_product_ivy(ip_fromDate,ip_toDate);
	call olap_ivymoda.them_fact_so_luong_sp_ivy_webapp(ip_fromDate,ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_ivy', '1 ETL và đổ vào fact số lượng sản phẩm ivy Webapp', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_orders_product_ivy;them_fact_so_luong_sp_ivy_webapp');
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_ivy', '1 ETL và đổ vào fact số lượng sản phẩm ivy TMDT', 'Start', ip_fromDate, ip_toDate,'procedure:etl_order_ecommerce_products;them_fact_so_luong_sp_ivy_TMDT');
    call staging_ivy_moda_it.etl_order_ecommerce_products(ip_fromDate,ip_toDate);
    call staging_metagent_it.etl_order_ecommerce_products(ip_fromDate,ip_toDate);
    call olap_ivymoda.them_fact_so_luong_sp_ivy_TMDT(ip_fromDate,ip_toDate);
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_ivy', '1 ETL và đổ vào fact số lượng sản phẩm ivy TMDT', 'Finish', ip_fromDate, ip_toDate,'procedure:etl_order_ecommerce_products;them_fact_so_luong_sp_ivy_TMDT');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_job_so_luong_san_pham_ivy', 'ETL all', 'Finish', ip_fromDate, ip_toDate,'ETL all');

PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_job_them_dimension` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_job_them_dimension`()
BEGIN    
 
	CALL olap_ivymoda.them_dim_sp();
    CALL olap_ivymoda.them_dim_nhan_vien_ban_hang();
    CALL olap_ivymoda.them_dim_khu_vuc();
    CALL olap_ivymoda.them_dim_page_marketings();
PURGE BINARY LOGS BEFORE '2050-01-01';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_san_pham_dat_lazada` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_san_pham_dat_lazada`( ip_fromDate date, ip_toDate date)
BEGIN

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '1 ETL loại đơn', 'Start', ip_fromDate, ip_toDate, '1 ETL loại đơn');
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet 
	SET 
		loai_don = 1
	WHERE
		loai_don IS NULL
			AND ((status = 'canceled'
			AND buyer_failed_delivery_return_initiator = 'buyer-cancel'
			AND buyer_failed_delivery_reason NOT IN ('Nhà bán hàng yêu cầu hủy' , 'Thời gian giao hàng quá lâu'))
			OR (status = 'returned'
			AND buyer_failed_delivery_reason IN ('Đổi ý' , 'Trùng đơn hàng/ Đặt nhầm',
			'Không có nhu cầu nữa')))
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
	        
	
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '1 ETL loại đơn', 'Finish', ip_fromDate, ip_toDate, '1 ETL loại đơn');    
 

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '2 ETL thông tin sản phẩm', 'Start', ip_fromDate, ip_toDate, '2 ETL thông tin sản phẩm');
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet 
	SET 
		seller_sku = '00ZZZ00000000000'
	WHERE
		LENGTH(seller_sku) != 16
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet 
	SET 
		ma_7 = CONCAT(SUBSTRING(seller_sku, 1, 2),
				SUBSTRING(seller_sku, 5, 1),
				SUBSTRING(seller_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(seller_sku, 1, 2),
		ma_size = SUBSTRING(seller_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(seller_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(seller_sku, 5, 1),
		ma_san_xuat = SUBSTRING(seller_sku, 6, 4),
		ma_mau_sac = SUBSTRING(seller_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_nguon_hang n ON od.ma_nguon_hang = n.ma_nguon_hang 
	SET 
		od.ma_nguon_hang = 'Z',
		od.ma_7 = '00Z0000'
	WHERE
		n.ma_nguon_hang IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_nhan_sp n ON od.ma_nhan_sp = n.ma_nhan_sp 
	SET 
		od.ma_nhan_sp = 'Z',
		od.ma_7 = '00Z0000'
	WHERE
		n.ma_nhan_sp IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_size n ON od.ma_size = n.ma_size 
	SET 
		od.ma_size = 'Z'
	WHERE
		n.ma_size IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '2 ETL thông tin sản phẩm', 'Finish', ip_fromDate, ip_toDate, '2 ETL thông tin sản phẩm');    

 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '3 Thêm sản phẩm thiếu', 'Start', ip_fromDate, ip_toDate, '3 Thêm sản phẩm thiếu');
	    
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_mau_sac n ON od.ma_mau_sac = n.ma_mau_sac 
	SET 
		od.ma_mau_sac = '000'
	WHERE
		n.ma_mau_sac IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;

	INSERT IGNORE olap_ivymoda.dim_sp7
	SELECT DISTINCT
		od.ma_7, od.ma_nhom_sp, od.ma_nhan_sp
	FROM
		staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_sp7 n ON od.ma_7 = n.ma_7
	WHERE
		n.ma_7 IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;

	
	INSERT IGNORE olap_ivymoda.dim_sp
	SELECT DISTINCT
		od.seller_sku,
		od.ma_7,
		od.ma_nhom_sp,
		od.ma_size,
		od.ma_nguon_hang,
		od.ma_nhan_sp,
		od.ma_san_xuat,
		od.ma_mau_sac
	FROM
		staging_ivy_moda.gd_lazada_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_sp n ON od.seller_sku = n.ma_sp
	WHERE
		n.ma_sp IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '3 Thêm sản phẩm thiếu', 'Finish', ip_fromDate, ip_toDate, '3 Thêm sản phẩm thiếu');    


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '4 ETL chương trình bán', 'Start', ip_fromDate, ip_toDate, '4 ETL chương trình bán');
	UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet 
	SET 
		chuong_trinh_ban = SUBSTRING(item_name,
			LOCATE('[', item_name),
			LOCATE(']', item_name) - LOCATE('[', item_name) + 1)
	WHERE
		chuong_trinh_ban IS NULL
			AND create_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', '4 ETL chương trình bán', 'Finish', ip_fromDate, ip_toDate, '4 ETL chương trình bán');    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_lazada', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_san_pham_dat_shopee` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_san_pham_dat_shopee`(ip_fromDate date, ip_toDate date)
BEGIN

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', '1 Cập nhật loại đơn chi tiết', 'Start', ip_fromDate, ip_toDate, '1 Cập nhật loại đơn chi tiết');
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od 
	SET 
		loai_don = 0
	WHERE
		trang_thai_don_hang != 'Đã huỷ'
			AND loai_don IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			JOIN
		staging_ivy_moda_it.order_ecommerces o ON od.ma_don_hang = o.tmdt_invoice 
	SET 
		od.loai_don = 0
	WHERE
		trang_thai_don_hang = 'Đã huỷ'
			AND od.loai_don IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od 
	SET 
		od.loai_don = 1
	WHERE
		od.loai_don IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', '1 Cập nhật loại đơn chi tiết', 'Finish', ip_fromDate, ip_toDate, '1 Cập nhật loại đơn chi tiết');    


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', '2 ETL thông tin sản phẩm', 'Start', ip_fromDate, ip_toDate, '2 ETL thông tin sản phẩm');
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet 
	SET 
		sku_phan_loai_hang = '00ZZZ00000000000'
	WHERE
		LENGTH(sku_phan_loai_hang) != 16
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet 
	SET 
		ma_7 = CONCAT(SUBSTRING(sku_phan_loai_hang, 1, 2),
				SUBSTRING(sku_phan_loai_hang, 5, 1),
				SUBSTRING(sku_phan_loai_hang, 6, 4)),
		ma_nhom_sp = SUBSTRING(sku_phan_loai_hang, 1, 2),
		ma_size = SUBSTRING(sku_phan_loai_hang, 3, 1),
		ma_nguon_hang = SUBSTRING(sku_phan_loai_hang, 4, 1),
		ma_nhan_sp = SUBSTRING(sku_phan_loai_hang, 5, 1),
		ma_san_xuat = SUBSTRING(sku_phan_loai_hang, 6, 4),
		ma_mau_sac = SUBSTRING(sku_phan_loai_hang, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_nguon_hang n ON od.ma_nguon_hang = n.ma_nguon_hang 
	SET 
		od.ma_nguon_hang = 'Z',
		od.ma_7 = '00Z0000'
	WHERE
		n.ma_nguon_hang IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_nhan_sp n ON od.ma_nhan_sp = n.ma_nhan_sp 
	SET 
		od.ma_nhan_sp = 'Z',
		od.ma_7 = '00Z0000'
	WHERE
		n.ma_nhan_sp IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_size n ON od.ma_size = n.ma_size 
	SET 
		od.ma_size = 'Z'
	WHERE
		n.ma_size IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', '2 ETL thông tin sản phẩm', 'Finish', ip_fromDate, ip_toDate, '2 ETL thông tin sản phẩm');    

 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', '3 Thêm sản phẩm thiếu', 'Start', ip_fromDate, ip_toDate, '3 Thêm sản phẩm thiếu');
	    
	UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_mau_sac n ON od.ma_mau_sac = n.ma_mau_sac 
	SET 
		od.ma_mau_sac = '000'
	WHERE
		n.ma_mau_sac IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	INSERT IGNORE olap_ivymoda.dim_sp7
	SELECT DISTINCT
		od.ma_7, od.ma_nhom_sp, od.ma_nhan_sp
	FROM
		staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_sp7 n ON od.ma_7 = n.ma_7
	WHERE
		n.ma_7 IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;

	
	INSERT IGNORE olap_ivymoda.dim_sp
	SELECT DISTINCT
		od.sku_phan_loai_hang,
		od.ma_7,
		od.ma_nhom_sp,
		od.ma_size,
		od.ma_nguon_hang,
		od.ma_nhan_sp,
		od.ma_san_xuat,
		od.ma_mau_sac
	FROM
		staging_ivy_moda.gd_shopee_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_sp n ON od.sku_phan_loai_hang = n.ma_sp
	WHERE
		n.ma_sp IS NULL
			AND od.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', '3 Thêm sản phẩm thiếu', 'Finish', ip_fromDate, ip_toDate, '3 Thêm sản phẩm thiếu');    

CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_shopee', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_san_pham_dat_tiktok` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_san_pham_dat_tiktok`(ip_fromDate date, ip_toDate date)
BEGIN
 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_tiktok', 'ETL all', 'Start', ip_fromDate, ip_toDate, 'ETL all');


CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_tiktok', '1 ETL thông tin sản phẩm', 'Start', ip_fromDate, ip_toDate, '1 ETL thông tin sản phẩm');
	
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet 
	SET 
		seller_sku = '00ZZZ00000000000'
	WHERE
		(LENGTH(seller_sku) != 16
			OR seller_sku IS NULL)
			AND created_time BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet 
	SET 
		ma_7 = CONCAT(SUBSTRING(seller_sku, 1, 2),
				SUBSTRING(seller_sku, 5, 1),
				SUBSTRING(seller_sku, 6, 4)),
		ma_nhom_sp = SUBSTRING(seller_sku, 1, 2),
		ma_size = SUBSTRING(seller_sku, 3, 1),
		ma_nguon_hang = SUBSTRING(seller_sku, 4, 1),
		ma_nhan_sp = SUBSTRING(seller_sku, 5, 1),
		ma_san_xuat = SUBSTRING(seller_sku, 6, 4),
		ma_mau_sac = SUBSTRING(seller_sku, 10, 3)
	WHERE
		ma_nhom_sp IS NULL
			AND created_time BETWEEN ip_fromDate AND ip_toDate;
	
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_nguon_hang n ON od.ma_nguon_hang = n.ma_nguon_hang 
	SET 
		od.ma_nguon_hang = 'Z',
		od.ma_7 = '00Z0000'
	WHERE
		n.ma_nguon_hang IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_nhan_sp n ON od.ma_nhan_sp = n.ma_nhan_sp 
	SET 
		od.ma_nhan_sp = 'Z',
		od.ma_7 = '00Z0000'
	WHERE
		n.ma_nhan_sp IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
		
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_size n ON od.ma_size = n.ma_size 
	SET 
		od.ma_size = 'Z'
	WHERE
		n.ma_size IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_tiktok', '1 ETL thông tin sản phẩm', 'Finish', ip_fromDate, ip_toDate, '1 ETL thông tin sản phẩm');    

 
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_tiktok', '2 Thêm sản phẩm thiếu trong olap', 'Start', ip_fromDate, ip_toDate, '2 Thêm sản phẩm thiếu trong olap');
	    
	UPDATE staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_mau_sac n ON od.ma_mau_sac = n.ma_mau_sac 
	SET 
		od.ma_mau_sac = '000'
	WHERE
		n.ma_mau_sac IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;

	INSERT IGNORE olap_ivymoda.dim_sp7
	SELECT DISTINCT
		od.ma_7, od.ma_nhom_sp, od.ma_nhan_sp
	FROM
		staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_sp7 n ON od.ma_7 = n.ma_7
	WHERE
		n.ma_7 IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;

	
	INSERT IGNORE olap_ivymoda.dim_sp
	SELECT DISTINCT
		od.seller_sku,
		od.ma_7,
		od.ma_nhom_sp,
		od.ma_size,
		od.ma_nguon_hang,
		od.ma_nhan_sp,
		od.ma_san_xuat,
		od.ma_mau_sac
	FROM
		staging_ivy_moda.gd_tiktok_don_hang_chi_tiet od
			LEFT JOIN
		olap_ivymoda.dim_sp n ON od.seller_sku = n.ma_sp
	WHERE
		n.ma_sp IS NULL
			AND od.created_time BETWEEN ip_fromDate AND ip_toDate;
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_tiktok', '2 Thêm sản phẩm thiếu trong olap', 'Finish', ip_fromDate, ip_toDate, '2 Thêm sản phẩm thiếu trong olap');    
CALL data_warehouse.track_log_procedure
('staging_ivy_moda', 'etl_so_luong_san_pham_dat_tiktok', 'ETL all', 'Finish', ip_fromDate, ip_toDate, 'ETL all');
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_san_pham_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_san_pham_ivy`()
BEGIN
    call staging_ivy_moda.etl_so_luong_sp_ivy_lazada();
    call staging_ivy_moda.etl_so_luong_sp_ivy_shopee();
    call staging_ivy_moda.etl_so_luong_sp_ivy_webapp();
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_sp_ivy_lazada` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_sp_ivy_lazada`()
BEGIN

 UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet ct
left JOIN 
staging_ivy_moda.gd_ams3_don_hang_lazada l ON ct.order_number =l.ma_don_hang_goc
SET ct.level_id_ivy = IFNULL(l.level_id, 0)	
WHERE
		ct.level_id_ivy IS NULL;


 UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet ct
left JOIN 
staging_ivy_moda.gd_ams3_don_hang_lazada l ON ct.order_number =l.ma_don_hang_goc
SET ct.ma_dvvc = IFNULL(l.ma_dvvc, 0)	
WHERE
		ct.ma_dvvc IS NULL;


 UPDATE staging_ivy_moda.gd_lazada_don_hang_chi_tiet ct
left JOIN 
staging_ivy_moda.gd_ams3_don_hang_lazada l ON ct.order_number =l.ma_don_hang_goc
SET ct.ma_kho = IFNULL(l.ma_kho, 0)	
WHERE
		ct.ma_kho IS NULL;


END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_sp_ivy_shopee` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_sp_ivy_shopee`()
BEGIN

UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet ct
        LEFT JOIN
    staging_ivy_moda.gd_ams3_don_hang_shopee s ON ct.ma_don_hang = s.ma_don_hang_goc 
SET 
    ct.level_id_ivy = IFNULL(s.level_id, 0)
WHERE
    ct.level_id_ivy IS NULL;
   

UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet ct
        LEFT JOIN
    staging_ivy_moda.gd_ams3_don_hang_shopee s ON ct.ma_don_hang = s.ma_don_hang_goc 
SET 
    ct.ma_dvvc = IFNULL(s.ma_dvvc, 0)
WHERE
    ct.ma_dvvc IS NULL;
     

UPDATE staging_ivy_moda.gd_shopee_don_hang_chi_tiet ct
        LEFT JOIN
    staging_ivy_moda.gd_ams3_don_hang_shopee s ON ct.ma_don_hang = s.ma_don_hang_goc 
SET 
    ct.ma_kho = IFNULL(s.ma_kho, 0)
WHERE
    ct.ma_kho IS NULL;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_so_luong_sp_ivy_webapp` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_so_luong_sp_ivy_webapp`()
BEGIN

UPDATE  staging_ivy_moda.gd_ams3_don_hang_chi_tiet_webapp od
LEFT JOIN
 staging_ivy_moda.gd_ams3_don_hang_webapp o on od.ivy_invoice= o.ivy_invoice
SET 
    od.ma_kho = IFNULL(o.ma_kho, 0)
WHERE
    od.ma_kho IS NULL;


UPDATE  staging_ivy_moda.gd_ams3_don_hang_chi_tiet_webapp od
LEFT JOIN
 staging_ivy_moda.gd_ams3_don_hang_webapp o on od.ivy_invoice= o.ivy_invoice
SET 
    od.ma_dvvc = IFNULL(o.ma_dvvc, 0)
WHERE
    od.ma_dvvc IS NULL;
 
UPDATE  staging_ivy_moda.gd_ams3_don_hang_chi_tiet_webapp od
LEFT JOIN
 staging_ivy_moda.gd_ams3_don_hang_webapp o on od.ivy_invoice= o.ivy_invoice
SET 
    od.level_id_ivy = IFNULL(o.level_id_ivy, 0)
WHERE
    od.level_id_ivy IS NULL;   

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_thong_ke_hoi_thoai_pancake` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `etl_thong_ke_hoi_thoai_pancake`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
 
	UPDATE staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai tkht 
	SET 
		tkht.check_hoi_thoai = 0
	WHERE
		(tkht.conversation_id , tkht.inserted_date) IN (SELECT 
				tkht0.conversation_id, tkht0.inserted_date
			FROM
				(SELECT 
					tkht1.conversation_id, tkht1.inserted_date
				FROM
					(SELECT 
					tkht2.conversation_id AS conversation_id,
						tkht2.inserted_date AS inserted_date
				FROM
					gd_pancake_thong_ke_hoi_thoai tkht2
				WHERE
					EXISTS( SELECT 
							1
						FROM
							gd_pancake_thong_ke_hoi_thoai tkht3
						WHERE
							((tkht3.conversation_id = tkht2.conversation_id)
								AND (tkht3.inserted_date BETWEEN (tkht2.inserted_date - INTERVAL 6 DAY) AND (tkht2.inserted_date - INTERVAL 1 DAY))
								AND (tkht3.check_hoi_thoai = 1)))
						AND tkht2.inserted_date BETWEEN ip_fromDate AND ip_toDate) AS tkht1) AS tkht0)
			AND tkht.inserted_date BETWEEN ip_fromDate AND ip_toDate;

	WHILE
		EXISTS( SELECT 
					1
				FROM
					staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai
				WHERE
					check_hoi_thoai IS NULL
						AND inserted_date BETWEEN ip_fromDate AND ip_toDate) DO
					
		UPDATE staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai tkht
				JOIN
			(SELECT 
				conversation_id, MIN(inserted_date) inserted_date
			FROM
				staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai
			WHERE
				check_hoi_thoai IS NULL
					AND inserted_date BETWEEN ip_fromDate AND ip_toDate
			GROUP BY conversation_id) ht ON (tkht.conversation_id = ht.conversation_id
				AND tkht.inserted_date = ht.inserted_date) 
		SET 
			tkht.check_hoi_thoai = 1
		WHERE
			tkht.check_hoi_thoai IS NULL
				AND tkht.inserted_date BETWEEN ip_fromDate AND ip_toDate;

		UPDATE staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai tkht 
		SET 
			tkht.check_hoi_thoai = 0
		WHERE
			(tkht.conversation_id , tkht.inserted_date) IN (SELECT 
					tkht0.conversation_id, tkht0.inserted_date
				FROM
					(SELECT 
						tkht1.conversation_id, tkht1.inserted_date
					FROM
						(SELECT 
						tkht2.conversation_id AS conversation_id,
							tkht2.inserted_date AS inserted_date
					FROM
						gd_pancake_thong_ke_hoi_thoai tkht2
					WHERE
						EXISTS( SELECT 
								1
							FROM
								gd_pancake_thong_ke_hoi_thoai tkht3
							WHERE
								((tkht3.conversation_id = tkht2.conversation_id)
									AND (tkht3.inserted_date BETWEEN (tkht2.inserted_date - INTERVAL 6 DAY) AND (tkht2.inserted_date - INTERVAL 1 DAY))
									AND (tkht3.check_hoi_thoai = 1)))
							AND tkht2.inserted_date BETWEEN ip_fromDate AND ip_toDate) AS tkht1) AS tkht0)
				AND tkht.inserted_date BETWEEN ip_fromDate AND ip_toDate;
	END WHILE;
    
	UPDATE staging_ivy_moda.gd_pancake_thong_ke_hoi_thoai tkht
			JOIN
		staging_ivy_moda.gd_pancake_hoi_thoai ht ON tkht.conversation_id = ht.id 
	SET 
		tkht.customer_fb_id = ht.customer_fb_id,
		tkht.ma_nhan_vien_ban = ht.ma_nhan_vien_ban,
		tkht.ma_page_marketings = ht.ma_page_marketings
	WHERE
		tkht.inserted_date BETWEEN ip_fromDate AND ip_toDate;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_email_mkt` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_email_mkt`()
BEGIN
	SELECT date_export as 'RecentDate'   FROM staging_ivy_moda.gd_email_mkt_chi_phi
	ORDER BY date_export DESC
	LIMIT 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_google_ads` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_google_ads`()
BEGIN



	

	SELECT ngay_bat_dau as 'RecentDate'  FROM staging_ivy_moda.gd_google_chien_dich

	ORDER BY ngay_bat_dau DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_kpi` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_kpi`()
BEGIN

	

	

	SELECT create_time  as 'RecentDate'  FROM staging_ivy_moda.gd_lazada_don_hang_chi_tiet

	ORDER BY create_time DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_lazada_ads` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_lazada_ads`()
BEGIN



	

	SELECT date_start as 'RecentDate'  FROM staging_ivy_moda.gd_lazada_chien_dich

	ORDER BY date_start DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_lazada_customers` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_lazada_customers`()
BEGIN

	

SELECT create_time as 'RecentDate'   FROM user_core.tmp_lazada_customers

	ORDER BY create_time DESC

	LIMIT 1;



END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_lazada_order_detail` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_lazada_order_detail`()
BEGIN



	

	SELECT create_time  as 'RecentDate'  FROM staging_ivy_moda.gd_lazada_don_hang_chi_tiet

	ORDER BY create_time DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_shopee_ads` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_shopee_ads`()
BEGIN



	SELECT ngay_ket_thuc as 'RecentDate'   FROM staging_ivy_moda.gd_shopee_chien_dich

	ORDER BY ngay_ket_thuc DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_shopee_order_detail` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_shopee_order_detail`()
BEGIN



	

	SELECT ngay_dat_hang as 'RecentDate'  FROM staging_ivy_moda.gd_shopee_don_hang_chi_tiet

	ORDER BY ngay_dat_hang DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_tiktok_customers` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_tiktok_customers`()
BEGIN

	

	

	SELECT created_time as 'RecentDate'   FROM user_core.tmp_tiktok_customers

	ORDER BY created_time DESC

	LIMIT 1;



END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `GetRecentDate_tiktok_order_detail` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `GetRecentDate_tiktok_order_detail`()
BEGIN



	

	SELECT created_time as 'RecentDate'   FROM staging_ivy_moda.gd_tiktok_don_hang_chi_tiet

	ORDER BY created_time DESC

	LIMIT 1;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_address` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `get_address`()
BEGIN
	SELECT 
    ten_tinh_thanh, ten_quan_huyen
FROM
    staging_ivy_moda.dm_quan_huyen
        JOIN
    dm_tinh_thanh ON dm_quan_huyen.tinh_thanh_id = dm_tinh_thanh.tinh_thanh_id
ORDER BY ten_quan_huyen;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_log_etl` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `insert_log_etl`(userName varchar(45), databaseName varchar(45) , tableName varchar(45), numOfRecord int, status varchar(10), fromDate date,  toDate date, description varchar(500))
BEGIN
	insert into data_warehouse.log_data_warehose(logDate, userName, databaseName, tableName, numOfRecord, status, fromDate, toDate, description)
    values (NOW(), username, databaseName, tableName, numOfRecord, status, fromDate, toDate, description) ;
    END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_pancake_access_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `insert_pancake_access_token`()
insert into data_warehouse.pancake_access_token (access_token,created_at) value ("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1aWQiOiIyNWU2Njc0MS00NTljLTRmOTAtYmVmOC0wNDAzZGY1ZjcyZGQiLCJsb2dpbl9zZXNzaW9uIjpudWxsLCJpYXQiOjE3MDIwMDY5MzIsImZiX25hbWUiOiJQb3dlciBCSSBJdnkiLCJmYl9pZCI6IjEyMjExMTc3OTQ5NjEyNjMxNSIsImV4cCI6MTcwOTc4MjkzMn0.SxXpbyswXBIBPu8MfUJQ0G3kRnIL_eBgPNWZ_pDOLqE", now()) ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_lazada_don_hang` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `them_lazada_don_hang`()
BEGIN

INSERT INTO `staging_ivy_moda`.`gd_lazada_don_hang`
(`ma_don_hang`,
`ma_van_don`,
`trang_thai_don_hang`,
`ngay_dat_hang`,
`tong_tien`)

SELECT 
min(order_number) as ma_don_hang,
min(tracking_code) as ma_van_don
,min(status )as trang_thai_don_hang
,left( max(create_time),10 )  as ngay_dat_hang
, (sum(unit_price) - sum(seller_discount_total) ) as tong_tien

FROM gd_lazada_don_hang_chi_tiet t1 left join gd_lazada_don_hang t2
on t1.order_number = t2.ma_don_hang



where t2.trang_thai_don_hang is null

group by t1.order_number
;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_shopee_don_hang` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `them_shopee_don_hang`()
BEGIN

	

insert into gd_shopee_don_hang (ma_don_hang,ma_van_don,ngay_dat_hang,trang_thai_don_hang,tong_tien,voucher,

tien_voucher,nguoi_nhan, phone,ma_khach_hang_kenh_ban)



select DISTINCT t1.ma_don_hang, min(t1.ma_van_don) as ma_van_don, min( t1.ngay_dat_hang) as ngay_dat_hang

, min( t1.trang_thai_don_hang) as trang_thai_don_hang,

sum(t1.tong_gia_ban_san_pham - t1.ma_giam_gia_cua_shop) as tong_tien,

null as voucher, min(t1.ma_giam_gia_cua_shop) as tien_voucher, min( t1.ten_nguoi_nhan) as ten_nguoi_nhan,

min( t1.so_dien_thoai) as phone, min( t1.ma_kh) as ma_khach_hang_kenh_ban

from gd_shopee_don_hang_chi_tiet t1 LEFT JOIN

gd_shopee_don_hang t2 ON

t1.ma_don_hang = t2.ma_don_hang



WHERE t2.trang_thai_don_hang is NULL

GROUP BY t1.ma_don_hang































;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `update_percent_shopee_ads` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`ivymoda`@`%` PROCEDURE `update_percent_shopee_ads`()
BEGIN
	
	update staging_ivy_moda.gd_shopee_chien_dich
    set ty_le_click = ty_le_click/100
		,ty_le_chuyen_doi = ty_le_chuyen_doi /100
        ,ty_le_chuyen_doi_truc_tiep =ty_le_chuyen_doi_truc_tiep/100
        ,acos = acos/100
        , acos_truc_tiep = acos_truc_tiep/100
        ,is_updated = 1
	where is_updated != 1
        ;
    


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

-- Dump completed on 2025-06-14 23:16:54
