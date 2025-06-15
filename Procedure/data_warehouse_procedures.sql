-- MySQL dump 10.13  Distrib 9.0.1, for Win64 (x86_64)
--
-- Host: 103.141.144.236    Database: data_warehouse
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
-- Dumping routines for database 'data_warehouse'
--
/*!50003 DROP PROCEDURE IF EXISTS `check_hoi_thoai_pancake` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_hoi_thoai_pancake`(IN ip_pageMarketingId INT, ip_fromDate DATE, ip_toDate DATE)
BEGIN

	DROP TABLE IF EXISTS staging_ivy_moda.danh_sach_hoi_thoai_thieu;
	CREATE TEMPORARY TABLE staging_ivy_moda.danh_sach_hoi_thoai_thieu AS
		SELECT
			*
		FROM
			staging_ivy_moda.gd_pancake_hoi_thoai_chi_tiet_excel
		WHERE
			conversation_id NOT IN (SELECT DISTINCT
					conversation_id
				FROM
					staging_ivy_moda.gd_pancake_hoi_thoai_chi_tiet
				WHERE
					inserted_date BETWEEN ip_fromDate AND ip_toDate)
				AND inserted_date BETWEEN ip_fromDate AND ip_toDate
				AND ma_page_marketing = ip_pageMarketingId;
    
    SELECT * FROM staging_ivy_moda.danh_sach_hoi_thoai_thieu;
    
	SELECT 
		ht.*
	FROM
		staging_ivy_moda.gd_pancake_hoi_thoai ht
			JOIN
		(SELECT DISTINCT
			conversation_id
		FROM
			staging_ivy_moda.danh_sach_hoi_thoai_thieu) htt ON ht.id = htt.conversation_id
	WHERE
		ht.ma_page_marketings = ip_pageMarketingId;
	
	SELECT 
		htt.*
	FROM
		staging_ivy_moda.gd_pancake_hoi_thoai ht
			RIGHT JOIN
		staging_ivy_moda.danh_sach_hoi_thoai_thieu htt ON ht.id = htt.conversation_id
	WHERE
		ht.id IS NULL;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `check_inf_orders` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_inf_orders`(IN ip_ivy_invoice VARCHAR(25))
BEGIN
-- Tạo procedure hỗ trợ kiểm tra đơn hàng
-- Mục đích: Kiểm tra đơn hàng nhập vào có trong hệ thống AMS3 và Metagent sau khi crawl về staging_ivymoda_it hay không
-- Nếu không có thì in ra thông báo "Không tìm thấy đơn", sau đó kiểm tra trên AMS3 và hệ thống dữ liệu IT cấp
-- Nếu có thì in ra thông tin về kênh bán, mã đơn hàng, ngày đặt hàng, ngày xuất bảng kê trong Fact giá trị đơn và số lượng sản phẩm.
-- In thêm thông tin trong bảng đầu phiếu
	DECLARE _check INT;
	DECLARE _Stt_HD VARCHAR(30);

	SELECT 
		MAX(ma_kiem_tra) ma_kiem_tra INTO _check
	FROM
		(SELECT 
			ivy_invoice, 1 ma_kiem_tra
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 2 ma_kiem_tra
		FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 3 ma_kiem_tra
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 4 ma_kiem_tra
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice) order_check;

	-- In thông tin trong hệ thống staging_it
	IF _check = 1 THEN
		SELECT 
			'WEB-APP-IVY' kenh_ban, id, ivy_invoice, ngay_mua_hang, ngay_cap_nhat, doanh_so_hang_ban, tong_tien, so_luong, level_id_ivy, trang_thai, loai_don_ivy
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSEIF _check = 2 THEN
		SELECT 
			'WEB-APP-META' kenh_ban, id, ivy_invoice, ngay_mua_hang, ngay_cap_nhat, tong_tien, doanh_so_hang_ban, so_luong, level_id_ivy, trang_thai, loai_don_ivy
		FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSEIF _check = 3 THEN
		SELECT 
			UPPER(CONCAT(tmdt_type, '-IVY')) kenh_ban, id, ivy_invoice, ngay_dat_hang, updated_at ngay_cap_nhat, gia_goc doanh_so_hang_ban, tong_gia_cuoi, so_luong_sp, level_id_ivy, trang_thai
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSEIF _check = 4 THEN
		SELECT 
			UPPER(CONCAT(tmdt_type, '-META')) kenh_ban, id, ivy_invoice, ngay_dat_hang, updated_at ngay_cap_nhat, gia_goc doanh_so_hang_ban, tong_gia_cuoi, so_luong_sp, level_id_ivy, trang_thai
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSE
		SELECT CONCAT('Không có đơn hàng với mã ', ip_ivy_invoice) 'Thông báo';
	END IF;
    
	CALL data_warehouse.check_inf_order_products(ip_ivy_invoice, _check);

	-- In ra thông tin trong bảng Fact
	IF _check IS NOT NULL THEN
		SELECT 
			*
		FROM
			olap_ivymoda.fact_don_hang_ivy
		WHERE
			ma_don_hang = ip_ivy_invoice;
	END IF;

	-- In ra thông tin trong bảng đầu phiếu
	SET _Stt_HD = (SELECT 
						MAX(Stt_HD) Stt_HD
					FROM
						staging_ivy_moda_it.ivm_info
					WHERE
						IVM = ip_ivy_invoice);
	IF _Stt_HD IS NOT NULL THEN
	SELECT 
		*
	FROM
		staging_ivy_moda_it.orders_dau_phieu
	WHERE
		stt = _Stt_HD;
	END IF;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `check_inf_orders_offline` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_inf_orders_offline`(IN ip_khoa_don_hang VARCHAR(50))
BEGIN
-- Tạo procedure hỗ trợ kiểm tra hoá đơn offline
-- Mục đích: 
	DECLARE _ma_ch VARCHAR(5);
    DECLARE _ma_ch_it VARCHAR(5);
	DECLARE _ma_hoa_don VARCHAR(10);
	DECLARE _ngay_xuat_hoa_don DATETIME;
	
    SET _ma_ch = SUBSTRING_INDEX(ip_khoa_don_hang, '_', 1);
    SET _ma_hoa_don = SUBSTRING_INDEX(SUBSTRING_INDEX(ip_khoa_don_hang, '_', 2), '_', -1);
    SET _ngay_xuat_hoa_don = SUBSTRING_INDEX(ip_khoa_don_hang, '_', -1);
    
	SELECT 
		Dvcs INTO _ma_ch_it
	FROM
		staging_ivy_moda_it.dm_dvcs_w
	WHERE
		Bravo = _ma_ch;	
        
	SELECT _ma_ch, _ma_ch_it, _ma_hoa_don, _ngay_xuat_hoa_don;
    
	SELECT 
		*
	FROM
		staging_ivy_moda_it.orders_dau_phieu
	WHERE
		Ngay_Ct = _ngay_xuat_hoa_don 
			AND So_Ct = _ma_hoa_don
			AND Ma_DvCs = _ma_ch_it
            ;
	
    SELECT 
		*
	FROM
		staging_ivy_moda_it.orders_than_phieu
	WHERE
		Ngay_Ct = _ngay_xuat_hoa_don
			AND So_Ct = _ma_hoa_don
			AND Ma_DvCs = _ma_ch_it
            AND Colors != '---'
	ORDER BY Ma_Vt;
            
	SELECT 
		*
	FROM
		data_warehouse.check_du_lieu_data_source_bang_ke_xuat_offline
	WHERE
		ngay = _ngay_xuat_hoa_don 
			AND so_hoa_don = _ma_hoa_don
			AND ch = _ma_ch
	ORDER BY Ma_Vt;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `check_inf_order_products` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_inf_order_products`(IN ip_ivy_invoice VARCHAR(25), IN ip_check VARCHAR(25))
BEGIN
	/*DECLARE ip_check INT;*/
	DECLARE _id INT;

	/*SELECT 
		MAX(ma_kiem_tra) ma_kiem_tra INTO ip_check
	FROM
		(SELECT 
			ivy_invoice, 1 ma_kiem_tra
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 2 ma_kiem_tra
		FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 3 ma_kiem_tra
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 4 ma_kiem_tra
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice) orderip_check;
		*/

	-- In thông tin trong hệ thống staging_it
	IF ip_check = 1 THEN
		SELECT 
			id
		INTO _id FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_ivy_moda_it.order_products
		WHERE
			order_id = _id;
	ELSEIF ip_check = 2 THEN
		SELECT 
			id
		INTO _id FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_metagent_it.order_products
		WHERE
			order_id = _id;
	ELSEIF ip_check = 3 THEN
		SELECT 
			id
		INTO _id FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_ivy_moda_it.order_ecommerce_products
		WHERE
			order_ecommerce_id = _id;
	ELSEIF ip_check = 4 THEN
		SELECT 
			id
		INTO _id FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_metagent_it.order_ecommerce_products
		WHERE
			order_ecommerce_id = _id;
	ELSE
		SELECT CONCAT('Không có đơn hàng chi tiết với mã ', ip_ivy_invoice) 'Thông báo';
	END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `check_inf_order_product_repays` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_inf_order_product_repays`(IN ip_ivy_invoice VARCHAR(25), IN ip_check VARCHAR(25))
BEGIN
	/*DECLARE ip_check INT;*/
	DECLARE _id INT;

	/*SELECT 
		MAX(ma_kiem_tra) ma_kiem_tra INTO ip_check
	FROM
		(SELECT 
			ivy_invoice, 1 ma_kiem_tra
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 2 ma_kiem_tra
		FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 3 ma_kiem_tra
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 4 ma_kiem_tra
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice) orderip_check;
		*/

	-- In thông tin trong hệ thống staging_it
	IF ip_check = 1 THEN
		SELECT 
			id
		INTO _id FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_ivy_moda_it.order_repays
		WHERE
			order_id = _id;
	ELSEIF ip_check = 2 THEN
		SELECT 
			id
		INTO _id FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_metagent_it.order_repays
		WHERE
			order_id = _id;
	ELSEIF ip_check = 3 THEN
		SELECT 
			id
		INTO _id FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_ivy_moda_it.order_ecommerce_repays
		WHERE
			order_ecommerce_id = _id;
	ELSEIF ip_check = 4 THEN
		SELECT 
			id
		INTO _id FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
		SELECT 
			*
		FROM
			staging_metagent_it.order_ecommerce_repays
		WHERE
			order_ecommerce_id = _id;
	ELSE
		SELECT CONCAT('Không có đơn hàng hoàn trả với mã ', ip_ivy_invoice) 'Thông báo';
	END IF;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `check_inf_order_repays` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_inf_order_repays`(IN ip_ivy_invoice VARCHAR(25))
BEGIN
-- Tạo procedure hỗ trợ kiểm tra đơn hàng
-- Mục đích: Kiểm tra đơn hàng nhập vào có trong hệ thống AMS3 và Metagent sau khi crawl về staging_ivymoda_it hay không
-- Nếu không có thì in ra thông báo "Không tìm thấy đơn", sau đó kiểm tra trên AMS3 và hệ thống dữ liệu IT cấp
-- Nếu có thì in ra thông tin về kênh bán, mã đơn hàng, ngày đặt hàng, ngày xuất bảng kê trong Fact giá trị đơn và số lượng sản phẩm.
-- In thêm thông tin trong bảng đầu phiếu
	DECLARE _check INT;
	DECLARE _Stt_HD VARCHAR(30);

	SELECT 
		MAX(ma_kiem_tra) ma_kiem_tra INTO _check
	FROM
		(SELECT 
			ivy_invoice, 1 ma_kiem_tra
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 2 ma_kiem_tra
		FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 3 ma_kiem_tra
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice UNION ALL SELECT 
			ivy_invoice, 4 ma_kiem_tra
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice) order_check;

	-- In thông tin trong hệ thống staging_it
	IF _check = 1 THEN
		SELECT 
			'WEB-APP-IVY' kenh_ban, id, ivy_invoice, ngay_mua_hang, ngay_cap_nhat, tong_tien, so_luong, level_id_ivy, trang_thai, loai_don_ivy
		FROM
			staging_ivy_moda_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSEIF _check = 2 THEN
		SELECT 
			'WEB-APP-META' kenh_ban, id, ivy_invoice, ngay_mua_hang, ngay_cap_nhat, tong_tien, so_luong, level_id_ivy, trang_thai, loai_don_ivy
		FROM
			staging_metagent_it.orders
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSEIF _check = 3 THEN
		SELECT 
			UPPER(CONCAT(tmdt_type, '-IVY')) kenh_ban, id, ivy_invoice, ngay_dat_hang, updated_at ngay_cap_nhat, tong_gia_cuoi, so_luong_sp, level_id_ivy, trang_thai
		FROM
			staging_ivy_moda_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSEIF _check = 4 THEN
		SELECT 
			UPPER(CONCAT(tmdt_type, '-META')) kenh_ban, id, ivy_invoice, ngay_dat_hang, updated_at ngay_cap_nhat, tong_gia_cuoi, so_luong_sp, level_id_ivy, trang_thai
		FROM
			staging_metagent_it.order_ecommerces
		WHERE
			ivy_invoice = ip_ivy_invoice;
	ELSE
		SELECT CONCAT('Không có đơn hàng với mã ', ip_ivy_invoice) 'Thông báo';
	END IF;
    
	CALL data_warehouse.check_inf_order_product_repays(ip_ivy_invoice, _check);

	-- In ra thông tin trong bảng Fact
	IF _check IS NOT NULL THEN
		SELECT 
			*
		FROM
			olap_ivymoda.fact_don_hang_ivy
		WHERE
			ma_don_hang = ip_ivy_invoice;
	END IF;

	-- In ra thông tin trong bảng đầu phiếu
	SET _Stt_HD = (SELECT 
						MAX(Stt_HD) Stt_HD
					FROM
						staging_ivy_moda_it.ivm_info
					WHERE
						IVM = ip_ivy_invoice);
	IF _Stt_HD IS NOT NULL THEN
	SELECT 
		*
	FROM
		staging_ivy_moda_it.orders_dau_phieu
	WHERE
		Stt_HBTL = _Stt_HD;
	END IF;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `check_inf_order_repays_offline` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `check_inf_order_repays_offline`(IN ip_ma_don_hang_goc VARCHAR(20))
BEGIN

	SELECT 
		'staging_ivy_moda_it.orders_dau_phieu' ten_nguon,
		stt, ma_ct, Ngay_Ct, Ma_DvCs, Ma_Loai2,
		TTien2 - IFNULL(TTien4, 0) - IFNULL(TTien_Nt41, 0) tong_tien, so_luong_sp,
		TTien2, TTien4, TTien_Nt41
	FROM
		staging_ivy_moda_it.orders_dau_phieu
	WHERE
		stt = ip_ma_don_hang_goc 
	UNION SELECT 
		'staging_ivy_moda_it.orders_dau_phieu' ten_nguon,
		stt, ma_ct, Ngay_Ct, Ma_DvCs, Ma_Loai2,
		TTien2 - IFNULL(TTien4, 0) - IFNULL(TTien_Nt41, 0) tong_tien, so_luong_sp,
		TTien2, TTien4, TTien_Nt41
	FROM
		staging_ivy_moda_it.orders_dau_phieu
	WHERE
		Stt_HBTL = ip_ma_don_hang_goc;
		
	SELECT 
		'staging_ivy_moda_it.orders_than_phieu' ten_nguon, tp.*
	FROM
		staging_ivy_moda_it.orders_than_phieu tp
	WHERE
		stt IN (SELECT 
				stt
			FROM
				staging_ivy_moda_it.orders_dau_phieu
			WHERE
				Stt_HBTL = ip_ma_don_hang_goc);
				
	SELECT 
		'user_core.gd_don_hang_all' ten_nguon,
		ma_don_hang,
		ngay_dat_hang,
		ds4,
		so_luong_san_pham,
		return_ds4,
		return_quantity
	FROM
		user_core.gd_don_hang_all
	WHERE
		ma_don_hang = ip_ma_don_hang_goc;
		
	SELECT 
		'user_core.gd_san_pham_all' ten_nguon, sp.*
	FROM
		user_core.gd_san_pham_all sp
	WHERE
		ma_don_hang = ip_ma_don_hang_goc;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `etl_check_chi_tiet_du_lieu_staging_customer_purchasing_power` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `etl_check_chi_tiet_du_lieu_staging_customer_purchasing_power`()
BEGIN
	UPDATE data_warehouse.check_chi_tiet_du_lieu_staging_customer_purchasing_power cpp0
			JOIN
		(SELECT DISTINCT
			cpp1.customer_index
		FROM
			data_warehouse.check_chi_tiet_du_lieu_staging_customer_purchasing_power cpp1
		WHERE
			is_new_customer = 1) AS new_customer ON cpp0.customer_index = new_customer.customer_index 
	SET 
		cpp0.is_new_customer = 0
	WHERE
		cpp0.is_new_customer IS NULL;
	-- ---------------------	
	UPDATE data_warehouse.check_chi_tiet_du_lieu_staging_customer_purchasing_power cpp0
			JOIN
		(SELECT 
			cpp1.customer_index, MIN(cpp1.year) year
		FROM
			data_warehouse.check_chi_tiet_du_lieu_staging_customer_purchasing_power cpp1
		WHERE
			cpp1.is_new_customer IS NULL
		GROUP BY cpp1.customer_index) AS new_customer ON cpp0.customer_index = new_customer.customer_index
			AND cpp0.year = new_customer.year 
	SET 
		cpp0.is_new_customer = 1
	WHERE
		cpp0.is_new_customer IS NULL;
	-- ---------------------	
	UPDATE data_warehouse.check_chi_tiet_du_lieu_staging_customer_purchasing_power 
	SET 
		is_new_customer = 0
	WHERE
		is_new_customer IS NULL;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_api_lazada_ads_column` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_api_lazada_ads_column`()
BEGIN
	SELECT 
    column_name, api_column_name
FROM
    data_warehouse.ht_column_info
WHERE
    api_exist = 1
        AND table_name = 'gd_lazada_chien_dich';
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_api_schema` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_api_schema`(in_platform_name text, in_platform_data_type text)
BEGIN
	SELECT * FROM data_warehouse.api_schema WHERE platform_name = in_platform_name and platform_data_type = in_platform_data_type;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_last_insert_times` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_last_insert_times`()
BEGIN
    SELECT 'gd_lazada_chien_dich' AS table_name, MAX(date_start) AS latest_time FROM staging_ivy_moda.gd_lazada_chien_dich
	UNION
	SELECT 'gd_lazada_don_hang_chi_tiet' AS table_name, MAX(update_time) AS latest_time FROM staging_ivy_moda.gd_lazada_don_hang_chi_tiet
	UNION
	SELECT 'gd_pancake_hoi_thoai' AS table_name, MAX(updated_at) AS latest_time FROM staging_ivy_moda.gd_pancake_hoi_thoai
	UNION
	SELECT 'gd_pancake_hoi_thoai_chi_tiet' AS table_name, MAX(inserted_date) AS latest_time FROM staging_ivy_moda.gd_pancake_hoi_thoai_chi_tiet
	UNION
	SELECT 'gd_facebook_chien_dich' AS table_name, MAX(date_start) AS latest_time FROM staging_ivy_moda.gd_facebook_chien_dich;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_lazada_access_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_lazada_access_token`()
BEGIN
	SELECT access_token FROM data_warehouse.lazada_access_token ORDER BY created_at DESC LIMIT 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_lazada_app_config` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_lazada_app_config`()
BEGIN
	select url,app_id, app_secret from data_warehouse.lazada_app_crawl;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_lazada_campaign_information` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_lazada_campaign_information`()
BEGIN
	CALL get_lazada_app_config();
	CALL get_api_lazada_ads_column();
    CALL get_lazada_access_token();
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_mapping_api_column` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_mapping_api_column`(in_database_name text, in_table_name text)
BEGIN
	SELECT api_column_name, database_column_name, update_status FROM data_warehouse.mapping_api_column 
    WHERE database_name = in_database_name and table_name = in_table_name;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `get_pancake_access_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `get_pancake_access_token`()
BEGIN
	SELECT access_token FROM data_warehouse.pancake_access_token ORDER BY created_at DESC LIMIT 1;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_log_data_warehouse` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `insert_log_data_warehouse`(in _userName TEXT, in _databaseName TEXT, in _tableName TEXT, in _numOfRecord TEXT, in _status TEXT, in _fromDate DATE, in _toDate DATE, in _description TEXT)
BEGIN
INSERT INTO data_warehouse.log_data_warehouse VALUES (now(), _userName, _databaseName, _tableName, _numOfRecord, _status, _fromDate, _toDate, _description);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_new_fb_access_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`angular`@`%` PROCEDURE `insert_new_fb_access_token`(in new_access_token text)
begin
	insert into fb_access_token (access_token, created_at) values (new_access_token, now()); 
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_new_fb_access_token_old` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`crawl_data`@`%` PROCEDURE `insert_new_fb_access_token_old`(in new_access_token text)
begin
	insert into fb_access_token (access_token, created_at) values (new_access_token, now()); 
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_new_lazada_access_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`backend_server`@`%` PROCEDURE `insert_new_lazada_access_token`(in in_access_token text)
begin
	insert into data_warehouse.lazada_access_token (access_token, created_at) values (in_access_token, now());
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_new_tiktok_access_token` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `insert_new_tiktok_access_token`(IN in_refresh_token text)
begin 
	insert into tiktok_access_token (access_token, create_time) value (in_refresh_token, now());
end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `refresh_check_ctdl_stg_dh_ivy_hoan_offline` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `refresh_check_ctdl_stg_dh_ivy_hoan_offline`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver2.2 2025/05/13*/ -- 
PURGE BINARY LOGS BEFORE '2050-01-01';
	/*UPDATE staging_ivy_moda_it.orders_dau_phieu o
			JOIN
		(SELECT 
			stt, SUM(So_Luong) so_luong_sp
		FROM
			staging_ivy_moda_it.orders_than_phieu
		WHERE
			Ngay_Ct BETWEEN ip_fromDate AND ip_toDate
				AND Colors != '---'
		GROUP BY stt) AS od USING (stt) 
	SET 
		o.so_luong_sp = od.so_luong_sp
	WHERE
		Ngay_Ct BETWEEN ip_fromDate AND ip_toDate
			AND ma_ct = 'TL'
			AND Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5')
			AND o.so_luong_sp IS NULL;*/

    TRUNCATE TABLE data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_hoan_offline;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_hoan_offline
	SELECT 
		MAX(CONCAT(Ma_DvCs, '_', So_Ct, '_', DATE(Ngay_Ct))) `khoa`,
		Ma_DvCs `cua_hang_hoan`,
		So_Ct `so_hoa_don_hoan`,
		DATE(Ngay_Ct) `ngay_hoan_bang_ke`,
		SUM(so_luong_sp) `staging_so_luong_sp`,
		SUM(TTien2 - TTien4 - IFNULL(TTien_Nt41, 0)) `staging_tien_tong`,
		SUM(TTien2) `staging_doanh_so_ban`,
		SUM(TTien4) `staging_chi_phi_ck`,
        COUNT(1) `so_don_hang`
	FROM
		staging_ivy_moda_it.orders_dau_phieu o
	WHERE
		Ngay_Ct BETWEEN ip_fromDate AND ip_toDate
			AND ma_ct = 'TL'
			AND Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5')
	GROUP BY Ma_DvCs , So_Ct , DATE(Ngay_Ct);
    
    TRUNCATE TABLE data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_hoan_offline_1;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_hoan_offline_1
	SELECT 
		MAX(o.stt) AS stt,
		IFNULL(o.Stt_HBTL, o.stt) AS ma_don_hang_goc,
		MAX(o.Ma_Dt) AS Ma_Dt,
		MAX(CAST(o.Ngay_Ct AS DATE)) AS ngay_hoan_bang_ke,
		SUM((o.TTien2 - o.TTien4 - IFNULL(o.TTien_Nt41, 0))) AS doanh_thu_hoan,
		SUM(o.so_luong_sp) AS so_luong_sp_hoan
	FROM
		staging_ivy_moda_it.orders_dau_phieu o
	WHERE
		((o.Ngay_Ct BETWEEN ip_fromDate AND ip_toDate)
			AND (o.ma_ct = 'TL')
			AND (o.Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5'))
            AND (IFNULL(o.Ma_Loai2, 'ZZ') NOT IN ('ON' , 'O3', 'O4', 'O5')))
	GROUP BY ma_don_hang_goc;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `refresh_check_ctdl_stg_dh_ivy_offline` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `refresh_check_ctdl_stg_dh_ivy_offline`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver2.2 2025/05/13*/ -- 
PURGE BINARY LOGS BEFORE '2050-01-01';
	-- Cập nhật số lượng sản phẩm của đơn hàng offline
	UPDATE staging_ivy_moda_it.orders_dau_phieu o
			JOIN
		(SELECT 
			Stt, SUM(So_Luong) so_luong_sp
		FROM
			staging_ivy_moda_it.orders_than_phieu
		WHERE
			Ngay_Ct BETWEEN ip_fromDate AND ip_toDate
				AND Colors != '---'
				AND Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5')
		GROUP BY Stt) sl ON o.Stt = sl.Stt 
	SET 
		o.so_luong_sp = sl.so_luong_sp
	WHERE
		Ngay_Ct BETWEEN ip_fromDate AND ip_toDate
			AND Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5');
    
    TRUNCATE TABLE data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_offline;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_offline
	SELECT 
		MAX(CONCAT(IFNULL(Bravo, Ma_DvCs), '_', So_Ct, '_', DATE(Ngay_Ct))) `khoa`,
		MAX(IFNULL(Bravo, Ma_DvCs)) `cua_hang_bang_ke`,
		So_Ct `so_hoa_don_bang_ke`,
		DATE(Ngay_Ct) `ngay_xuat_bang_ke`,
		SUM(so_luong_sp) `staging_so_luong_sp`,
		SUM(TTien2 - IFNULL(TTien4, 0) - IFNULL(TTien_Nt41, 0)) `staging_tien_tong`,
		SUM(TTien2) `staging_doanh_so_ban`,
		SUM(TTien4) `staging_chi_phi_ck`,
		COUNT(1) `so_don_hang`
	FROM
		staging_ivy_moda_it.orders_dau_phieu o
			LEFT JOIN
		staging_ivy_moda_it.dm_dvcs_w ch ON o.Ma_DvCs = ch.Dvcs
	WHERE
		Ngay_Ct BETWEEN ip_fromDate AND ip_toDate
			AND ma_ct = 'HD'
			AND Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5')
	GROUP BY Ma_DvCs , So_Ct , DATE(Ngay_Ct);
    
    TRUNCATE TABLE data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_offline_1;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy_offline_1
    SELECT 
		o.stt AS stt,
		o.Ma_Dt AS Ma_Dt,
		CAST(o.Ngay_Ct
			AS DATE) AS ngay_xuat_bang_ke,
		((o.TTien2 - o.TTien4) - IFNULL(o.TTien_Nt41,
				0)) AS doanh_thu,
		o.so_luong_sp AS so_luong_sp
	FROM
		staging_ivy_moda_it.orders_dau_phieu o
	WHERE
		((o.Ngay_Ct BETWEEN ip_fromDate AND ip_toDate)
			AND (o.ma_ct = 'HD')
			AND (o.Ma_DvCs NOT IN ('ON' , 'O3', 'O4', 'O5'))
            AND Ma_Dt != 'KL01');

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_chi_tiet_du_lieu_staging_customer_purchasing_power` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_chi_tiet_du_lieu_staging_customer_purchasing_power`(ip_fromYear INT, ip_toYear INT)
BEGIN
	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_customer_purchasing_power
	(customer_index
    , year
    , total_ds4
    , purchase_frequency
    , total_items)
	SELECT 
		gd_don_hang_all.customer_index AS customer_index,
		YEAR(gd_don_hang_all.ngay_dat_hang) AS year,
		SUM(gd_don_hang_all.ds4) AS total_ds4,
		COUNT(DISTINCT gd_don_hang_all.ma_don_hang) AS purchase_frequency,
		SUM(gd_don_hang_all.so_luong_san_pham) AS total_items
	FROM
		user_core.gd_don_hang_all
	WHERE
		customer_index IS NOT NULL
			AND YEAR(gd_don_hang_all.ngay_dat_hang) BETWEEN ip_fromYear AND ip_toYear
	GROUP BY gd_don_hang_all.customer_index , YEAR(gd_don_hang_all.ngay_dat_hang)
	ON DUPLICATE KEY UPDATE 
	total_ds4 = VALUES(total_ds4),
	purchase_frequency = VALUES(purchase_frequency),
	total_items = VALUES(total_items);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_chi_tiet_du_lieu_staging_don_hang_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_chi_tiet_du_lieu_staging_don_hang_dat`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
	REPLACE INTO data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_dat(
			kenh_ban
			, ma_don_hang
			, ngay_dat_hang
			, staging_so_luong_sp
            , staging_doanh_so_ban
            , staging_doanh_so_sau_ck_voucher)
	SELECT 
		2 AS kenh_ban,
		ow.ivy_invoice AS ma_don_hang,
		CAST(ow.ngay_mua_hang AS DATE) AS ngay_dat_hang,
		ow.so_luong AS so_luong,
		ow.doanh_so_hang_ban AS doanh_so_hang_ban,
		IF(tong_tien > tien_giam_gia,
			tong_tien - tien_giam_gia,
			0) AS doanh_so_sau_ck_voucher
	FROM
		staging_ivy_moda_it.orders ow
	WHERE
		(ow.loai_don = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate 
	UNION SELECT 
		2 AS kenh_ban,
		owm.ivy_invoice AS ma_don_hang,
		CAST(owm.ngay_mua_hang AS DATE) AS ngay_dat_hang,
		owm.so_luong AS so_luong,
		owm.doanh_so_hang_ban AS doanh_so_hang_ban,
		IF(tong_tien > tien_giam_gia,
			tong_tien - tien_giam_gia,
			0) AS doanh_so_sau_ck_voucher
	FROM
		staging_metagent_it.orders owm
	WHERE
		(owm.loai_don = 0)
			AND ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate 
	UNION SELECT 
		3 AS kenh_ban,
		ods.ma_don_hang AS ma_don_hang,
		CAST(ods.ngay_dat_hang AS DATE) AS ngay_dat_hang,
		ods.so_luong_sp AS so_luong_sp,
		ods.doanh_so_ban AS doanh_so_ban,
		ods.doanh_so_sau_ck_voucher AS doanh_so_sau_ck_voucher
	FROM
		staging_ivy_moda.v_gd_shopee_don_hang ods
	WHERE
		ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate 
	UNION SELECT 
		4 AS kenh_ban,
		odl.order_number AS ma_don_hang,
		CAST(odl.create_time AS DATE) AS ngay_dat_hang,
		COUNT(1) AS staging_so_luong_sp,
		SUM(odl.gia_goc) AS staging_doanh_so_ban,
		SUM(odl.paid_price) AS staging_doanh_so_sau_ck_voucher
	FROM
		staging_ivy_moda.gd_lazada_don_hang_chi_tiet odl
	WHERE
		create_time BETWEEN ip_fromDate AND ip_toDate
	GROUP BY ma_don_hang , CAST(odl.create_time AS DATE) 
	UNION SELECT 
		5 AS kenh_ban,
		ot.order_id AS ma_don_hang,
		CAST(ot.created_time AS DATE) AS ngay_dat_hang,
		SUM(ot.quantity) AS so_luong,
		SUM(ot.sku_subtotal_before_discount) AS doanh_so_hang_ban,
		SUM(ot.sku_subtotal_after_discount) AS doanh_so_sau_ck_voucher
	FROM
		staging_ivy_moda.gd_tiktok_don_hang_chi_tiet ot
	WHERE
		created_time BETWEEN ip_fromDate AND ip_toDate
	GROUP BY order_id , CAST(ot.created_time AS DATE);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_chi_tiet_du_lieu_staging_don_hang_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_chi_tiet_du_lieu_staging_don_hang_ivy`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
	REPLACE INTO data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy(
			kenh_ban
            , ma_don_hang
			, ngay_dat_hang
            , staging_so_luong_sp
			, staging_doanh_so_ban_sau_ck_voucher
            , loai_don_ivy
            )
	SELECT 
		tmdt.ma_kenh_ban AS ma_kenh_ban,
		tmdt.ivy_invoice AS ma_don_hang,
		DATE(tmdt.ngay_dat_hang) AS ngay_dat_hang,
		tmdt.so_luong_sp AS staging_so_luong_sp,
		tmdt.tong_gia_cuoi AS staging_doanh_so_ban_sau_ck_voucher,
        1
	FROM
		staging_ivy_moda_it.order_ecommerces tmdt
	WHERE
		ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate 
	UNION SELECT 
		2 AS ma_kenh_ban,
		ow.ivy_invoice,
		DATE(ow.ngay_mua_hang) ngay_mua_hang,
		ow.so_luong,
		IF(ow.tong_tien > ow.tien_giam_gia,
			ow.tong_tien - ow.tien_giam_gia,
			0),
		ow.loai_don_ivy
	FROM
		staging_ivy_moda_it.orders ow
	WHERE
		/*ow.loai_don_ivy = 1 -- chỉ lấy những đơn không bị huỷ do hệ thống
			AND*/ ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate -- Thêm tất cả đơn hàng kể cả đơn không phải là đơn IVY sau đó xoá
	UNION SELECT 
		tmdtm.ma_kenh_ban AS ma_kenh_ban,
		tmdtm.ivy_invoice AS ma_don_hang,
		DATE(tmdtm.ngay_dat_hang) AS ngay_dat_hang,
		tmdtm.so_luong_sp AS staging_so_luong_sp,
		tmdtm.tong_gia_cuoi AS staging_doanh_so_ban_sau_ck_voucher,
        1
	FROM
		staging_metagent_it.order_ecommerces tmdtm
	WHERE
		ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate 
	UNION SELECT 
		2 AS ma_kenh_ban,
		owm.ivy_invoice,
		DATE(owm.ngay_mua_hang) ngay_mua_hang,
		owm.so_luong,
		IF(owm.tong_tien > owm.tien_giam_gia,
			owm.tong_tien - owm.tien_giam_gia,
			0),
		owm.loai_don_ivy
	FROM
		staging_metagent_it.orders owm
	WHERE
		/*owm.loai_don_ivy = 1 -- chỉ lấy những đơn không bị huỷ do hệ thống
			AND*/ ngay_mua_hang BETWEEN ip_fromDate AND ip_toDate; -- Thêm tất cả đơn hàng kể cả đơn không phải là đơn IVY sau đó xoá
	
    DELETE FROM data_warehouse.check_chi_tiet_du_lieu_staging_don_hang_ivy 
	WHERE
		loai_don_ivy = 0
		AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_chi_tiet_du_lieu_staging_so_luong_san_pham_dat` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_chi_tiet_du_lieu_staging_so_luong_san_pham_dat`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
	REPLACE INTO data_warehouse.check_chi_tiet_du_lieu_staging_so_luong_san_pham_dat(
			kenh_ban
			, ma_don_hang
			, ngay_dat_hang
            , ma_san_pham
			, staging_so_luong_sp
            , staging_doanh_so_sp_ban
            , staging_doanh_so_sp_sau_ck)
	SELECT 
		2 AS kenh_ban,
		odw.order_id AS ma_don_hang,
		CAST(odw.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        odw.product_sub_sku AS ma_san_pham,
		SUM(odw.quantity) AS so_luong,
		SUM(odw.price * odw.quantity) AS doanh_so_sp_ban,
		SUM(odw.price_end * odw.quantity) AS doanh_so_sp_sau_ck
	FROM
		staging_ivy_moda_it.order_products odw
	WHERE
		(odw.loai_don = 0)
			AND odw.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	GROUP BY ma_don_hang , CAST(odw.ngay_dat_hang AS DATE), odw.product_sub_sku
    UNION SELECT 
		2 AS kenh_ban,
		- odwm.order_id AS ma_don_hang,
		CAST(odwm.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        odwm.product_sub_sku AS ma_san_pham,
		SUM(odwm.quantity) AS so_luong,
		SUM(odwm.price * odwm.quantity) AS doanh_so_sp_ban,
		SUM(odwm.price_end * odwm.quantity) AS doanh_so_sp_sau_ck
	FROM
		staging_metagent_it.order_products odwm
	WHERE
		(odwm.loai_don = 0)
			AND odwm.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	GROUP BY ma_don_hang , CAST(odwm.ngay_dat_hang AS DATE), odwm.product_sub_sku
    UNION SELECT 
		3 AS kenh_ban,
		ods.ma_don_hang AS ma_don_hang,
		CAST(ods.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        sku_phan_loai_hang AS ma_san_pham,
		ods.so_luong AS so_luong_sp,
		ods.gia_goc * ods.so_luong AS doanh_so_sp_ban,
		ods.gia_uu_dai * ods.so_luong AS doanh_so_sp_sau_ck
	FROM
		staging_ivy_moda.gd_shopee_don_hang_chi_tiet ods
	WHERE
		ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	UNION SELECT 
		4 AS kenh_ban,
		odl.order_number AS ma_don_hang,
		CAST(odl.create_time AS DATE) AS ngay_dat_hang,
        odl.seller_sku AS ma_san_pham,
		COUNT(1) AS staging_so_luong_sp,
		SUM(odl.gia_goc) AS staging_doanh_so_sp_ban,
		SUM(odl.unit_price) AS staging_doanh_so_sp_sau_ck
	FROM
		staging_ivy_moda.gd_lazada_don_hang_chi_tiet odl
	WHERE
		create_time BETWEEN ip_fromDate AND ip_toDate
	GROUP BY odl.order_number , CAST(odl.create_time AS DATE), odl.seller_sku
	UNION SELECT 
		5 AS kenh_ban,
		odt.order_id AS ma_don_hang,
        CAST(odt.created_time AS DATE) AS ngay_dat_hang,
        odt.seller_sku AS ma_san_pham,
		SUM(odt.quantity) AS so_luong,
		SUM(odt.sku_subtotal_before_discount) AS doanh_so_sp_ban,
		SUM(odt.sku_subtotal_after_discount) AS doanh_so_sp_sau_ck
	FROM
		staging_ivy_moda.gd_tiktok_don_hang_chi_tiet odt
	WHERE
		created_time BETWEEN ip_fromDate AND ip_toDate
	GROUP BY odt.order_id , CAST(odt.created_time AS DATE), odt.seller_sku;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_chi_tiet_du_lieu_staging_so_luong_san_pham_ivy` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_chi_tiet_du_lieu_staging_so_luong_san_pham_ivy`(ip_fromDate DATE, ip_toDate DATE)
BEGIN
/*Ver3.5 2025/03/27 - */
	REPLACE INTO data_warehouse.check_chi_tiet_du_lieu_staging_so_luong_san_pham_ivy(
			kenh_ban
			, ma_don_hang
			, ngay_dat_hang
            , ma_san_pham
            , gia_sp_sau_ck
			, staging_so_luong_sp
            , staging_doanh_so_sp_sau_ck
            , loai_don_ivy)
	SELECT 
		2 AS kenh_ban,
		odw.order_id AS ma_don_hang,
		CAST(odw.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        odw.product_sub_sku AS ma_san_pham,
        odw.price_end AS gia_sp_sau_ck,
		SUM(odw.quantity) AS so_luong,
		SUM(odw.price_end * odw.quantity) AS doanh_so_sp_sau_ck,
        MAX(odw.loai_don_ivy) AS loai_don_ivy
	FROM
		staging_ivy_moda_it.order_products odw
	WHERE
		/*(odw.loai_don_ivy = 1)
			AND*/ odw.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	GROUP BY ma_don_hang , CAST(odw.ngay_dat_hang AS DATE), odw.product_sub_sku, odw.price_end
    UNION SELECT 
		tmdt.ma_kenh_ban AS kenh_ban,
		tmdt.order_ecommerce_id AS ma_don_hang,
        CAST(tmdt.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        tmdt.product_sub_sku AS ma_san_pham,
        MAX(tmdt.gia_mot_san_pham) AS gia_sp_sau_ck,
		SUM(tmdt.quantity) AS so_luong,
		SUM(tmdt.quantity * tmdt.gia_mot_san_pham) AS doanh_so_sp_sau_ck,
        1
	FROM
		staging_ivy_moda_it.order_ecommerce_products tmdt
	WHERE
		tmdt.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	GROUP BY tmdt.ma_kenh_ban, tmdt.order_ecommerce_id , CAST(tmdt.ngay_dat_hang AS DATE), tmdt.product_sub_sku
    UNION SELECT 
		2 AS kenh_ban,
		- odwm.order_id AS ma_don_hang,
		CAST(odwm.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        odwm.product_sub_sku AS ma_san_pham,
        odwm.price_end AS gia_sp_sau_ck,
		SUM(odwm.quantity) AS so_luong,
		SUM(odwm.price_end * odwm.quantity) AS doanh_so_sp_sau_ck,
        MAX(odwm.loai_don_ivy) AS loai_don_ivy
	FROM
		staging_metagent_it.order_products odwm
	WHERE
		/*(odw.loai_don_ivy = 1)
			AND*/ odwm.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	GROUP BY ma_don_hang , CAST(odwm.ngay_dat_hang AS DATE), odwm.product_sub_sku, odwm.price_end
    UNION SELECT 
		tmdtm.ma_kenh_ban AS kenh_ban,
		- tmdtm.order_ecommerce_id AS ma_don_hang,
        CAST(tmdtm.ngay_dat_hang AS DATE) AS ngay_dat_hang,
        tmdtm.product_sub_sku AS ma_san_pham,
        MAX(tmdtm.gia_mot_san_pham) AS gia_sp_sau_ck,
		SUM(tmdtm.quantity) AS so_luong,
		SUM(tmdtm.quantity * tmdtm.gia_mot_san_pham) AS doanh_so_sp_sau_ck,
        1
	FROM
		staging_metagent_it.order_ecommerce_products tmdtm
	WHERE
		tmdtm.ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate
	GROUP BY tmdtm.ma_kenh_ban, tmdtm.order_ecommerce_id , CAST(tmdtm.ngay_dat_hang AS DATE), tmdtm.product_sub_sku;
    
    DELETE FROM data_warehouse.check_chi_tiet_du_lieu_staging_so_luong_san_pham_ivy 
	WHERE
		loai_don_ivy = 0
		AND ngay_dat_hang BETWEEN ip_fromDate AND ip_toDate;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_du_lieu_staging_customer_life_time_values` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_du_lieu_staging_customer_life_time_values`()
BEGIN
	REPLACE INTO data_warehouse.check_du_lieu_staging_customer_life_time_values(
			nam
			, ki
			, ky_6_thang
			, half_year_cohort
            , so_luong_khach_hang
            , so_luong_don_hang
            , doanh_thu)
	SELECT 
		ky_0.nam,
		ky_0.ki,
		CONCAT(ky_0.nam, ky_0.ki),
        ky_0.half_year_cohort,
		ky_0.so_luong_khach_hang,
        ky_0.so_luong_don_hang,
        ky_0.doanh_thu
	FROM
		data_warehouse.v_check_du_lieu_staging_customer_life_time_values_0 ky_0;

	REPLACE INTO data_warehouse.check_du_lieu_staging_customer_life_time_values(
			nam
			, ki
			, ky_6_thang
			, half_year_cohort
            , so_luong_khach_hang
            , so_luong_don_hang
            , doanh_thu)
	SELECT 
		ky_1.nam,
		ky_1.ki,
		CONCAT(ky_1.nam, ky_1.ki),
        ky_1.half_year_cohort,
		ky_1.so_luong_khach_hang,
        ky_1.so_luong_don_hang,
        ky_1.doanh_thu
	FROM
		data_warehouse.v_check_du_lieu_staging_customer_life_time_values_1 ky_1;

	REPLACE INTO data_warehouse.check_du_lieu_staging_customer_life_time_values(
			nam
			, ki
			, ky_6_thang
			, half_year_cohort
            , so_luong_khach_hang
            , so_luong_don_hang
            , doanh_thu)
	SELECT 
		ky_2.nam,
		ky_2.ki,
		CONCAT(ky_2.nam, ky_2.ki),
        ky_2.half_year_cohort,
		ky_2.so_luong_khach_hang,
        ky_2.so_luong_don_hang,
        ky_2.doanh_thu
	FROM
		data_warehouse.v_check_du_lieu_staging_customer_life_time_values_2 ky_2;
        
	REPLACE INTO data_warehouse.check_du_lieu_staging_customer_life_time_values(
			nam
			, ki
			, ky_6_thang
			, half_year_cohort
            , so_luong_khach_hang
            , so_luong_don_hang
            , doanh_thu)
	SELECT 
		ky_3.nam,
		ky_3.ki,
		CONCAT(ky_3.nam, ky_3.ki),
        ky_3.half_year_cohort,
		ky_3.so_luong_khach_hang,
        ky_3.so_luong_don_hang,
        ky_3.doanh_thu
	FROM
		data_warehouse.v_check_du_lieu_staging_customer_life_time_values_3 ky_3;
        
	REPLACE INTO data_warehouse.check_du_lieu_staging_customer_life_time_values(
			nam
			, ki
			, ky_6_thang
			, half_year_cohort
            , so_luong_khach_hang
            , so_luong_don_hang
            , doanh_thu)
	SELECT 
		ky_4.nam,
		ky_4.ki,
		CONCAT(ky_4.nam, ky_4.ki),
        ky_4.half_year_cohort,
		ky_4.so_luong_khach_hang,
        ky_4.so_luong_don_hang,
        ky_4.doanh_thu
	FROM
		data_warehouse.v_check_du_lieu_staging_customer_life_time_values_4 ky_4;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `them_check_du_lieu_staging_customer_preferences` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `them_check_du_lieu_staging_customer_preferences`()
BEGIN
	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_customer_preferences
	(customer_index, nam, mau_sac)
	SELECT DISTINCT
			index_khach_hang,
			nam_mua,
			FIRST_VALUE(id_mau_sac) OVER (PARTITION BY index_khach_hang, nam_mua ORDER BY tan_suat DESC, id_mau_sac DESC) AS id_mau_sac
		FROM (
			SELECT 
				index_khach_hang,
				id_mau_sac,
				YEAR(ngay) AS nam_mua,
				COUNT(*) AS tan_suat
			FROM 
				user_core.gd_san_pham_all
			WHERE index_khach_hang IS NOT NULL
			GROUP BY 
				index_khach_hang,
				id_mau_sac,
				YEAR(ngay)
			HAVING 
				COUNT(*) > 1
		) AS so_thich_mau_sac 
	ON DUPLICATE KEY UPDATE mau_sac = id_mau_sac;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_customer_preferences
	(customer_index, nam, kich_co)
	SELECT DISTINCT
			index_khach_hang,
			nam_mua,
			FIRST_VALUE(id_size) OVER (PARTITION BY index_khach_hang, nam_mua ORDER BY tan_suat DESC, id_size DESC) AS id_size
		FROM (
			SELECT 
				index_khach_hang,
				id_size,
				YEAR(ngay) AS nam_mua,
				COUNT(*) AS tan_suat
			FROM 
				user_core.gd_san_pham_all
			WHERE index_khach_hang IS NOT NULL
			GROUP BY 
				index_khach_hang,
				id_size,
				YEAR(ngay)
			HAVING 
				COUNT(*) > 1
		) AS so_thich_kich_co
	ON DUPLICATE KEY UPDATE kich_co = id_size;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_customer_preferences
	(customer_index, nam, thuong_hieu)
	SELECT DISTINCT
			index_khach_hang,
			nam_mua,
			FIRST_VALUE(id_thuong_hieu) OVER (PARTITION BY index_khach_hang, nam_mua ORDER BY tan_suat DESC, id_thuong_hieu DESC) AS id_thuong_hieu
		FROM (
			SELECT 
				index_khach_hang,
				id_thuong_hieu,
				YEAR(ngay) AS nam_mua,
				COUNT(*) AS tan_suat
			FROM 
				user_core.gd_san_pham_all
			WHERE index_khach_hang IS NOT NULL
			GROUP BY 
				index_khach_hang,
				id_thuong_hieu,
				YEAR(ngay)
			HAVING 
				COUNT(*) > 1
		) AS so_thich_thuong_hieu 
	ON DUPLICATE KEY UPDATE thuong_hieu = id_thuong_hieu;

	INSERT INTO data_warehouse.check_chi_tiet_du_lieu_staging_customer_preferences
	(customer_index, nam, nhom_hang)
	SELECT DISTINCT
			index_khach_hang,
			nam_mua,
			FIRST_VALUE(id_nhom_san_pham) OVER (PARTITION BY index_khach_hang, nam_mua ORDER BY tan_suat DESC, id_nhom_san_pham DESC) AS id_nhom_san_pham
		FROM (
			SELECT 
				index_khach_hang,
				id_nhom_san_pham,
				YEAR(ngay) AS nam_mua,
				COUNT(*) AS tan_suat
			FROM 
				user_core.gd_san_pham_all
			WHERE index_khach_hang IS NOT NULL
			GROUP BY 
				index_khach_hang,
				id_nhom_san_pham,
				YEAR(ngay)
			HAVING 
				COUNT(*) > 1
		) AS so_thich_nhom_hang
	ON DUPLICATE KEY UPDATE nhom_hang = id_nhom_san_pham;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `track_log_procedure` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `track_log_procedure`(IN iDatabase VARCHAR(30), 
IN iProcedure VARCHAR(128), 
IN iTask VARCHAR(255), 
IN iLoaiThoiGian VARCHAR(10), 
IN ip_fromDate DATE, 
IN ip_toDate DATE, 
IN iGhiChu TEXT)
BEGIN
	INSERT INTO log_tracking_procedure(
    ten_database, 
    ten_procedure, 
    task, 
    loai_thoi_gian, 
    thoi_gian_ghi,
    from_date,
    to_date,
    ghi_chu)
	VALUES (iDatabase, iProcedure, iTask, iLoaiThoiGian, NOW(), ip_fromDate, ip_toDate, iGhiChu);
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `update_ma_kenh_ban_data_source_bang_ke_hoan_tra` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_0900_ai_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=`root`@`%` PROCEDURE `update_ma_kenh_ban_data_source_bang_ke_hoan_tra`()
BEGIN
    UPDATE data_warehouse.check_du_lieu_data_source_bang_ke_hoan_tra
    SET ma_kenh_ban = 
        CASE 
            WHEN ivm LIKE '%S%' THEN 3
            WHEN ivm LIKE '%L%' THEN 4
            WHEN ivm LIKE '%T%' THEN 5
            ELSE ma_kenh_ban
        END
    WHERE ma_kenh_ban = 2;
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

-- Dump completed on 2025-06-14 23:50:00
