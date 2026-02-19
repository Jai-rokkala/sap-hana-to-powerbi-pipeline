CREATE OR REPLACE VIEW vw_sales_daily AS
SELECT 
    doc_date,
    SUM(doc_total) AS total_sales,
    COUNT(*) AS invoice_count
FROM stg_invoices
GROUP BY doc_date
ORDER BY doc_date;


CREATE OR REPLACE VIEW vw_sales_by_customer AS
SELECT 
    card_code,
    SUM(doc_total) AS total_sales,
    COUNT(*) AS invoices
FROM stg_invoices
GROUP BY card_code
ORDER BY total_sales DESC;


CREATE OR REPLACE VIEW vw_sales_monthly AS
SELECT 
    DATE_TRUNC('month', doc_date) AS month,
    SUM(doc_total) AS total_sales,
    COUNT(*) AS invoice_count
FROM stg_invoices
GROUP BY month
ORDER BY month;