CREATE TABLE IF NOT EXISTS stg_invoices (
    doc_entry INT PRIMARY KEY,
    doc_num INT,
    card_code VARCHAR(50),
    doc_date DATE,
    doc_total NUMERIC(18,2),
    update_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_doc_date ON stg_invoices(doc_date);
CREATE INDEX IF NOT EXISTS idx_update_date ON stg_invoices(update_date);