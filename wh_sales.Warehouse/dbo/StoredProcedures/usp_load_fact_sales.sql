CREATE   PROCEDURE dbo.usp_load_fact_sales AS
BEGIN
    DELETE FROM dbo.fact_sales;
    INSERT INTO dbo.fact_sales SELECT * FROM lh_sales.dbo.gold_fact_sales;
END;

GO