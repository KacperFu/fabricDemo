CREATE TABLE [dbo].[fact_sales] (
    [OrderKey]     BIGINT          NULL,
    [LineNumber]   INT             NULL,
    [OrderDate]    DATE            NULL,
    [DeliveryDate] DATE            NULL,
    [CustomerKey]  INT             NULL,
    [StoreKey]     INT             NULL,
    [ProductKey]   INT             NULL,
    [Quantity]     INT             NULL,
    [NetPrice]     DECIMAL (20, 5) NULL,
    [UnitCost]     DECIMAL (20, 5) NULL,
    [Revenue]      DECIMAL (18, 2) NULL
);


GO