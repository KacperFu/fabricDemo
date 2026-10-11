CREATE TABLE [dbo].[fact_sales] (
    [SalesOrderNumber]     VARCHAR (8000) NULL,
    [SalesOrderLineNumber] INT            NULL,
    [CustomerKey]          INT            NULL,
    [ProductID]            INT            NULL,
    [OrderDate]            DATE           NULL,
    [Quantity]             INT            NULL,
    [UnitPrice]            FLOAT (53)     NULL,
    [TaxAmount]            FLOAT (53)     NULL,
    [Revenue]              FLOAT (53)     NULL
);


GO