CREATE TABLE [dbo].[dim_product] (
    [ProductKey]  INT             NULL,
    [ProductCode] VARCHAR (8000)  NULL,
    [ProductName] VARCHAR (8000)  NULL,
    [Brand]       VARCHAR (8000)  NULL,
    [Color]       VARCHAR (8000)  NULL,
    [Category]    VARCHAR (8000)  NULL,
    [Subcategory] VARCHAR (8000)  NULL,
    [Price]       DECIMAL (18, 2) NULL
);


GO