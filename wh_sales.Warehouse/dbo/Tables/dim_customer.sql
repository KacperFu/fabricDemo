CREATE TABLE [dbo].[dim_customer] (
    [CustomerKey] INT            NULL,
    [Name]        VARCHAR (8000) NULL,
    [Gender]      VARCHAR (8000) NULL,
    [City]        VARCHAR (8000) NULL,
    [State]       VARCHAR (8000) NULL,
    [Country]     VARCHAR (8000) NULL,
    [Continent]   VARCHAR (8000) NULL,
    [Birthday]    DATE           NULL
);


GO