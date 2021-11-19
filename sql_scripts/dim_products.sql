CREATE TABLE DIM_PRODUCTS
(
	PRODUCT_ID INT NOT NULL,
	PRODUCT_NAME VARCHAR() NOT NULL,
	AISLE VARCHAR() NOT NULL,
	DEPARTMENT VARCHAR() NOT NULL
)
WITH (APPENDOPTIMIZED=TRUE, ORIENTATION=COLUMN, COMPRESSTYPE=ZLIB, COMPRESSLEVEL=5)
DISTRIBUTED BY (PRODUCT_ID)
;