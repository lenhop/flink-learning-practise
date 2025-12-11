-- Create ODS layer Walmart order table
CREATE TABLE IF NOT EXISTS ods.walmart_order (
    -- Order basic information
    purchaseOrderId BIGINT COMMENT 'Purchase order ID',
    customerOrderId BIGINT COMMENT 'Customer order ID',
    customerEmailId VARCHAR(100) COMMENT 'Customer email ID',
    orderDate BIGINT COMMENT 'Order date (timestamp)',
    orderDate_formatted TIMESTAMP COMMENT 'Formatted order date',
    
    -- Ship node information
    shipNode_type VARCHAR(50) COMMENT 'Ship node type',
    shipNode_name VARCHAR(100) COMMENT 'Ship node name',
    shipNode_id VARCHAR(50) COMMENT 'Ship node ID',
    
    -- Data source information
    source_file VARCHAR(100) COMMENT 'Source file name',
    phone VARCHAR(20) COMMENT 'Contact phone',
    
    -- Estimated delivery information
    estimatedDeliveryDate BIGINT COMMENT 'Estimated delivery date (timestamp)',
    estimatedDeliveryDate_formatted TIMESTAMP COMMENT 'Formatted estimated delivery date',
    estimatedShipDate BIGINT COMMENT 'Estimated ship date (timestamp)',
    estimatedShipDate_formatted TIMESTAMP COMMENT 'Formatted estimated ship date',
    methodCode VARCHAR(50) COMMENT 'Delivery method code',
    
    -- Recipient address information
    recipient_name VARCHAR(100) COMMENT 'Recipient name',
    address1 VARCHAR(200) COMMENT 'Address line 1',
    address2 VARCHAR(200) COMMENT 'Address line 2',
    city VARCHAR(100) COMMENT 'City',
    state VARCHAR(50) COMMENT 'State/Province',
    postalCode VARCHAR(20) COMMENT 'Postal code',
    country VARCHAR(10) COMMENT 'Country',
    addressType VARCHAR(20) COMMENT 'Address type',
    
    -- Order line item information
    lineNumber INT COMMENT 'Order line number',
    sku VARCHAR(50) COMMENT 'Product SKU',
    productName TEXT COMMENT 'Product name',
    product_condition VARCHAR(50) COMMENT 'Product condition',
    quantity INT COMMENT 'Quantity',
    unitOfMeasurement VARCHAR(20) COMMENT 'Unit of measurement',
    
    -- Order status information
    statusDate BIGINT COMMENT 'Status date (timestamp)',
    statusDate_formatted TIMESTAMP COMMENT 'Formatted status date',
    fulfillmentOption VARCHAR(50) COMMENT 'Fulfillment option',
    shipMethod VARCHAR(50) COMMENT 'Shipping method',
    storeId VARCHAR(50) COMMENT 'Store ID',
    shippingProgramType VARCHAR(50) COMMENT 'Shipping program type',
    
    -- Charge information
    chargeType VARCHAR(50) COMMENT 'Charge type',
    chargeName VARCHAR(100) COMMENT 'Charge name',
    chargeAmount DECIMAL(10,2) COMMENT 'Charge amount',
    currency VARCHAR(10) COMMENT 'Currency',
    taxAmount DECIMAL(10,2) COMMENT 'Tax amount',
    taxName VARCHAR(50) COMMENT 'Tax name',
    
    -- Order line status information
    orderLineStatus VARCHAR(50) COMMENT 'Order line status',
    statusQuantity INT COMMENT 'Status quantity',
    cancellationReason VARCHAR(200) COMMENT 'Cancellation reason',
    
    -- Shipping information
    shipDateTime BIGINT COMMENT 'Ship date time (timestamp)',
    shipDateTime_formatted TIMESTAMP COMMENT 'Formatted ship date time',
    carrierName VARCHAR(100) COMMENT 'Carrier name',
    carrierMethodCode VARCHAR(50) COMMENT 'Carrier method code',
    trackingNumber VARCHAR(100) COMMENT 'Tracking number',
    trackingURL VARCHAR(500) COMMENT 'Tracking URL',
    
    -- Data processing information
    request_time DATETIME COMMENT 'Request time when order was fetched from API',
    load_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Load time when record was inserted into database',
    
    PRIMARY KEY (purchaseOrderId, sku)
)
COMMENT 'Walmart order table';