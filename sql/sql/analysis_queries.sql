CREATE DATABASE customer_revenue_analysis;
USE customer_revenue_analysis;
CREATE TABLE raw_transactions (
    Customer_ID INT,
    Age INT,
    Gender VARCHAR(20),
    City VARCHAR(50),
    Category VARCHAR(50),
    Product_Name VARCHAR(100),
    Purchase_Date DATE,
    Purchase_Amount DECIMAL(10,2),
    Payment_Method VARCHAR(50),
    Discount_Applied VARCHAR(10),
    Rating INT,
    Repeat_Customer VARCHAR(10)
);
DROP TABLE raw_transactions;
CREATE TABLE raw_transactions (
    Customer_ID VARCHAR(50),
    Age INT,
    Gender VARCHAR(20),
    City VARCHAR(50),
    Category VARCHAR(50),
    Product_Name VARCHAR(100),
    Purchase_Date DATE,
    Purchase_Amount DECIMAL(10,2),
    Payment_Method VARCHAR(50),
    Discount_Applied VARCHAR(10),
    Rating INT,
    Repeat_Customer VARCHAR(10)
);
SELECT COUNT(*) FROM raw_transactions;
TRUNCATE TABLE raw_transactions;
SELECT COUNT(*) FROM raw_transactions;
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/raw_data.csv.csv"
INTO TABLE raw_transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

CREATE TABLE users AS
SELECT
    Customer_ID,
    MIN(Purchase_Date) AS first_purchase_date,
    MAX(Purchase_Date) AS last_purchase_date,
    MAX(Repeat_Customer) AS repeat_customer_flag,
    MAX(Age) AS age,
    MAX(Gender) AS gender,
    MAX(City) AS city
FROM raw_transactions
GROUP BY Customer_ID;
SELECT * FROM users LIMIT 5;

CREATE TABLE events (
    event_id INT AUTO_INCREMENT PRIMARY KEY,
    Customer_ID VARCHAR(50),
    event_name VARCHAR(50),
    event_date DATE,
    Purchase_Amount DECIMAL(10,2),
    Category VARCHAR(50),
    Discount_Applied VARCHAR(10),
    Rating INT
);
INSERT INTO events (Customer_ID, event_name, event_date)
SELECT
    Customer_ID,
    'app_open',
    Purchase_Date
FROM raw_transactions;
INSERT INTO events (Customer_ID, event_name, event_date, Category)
SELECT
    Customer_ID,
    'product_view',
    Purchase_Date,
    Category
FROM raw_transactions;
INSERT INTO events (Customer_ID, event_name, event_date, Purchase_Amount)
SELECT
    Customer_ID,
    'add_to_cart',
    Purchase_Date,
    Purchase_Amount
FROM raw_transactions
WHERE Purchase_Amount > 0;
INSERT INTO events (Customer_ID, event_name, event_date, Purchase_Amount, Discount_Applied)
SELECT
    Customer_ID,
    'purchase',
    Purchase_Date,
    Purchase_Amount,
    Discount_Applied
FROM raw_transactions;
INSERT INTO events (Customer_ID, event_name, event_date, Rating)
SELECT
    Customer_ID,
    'rating_given',
    Purchase_Date,
    Rating
FROM raw_transactions
WHERE Rating IS NOT NULL;

SELECT event_name, COUNT(*) 
FROM events
GROUP BY event_name;
SELECT * FROM events LIMIT 10;

DELETE FROM events
WHERE event_name = 'rating_given';
SET SQL_SAFE_UPDATES = 0;
INSERT INTO events (Customer_ID, event_name, event_date, Rating)
SELECT
    Customer_ID,
    'rating_given',
    Purchase_Date,
    Rating
FROM raw_transactions
WHERE Rating >= 3;

SELECT event_name, COUNT(*)
FROM events
GROUP BY event_name;
SET SQL_SAFE_UPDATES = 1;

SELECT 
    event_name,
    COUNT(DISTINCT Customer_ID) AS users
FROM events
GROUP BY event_name
ORDER BY users DESC;

SELECT 
    event_name,
    COUNT(DISTINCT Customer_ID) AS users
FROM events
WHERE event_name IN (
    'app_open',
    'product_view',
    'add_to_cart',
    'purchase',
    'rating_given'
)
GROUP BY event_name
ORDER BY FIELD(
    event_name,
    'app_open',
    'product_view',
    'add_to_cart',
    'purchase',
    'rating_given'
);



SELECT COUNT(DISTINCT Customer_ID) AS total_users
FROM users;
SELECT COUNT(DISTINCT Customer_ID) AS total_users
FROM users;
SELECT 
    repeat_customer_flag,
    COUNT(*) AS users,
    ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM users), 2) AS percentage
FROM users
GROUP BY repeat_customer_flag;


SELECT 
    u.repeat_customer_flag,
    SUM(r.Purchase_Amount) AS total_revenue,
    ROUND(100 * SUM(r.Purchase_Amount) / 
        (SELECT SUM(Purchase_Amount) FROM raw_transactions), 2
    ) AS revenue_share_percentage
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY u.repeat_customer_flag;

SELECT 
    u.repeat_customer_flag,
    ROUND(AVG(r.Purchase_Amount), 2) AS avg_purchase_value
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY u.repeat_customer_flag;

SELECT 
    u.repeat_customer_flag,
    COUNT(DISTINCT CASE WHEN r.Rating IS NOT NULL THEN r.Customer_ID END) 
        AS users_who_rated,
    COUNT(DISTINCT r.Customer_ID) AS purchasing_users,
    ROUND(
        100 * COUNT(DISTINCT CASE WHEN r.Rating IS NOT NULL THEN r.Customer_ID END) 
        / COUNT(DISTINCT r.Customer_ID),
    2) AS rating_participation_rate
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY u.repeat_customer_flag;

SELECT 
    u.repeat_customer_flag,
    COUNT(*) * 1.0 / COUNT(DISTINCT r.Customer_ID) 
        AS avg_transactions_per_user
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY u.repeat_customer_flag;

SELECT 
    u.repeat_customer_flag,
    ROUND(AVG(r.Rating), 2) AS avg_rating
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY u.repeat_customer_flag;

SELECT 
    u.repeat_customer_flag,
    r.Category,
    COUNT(*) AS transactions
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY u.repeat_customer_flag, r.Category
ORDER BY u.repeat_customer_flag, transactions DESC;

SELECT 
    DATE_FORMAT(Purchase_Date, '%Y-%m') AS month,
    SUM(Purchase_Amount) AS monthly_revenue
FROM raw_transactions
GROUP BY month
ORDER BY month;
SELECT 
    DATE_FORMAT(r.Purchase_Date, '%Y-%m') AS month,
    u.repeat_customer_flag,
    SUM(r.Purchase_Amount) AS revenue
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY month, u.repeat_customer_flag
ORDER BY month;
SELECT 
    DATE_FORMAT(Purchase_Date, '%Y-%m') AS month,
    ROUND(
        100 * SUM(CASE WHEN Repeat_Customer = 'Yes' THEN 1 ELSE 0 END)
        / COUNT(*),
    2) AS repeat_share_percentage
FROM raw_transactions
GROUP BY month
ORDER BY month;
SELECT 
    DATE_FORMAT(r.Purchase_Date, '%Y-%m') AS month,
    u.repeat_customer_flag,
    SUM(r.Purchase_Amount) AS revenue
FROM raw_transactions r
JOIN users u 
    ON r.Customer_ID = u.Customer_ID
GROUP BY month, u.repeat_customer_flag
ORDER BY month;
