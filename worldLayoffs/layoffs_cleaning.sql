-- Data Cleaning

-- Selecting all columns from the original table for initial inspection
SELECT *
FROM layoffs;

-- 1. Remove duplicates

-- Creating a staging table to preserve raw data for cleaning/transformation
DROP TABLE IF EXISTS layoffs_staging;
CREATE TABLE layoffs_staging
LIKE layoffs;

-- Checking the structure of the new staging table
SELECT *
FROM layoffs_staging;

-- Copying raw data to the new staging table
INSERT layoffs_staging
SELECT * FROM layoffs;

-- Exploring and identifying duplicate records
WITH duplicates_cte AS (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
        `date`, stage, country, funds_raised_millions
    ) AS row_id
    FROM layoffs_staging
)
SELECT * FROM  duplicates_cte
WHERE row_id > 1;

-- Double checking duplicate records for a specific company
SELECT *
FROM layoffs_staging WHERE company = 'Cazoo';

-- Creating 2nd staging table to delete duplicate rows. Can't delete from CTE in MySQL
CREATE TABLE `layoffs_staging2` AS (
    SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
        `date`, stage, country, funds_raised_millions
    ) AS row_id
    FROM layoffs_staging
);

-- Inspecting duplicate rows in the second staging table
SELECT *
FROM layoffs_staging2 WHERE row_id > 1;

-- Deleting duplicate rows from the second staging table
DELETE FROM layoffs_staging2
WHERE row_id > 1;

-- 2. Standardize the Data
-- Removing extra white spaces from text columns
UPDATE layoffs_staging2
SET company = TRIM(company), 
location = TRIM(location),
industry = TRIM(industry),
country = TRIM(country);

-- Inspecting unique values in columns for standardization
SELECT DISTINCT industry 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location 
FROM layoffs_staging2
ORDER BY 1;

-- Standardizing data based on inspection results
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%';

-- Converting date strings to date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Changing the data type of the date and percentage columns
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE,
MODIFY COLUMN percentage_laid_off FLOAT4;


-- 3. Handling Null Values
-- Inspecting rows with null values
SELECT *
FROM layoffs_staging2
WHERE industry = '' OR industry IS NULL;

-- Populating missing industry values based on similar rows with data
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry = '' AND t2.industry NOT IN ('');

-- Deleting rows with unsalvageable null values
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL AND
percentage_laid_off IS NULL;

-- 4. Remove unnecessary columns
-- Dropping the row_id column
ALTER TABLE layoffs_staging2
DROP COLUMN row_id; 

-- Housekeeping
-- Dropping the original staging table and renaming the second staging table
DROP TABLE layoffs_staging;
ALTER TABLE layoffs_staging2
RENAME TO layoffs_staging;

-- Displaying the cleaned data from the staging table
SELECT *
FROM layoffs_staging
LIMIT 100;
