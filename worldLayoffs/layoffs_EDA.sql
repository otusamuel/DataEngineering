-- Exploratory data analysis

-- What was the biggest layoff?
SELECT 
    MAX(total_laid_off) AS max_total_laid_off,
    MAX(percentage_laid_off) AS max_percentage_laid_off
FROM 
    layoffs_staging;

-- Explore data period
SELECT 
    MIN(`date`) AS min_date,
    MAX(`date`) AS max_date
FROM 
    layoffs_staging;

-- Total layoff by company
SELECT 
    company, 
    SUM(total_laid_off) AS total_laid_off
FROM 
    layoffs_staging
GROUP BY 
    company 
ORDER BY 
    total_laid_off DESC;

-- Total layoff by industry
SELECT 
    industry, 
    SUM(total_laid_off) AS total_laid_off
FROM 
    layoffs_staging
GROUP BY 
    industry 
ORDER BY 
    total_laid_off DESC;

-- Total layoff by country
SELECT 
    country, 
    SUM(total_laid_off) AS total_laid_off
FROM 
    layoffs_staging
GROUP BY 
    country 
ORDER BY 
    total_laid_off DESC;

-- Total layoffs by year
SELECT 
    YEAR(`date`) AS year, 
    SUM(total_laid_off) AS total_laid_off
FROM 
    layoffs_staging
GROUP BY 
    YEAR(`date`)
ORDER BY 
    total_laid_off DESC;

-- Layoffs monthly rolling sum 
WITH monthly_sum AS (
    SELECT 
        SUBSTR(`date`, 1, 7) AS `month`, 
        SUM(total_laid_off) AS total_laid_off
    FROM 
        layoffs_staging 
    WHERE 
        SUBSTR(`date`, 1, 7) IS NOT NULL
    GROUP BY 
        1 
    ORDER BY 
        1
)
SELECT 
    `month`, 
    total_laid_off,
    SUM(total_laid_off) OVER (ORDER BY `month`) AS rolling_total
FROM 
    monthly_sum;

-- Average percentage of layoffs by industry
SELECT 
    industry, 
    ROUND(AVG(percentage_laid_off) * 100, 2) AS avg_percentage_laid_off
FROM 
    layoffs_staging
WHERE 
    industry IS NOT NULL
GROUP BY 
    industry
ORDER BY 
    avg_percentage_laid_off;

-- Number of companies with layoffs by stage
SELECT 
    stage, 
    COUNT(DISTINCT company) AS num_companies
FROM 
    layoffs_staging
WHERE 
    stage IS NOT NULL
GROUP BY 
    stage
ORDER BY 
    num_companies DESC;

-- Layoff distribution by stages
SELECT 
    stage, 
    COUNT(*) AS num_layoffs
FROM 
    layoffs_staging
WHERE 
    stage IS NOT NULL
GROUP BY 
    stage
ORDER BY 
    num_layoffs DESC;

-- layoff distribution by 'apparent company size'
SELECT
    CASE
        WHEN total_laid_off <= 100 THEN 'small'
        WHEN total_laid_off > 100 AND total_laid_off <= 500 THEN 'medium'
        ELSE 'large'
    END AS company_size,
    COUNT(*) AS num_layoffs
FROM 
    layoffs_staging
GROUP BY 
    company_size;
    
    
select * from layoffs_staging;
