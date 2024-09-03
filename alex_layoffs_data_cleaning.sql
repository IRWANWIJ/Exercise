-- Data Cleaning

CREATE DATABASE world_layoffs ;

USE world_layoffs ;

SELECT * FROM layoffs ;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove any columns

-- To keep raw data, we create new table which similiar to raw data

CREATE TABLE layoffs_staging
LIKE layoffs ;

SELECT * FROM layoffs_staging ;

-- To insert data to new table with data from original table

INSERT INTO layoffs_staging
SELECT * FROM layoffs ;

-- Remove Duplicates
-- Find duplicate use WINDOW FUNCTION AND RAW

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
           `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging ;

-- using CTE

WITH duplicate_cte AS
(
 SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
           `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging 
) 
SELECT * FROM duplicate_cte
WHERE row_num > 1 ;

-- Before delete duplicates, pls check one more time

SELECT * FROM layoffs_staging 
WHERE company = 'Casper' ;

-- Now to remove duplicates, create new table same as layoffs_staging but add one more column row_num 
-- and transfer all data from duplicate_cte to new table

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
           `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging ;

-- filter row_num, which > 1, should be duplicates, then delete them

SELECT * FROM layoffs_staging2
WHERE row_num > 1 ;

DELETE
FROM layoffs_staging2
WHERE row_num > 1 ;

SELECT * FROM layoffs_staging2 ; -- duplicates already removed

-- Standardizing Data

SELECT company, TRIM(company)
FROM layoffs_staging2 ;

UPDATE layoffs_staging2
SET company = TRIM(company) ;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry ; -- look through, found crypto, Crypto Currency CryptoCurrency should be same industry

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%' ;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%' ;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1 ;               -- Found United States & United States. 

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1 ;  


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%' ;        

-- Cleaning date data
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2 ;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ;

SELECT `date` FROM layoffs_staging2 ;  -- The format looks fine, but the type still text

ALTER TABLE layoffs_staging2
MODIFY `date` DATE ;             -- Now, date type data already correct

USE world_layoffs ;

-- Null or Blank Values


SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL ;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
		AND percentage_laid_off IS NULL;
        
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '' ;          

SELECT * 
FROM layoffs_staging2 
WHERE industry IS NULL
OR industry = '' ;        -- This to check industry column data that have blank or null data

SELECT * 
FROM layoffs_staging2 
WHERE company = 'juul' ;

SELECT * 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

-- To make it simpler, easier to see

SELECT t1.industry, t2.industry 
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;                  --

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;     -- this is not working, therefore we set blank row on industry into null (row 143 at above)


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL ;  -- this to fill out same company which industry column blank meanwhile at other row has data
                               -- for example, Airbnb has blank data at industry column, 
                               -- meanwhile at other row Airbnb showed travel at industry column

-- Before we found Airbnb, Carvana, Juul and Bally's Interactive have blank or null data at industry column
-- Now we check the result after we the result

SELECT * 
FROM layoffs_staging2 
WHERE industry IS NULL
OR industry = '' ;      -- left Bally's Interactive, which we can do nothing as only one company name as Bally's Interactive

SELECT * 
FROM layoffs_staging2 
WHERE company LIKE 'Bally%' ;  -- There is only one Bally's interactive on Company column


-- Remove row and column

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
		AND percentage_laid_off IS NULL;
        
DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
		AND percentage_laid_off IS NULL;
        
SELECT * FROM layoffs_staging2 ;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num ;         -- to delete row_num column as no longer needed         














