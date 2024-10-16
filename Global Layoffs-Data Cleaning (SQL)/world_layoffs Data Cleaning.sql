-- Data Cleaning

SELECT * FROM layoffs;

-- First, we'll create a staging table, which is where we will clean the data. This allows us to keep the original raw data safe in case anything goes wrong.

CREATE TABLE layoffs_staging LIKE layoffs;

SELECT * FROM layoffs_staging;

INSERT layoffs_staging SELECT * FROM layoffs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary 

-- 1. Remove Duplicates

# First let's check for duplicates


SELECT * FROM layoffs_staging;

SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`) AS row_num
	FROM  layoffs_staging;

SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM layoffs_staging
) duplicates
WHERE  row_num > 1;

-- let's just look at oda to confirm

SELECT * FROM layoffs_staging
WHERE company = 'Oda';
 
-- It seems like these are all valid entries, so they shouldn't be deleted. We need to carefully check each row to ensure accuracy.
 -- these are our real duplicates 

SELECT *, ROW_NUMBER() OVER (
	PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, 
    `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

WITH duplicate_row as(
SELECT *, ROW_NUMBER() OVER (
	PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, 
    `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging)

SELECT * FROM duplicate_row
WHERE row_num > 1;



CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;
SELECT * FROM layoffs_staging2;

INSERT layoffs_staging2 SELECT *,ROW_NUMBER() OVER (
     PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, 
     `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_staging2 
WHERE row_num > 1;

SELECT * FROM layoffs_staging2;


-- standardizing data

SELECT company, TRIM(company)
FROM layoffs_staging2;    -- trim is used to remove the white space 

UPDATE layoffs_staging2 
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2 
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT * FROM layoffs_staging2;

SELECT DISTINCT Location
FROM layoffs_staging2
ORDER BY 1;

SELECT * FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2 
SET  country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

SELECT  `date`, STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


-- 3. Handling with Null Values or Blank Values 
SELECT  *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2 
SET  industry = NULL
WHERE industry = '';

SELECT * FROM layoffs_staging2
WHERE industry IS NULL OR industry = '';

SELECT * FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN
    layoffs_staging2 t2 ON t1.company = t2.company
WHERE
    t1.industry IS NULL AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN
    layoffs_staging2 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL AND t2.industry IS NOT NULL;

-- 4. remove any columns or rows that are not necessary 
SELECT * FROM layoffs_staging2;

SELECT  *
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE FROM layoffs_staging2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;

SELECT  * FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
