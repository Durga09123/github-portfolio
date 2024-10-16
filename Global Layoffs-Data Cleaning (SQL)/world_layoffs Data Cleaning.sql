
-- SQL Project - Data Cleaning

-- https://www.kaggle.com/datasets/swaptr/layoffs-2022


select * from layoffs;

-- 1. remove duplicates
-- 2. Standardize the data ( if there are issues in data inspelling or something like that we have to standarize it)
-- 3. Null Values or Blank Values 
-- 4. Remove any columns or rows 

--

create table layoffs_staging like layoffs;

select * from layoffs_staging;



insert layoffs_staging select * from layoffs;

select * from layoffs_staging;

select *, row_number() over (
partition by company,location,industry,total_laid_off,percentage_laid_off, 
`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

with duplicate_row as(
select *, row_number() over (
partition by company,location,industry,total_laid_off,percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) as row_num
from layoffs_staging)
select * from duplicate_row where row_num > 1;

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
select * from layoffs_staging2;

Insert layoffs_staging2 select *,row_number() over (
partition by company,location,industry,total_laid_off,percentage_laid_off, 
`date`, stage, country, funds_raised_millions ) as row_num
from layoffs_staging;

select * from layoffs_staging2 where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

delete from layoffs_staging2 where row_num > 1;
select * from layoffs_staging2;


-- standardizing data

select company,Trim(company) from layoffs_staging2; -- trim is used to remove the white space 

update layoffs_staging2 
set company = trim(company);

select distinct industry 
from layoffs_staging2 
order by 1;

select * 
from layoffs_staging2 
where industry like 'Crypto%';

update layoffs_staging2 
set industry = 'Crypto'
where industry like 'Crypto%';

select * from layoffs_staging2;

select distinct Location
from layoffs_staging2 
order by 1;

select * 
from layoffs_staging2 
where country like 'United States%';

select distinct country
from layoffs_staging2 
order by 1;

select distinct country,Trim(trailing '.' from country) 
from layoffs_staging2 
order by 1;


update layoffs_staging2 
set country = Trim(trailing '.' from country) 
where country like 'United States%';

select `date` ,
str_to_date (`date`,'%m/%d/%Y')from layoffs_staging2;

update layoffs_staging2 
set `date` = str_to_date (`date`,'%m/%d/%Y');

select * from layoffs_staging2;

alter table layoffs_staging2
modify column `date` DATE;


-- 3. Null Values or Blank Values 
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

update layoffs_staging2
set  industry = null where industry = '';

select * from layoffs_staging2 where industry is null or industry = '';
select * from layoffs_staging2 where company = 'Airbnb';

select t1.industry, t2.industry from layoffs_staging2 t1 
join layoffs_staging2 t2 on t1.company = t2.company 
where t1.industry is null and  t2.industry is not null;

update layoffs_staging2 t1 join layoffs_staging2 t2 on t1.company = t2.company 
set t1.industry = t2.industry
where t1.industry is null and t2.industry is not null;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

delete from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

select * from layoffs_staging2;

-- remove any columns or rows

alter table layoffs_staging2
drop column row_num;
