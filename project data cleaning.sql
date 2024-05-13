-- data cleaning 
-- fix issues in the raw data 
select * 
from layoffs;
-- aim of this project (Data cleaning)
-- 1. remove duplicates
-- 2. standardise the data 
-- 3. null values 
-- 4. remove any cols

create table layoffs_staging   #right now does not contain any data
like layoffs;

insert layoffs_staging  #inserting data 
select *
from layoffs;

select *
from layoffs_staging;


-- removing the duplicates

select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;


with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'casper' ;


with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
delete #does not work because you can not update a cte and a delete statement is like an update statement
from duplicate_cte
where row_num > 1;

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

select * 
from layoffs_staging2;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

delete 
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2;

-- standardizing data 
-- finding issues and fixing them

select DISTINCT(company), trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select DISTINCT industry
from layoffs_staging2;

# issue - in thecolumns of industry some entries are similar but written differenty. for ex cryptocurrency and crypto currency

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Cypto%';

select DISTINCT country, trim(trailing '.' from country)
from layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

UPDATE layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

Alter table layoffs_staging2
modify column `date` DATE;

-- working with nulls and blank values

SELECT *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

SELECT *
from layoffs_staging2
where industry is null
or industry = '';

-- we wanna populate the industry field for airbnd because when we checked the company airbnb, the industry value is travel so that is not going to change. therefore let us fill up the value for the industry

select *
from layoffs_staging2
where company = 'airbnb';

select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
where (t1.industry is null)
and t2.industry is not null;

UPDATE layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null)
and t2.industry is not null;

-- deleting a row

delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

-- checking
select *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null; 

-- dropping a column

alter table layoffs_staging2
drop COLUMN row_num;

SELECT * from layoffs_staging2;