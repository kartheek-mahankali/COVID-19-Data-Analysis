-- CREATE TABLES

CREATE TABLE coviddeaths (
iso_code	VARCHAR(20),
continent	VARCHAR(30),
location	VARCHAR(50),
date	DATE,
population	BIGINT,
total_cases	BIGINT,
new_cases	BIGINT,
total_deaths	BIGINT,
new_deaths	BIGINT,
total_cases_per_million	DECIMAL(14,6),
new_cases_per_million	DECIMAL(14,6),
total_deaths_per_million	DECIMAL(14,6),
new_deaths_per_million	DECIMAL(14,6),
reproduction_rate	DECIMAL(14,6),
icu_patients	BIGINT,
icu_patients_per_million	DECIMAL(14,6),
hosp_patients	BIGINT,
hosp_patients_per_million	DECIMAL(14,6),
weekly_icu_admissions	DECIMAL(14,6),
weekly_icu_admissions_per_million	DECIMAL(14,6),
weekly_hosp_admissions	BIGINT,
weekly_hosp_admissions_per_million	DECIMAL(14,6)
)


CREATE TABLE covidvaccinations(
iso_code	VARCHAR(20),
continent	VARCHAR(30),
location	VARCHAR(50),
date	DATE,
total_tests	BIGINT,
new_tests	BIGINT,
total_tests_per_thousand	DECIMAL(14,6),
new_tests_per_thousand	DECIMAL(14,6),
positive_rate	DECIMAL(14,6),
tests_per_case	DECIMAL(14,6),
tests_units	VARCHAR(30),
total_vaccinations	BIGINT,
people_vaccinated	BIGINT,
people_fully_vaccinated	BIGINT,
total_boosters	BIGINT,
new_vaccinations	BIGINT,
total_vaccinations_per_hundred	DECIMAL(14,6),
people_vaccinated_per_hundred	DECIMAL(14,6),
people_fully_vaccinated_per_hundred	DECIMAL(14,6),
total_boosters_per_hundred	DECIMAL(14,6),
stringency_index	DECIMAL(14,6),
population_density	DECIMAL(14,6),
median_age	DECIMAL(14,6),
aged_65_older	DECIMAL(14,6),
aged_70_older	DECIMAL(14,6),
gdp_per_capita	DECIMAL(14,6),
extreme_poverty	DECIMAL(14,6),
cardiovasc_death_rate	DECIMAL(14,6),
diabetes_prevalence	DECIMAL(14,6),
female_smokers	DECIMAL(14,6),
male_smokers	DECIMAL(14,6),
handwashing_facilities	DECIMAL(14,6),
hospital_beds_per_thousand	DECIMAL(14,6),
life_expectancy	DECIMAL(14,6),
human_development_index	DECIMAL(14,6)
)

-- TOTAL CASES, NEW CASES, TOTAL DEATHS, NEW DEATHS, DEATH PERCENTAGE, PERCENT POPULATION INFECTED

SELECT location, date, population, 
total_cases, new_cases, total_deaths, 
new_deaths, (total_deaths::NUMERIC/total_cases)*100 AS death_percentage,
(total_cases::NUMERIC/population)*100 AS percent_pop_infected
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2

-- COUNTRY WISE AND YEAR WISE DEATHS PER MILLION (TB1)
SELECT continent, location, date, new_deaths_per_million
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2

SELECT location, date, new_cases_per_million
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2

-- COUNTRY WISE NEW CASES PER MILLION (TB2)
SELECT location, date, new_cases_per_million,
SUM(new_cases_per_million) OVER (PARTITION BY location ORDER BY location, date) AS rolling_new_cases_per_million
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2,3
ORDER BY 1,2

-- TB2-1 : Country wise highest infection count and percent_pop_infected
SELECT location, population, MAX(total_cases::NUMERIC) AS highest_infection_count,
(MAX(total_cases::NUMERIC)/population)*100 AS percent_pop_infected
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 4 DESC

--TB2-2 : Time series of new_cases_per_million for the countries with highest infection count
WITH Q2 AS (
SELECT location, population, MAX(total_cases::NUMERIC) AS highest_infection_count,
(MAX(total_cases::NUMERIC)/population)*100 AS percent_pop_infected
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 4 DESC
)

SELECT location, date, new_cases_per_million
FROM coviddeaths
WHERE continent IS NOT NULL
AND location IN (SELECT location
FROM Q2
WHERE highest_infection_count IS NOT NULL
ORDER BY highest_infection_count DESC
LIMIT 10)
ORDER BY 1,2


-- HIGHEST DEATH COUNT PER COUNTRY (TB3)
SELECT location, population, 
MAX(total_deaths) AS highest_death_count, (MAX(total_deaths::NUMERIC)/population)*100 AS death_percentage
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC

-- VACCINATIONS, PERCENT POPULATION VACCINATED (TB4)
SELECT dea.location, dea.date, dea.population, vac.people_vaccinated,
(vac.people_vaccinated/dea.population::NUMERIC)*100 AS percent_pop_vaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT null
AND vac.people_vaccinated IS NOT NULL

-- INFECTION COUNT, TOTAL DEATHS VS GDP PER CAPITA, HUMAN DEVELOPMENT INDEX (TB5)
WITH TB5 AS (
SELECT dea.location, dea.population, MAX(dea.total_cases) AS highest_infection_count,
MAX(dea.total_deaths) AS highest_death_count, MAX(vac.people_vaccinated) AS highest_vacc_count,
MAX(vac.gdp_per_capita) AS gdp_per_capita,
MAX(vac.human_development_index) AS human_development_index
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
AND vac.gdp_per_capita IS NOT NULL
AND vac.human_development_index IS NOT NULL
GROUP BY 1,2
)
SELECT *,
(highest_infection_count::NUMERIC/population)*100 AS percent_pop_infected, 
(highest_death_count::NUMERIC/population)*100 AS percent_pop_dead, 
(highest_vacc_count::NUMERIC/population::NUMERIC)*100 AS percent_pop_vaccinated
FROM TB5

-- STRINGENCY INDEX VS INFECTION PERCENTAGE, DEATH PERCENTAGE (TB6)
SELECT dea.location, dea.date, dea.total_cases, 
(dea.total_cases::NUMERIC/dea.population)*100 AS percent_pop_infected, dea.total_deaths, 
(dea.total_deaths::NUMERIC/dea.total_cases)*100 AS death_percentage, 
vac.stringency_index
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL

-- PERCENT_POP_INFECTED VS POPULATION DENSITY (TB7)
SELECT dea.location, dea.population, vac.population_density,
MAX(dea.total_cases) AS highest_infection_count, 
(MAX(dea.total_cases::NUMERIC)/dea.population)*100 AS percent_pop_infected
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
AND vac.population_density IS NOT NULL
GROUP BY 1,2,3
ORDER BY 1

-- GLOBAL NUMBERS (TB8) 

SELECT location, population, 
MAX(total_cases) AS highest_infection_count, MAX(total_deaths) AS highest_death_count
FROM coviddeaths
WHERE continent IS NULL
AND location IN ('Asia', 'Africa', 'North America', 'South America', 'Europe', 'Oceania')
GROUP BY location, population
ORDER BY 3 DESC

-- TOTAL NUMBERS (TB9)
SELECT SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, 
(SUM(new_deaths::NUMERIC)/SUM(new_cases))*100 AS death_percentage
FROM coviddeaths
WHERE continent IS NULL
AND location IN ('Asia', 'Africa', 'North America', 'South America', 'Europe', 'Oceania')
--------------------------------

--Highest infection count per country
SELECT location, population, MAX(total_cases_per_million::NUMERIC) AS highest_infection_count
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC

--New cases per million for top 20 countries with highest infection count
WITH Q2 AS (
SELECT location, population, MAX(total_cases::NUMERIC) AS highest_infection_count
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC
)

SELECT continent, location, date, new_cases_per_million
FROM coviddeaths
WHERE continent IS NOT NULL
AND location IN (SELECT location
FROM Q2
WHERE highest_infection_count IS NOT NULL
ORDER BY highest_infection_count DESC
LIMIT 20)
ORDER BY 1,2

--Highest death count per country (for the above 20 countries)
WITH Q3 AS (
SELECT location, population, MAX(total_cases::NUMERIC) AS highest_infection_count
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC
)

SELECT location, MAX(total_deaths) AS highest_death_count
FROM coviddeaths
WHERE continent IS NOT NULL
AND location IN (SELECT location
FROM Q3
WHERE highest_infection_count IS NOT NULL
ORDER BY highest_infection_count DESC
LIMIT 20)
GROUP BY 1
ORDER BY 2 DESC


-- Most people vaccinated and population (for the above 20 countries)
WITH Q4 AS (
SELECT location, population, MAX(total_cases::NUMERIC) AS highest_infection_count
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY 1,2
ORDER BY 3 DESC
)
SELECT dea.iso_code, dea.location, dea.population, MAX(vac.people_fully_vaccinated) AS people_fully_vaccinated,
(MAX(vac.people_fully_vaccinated::NUMERIC)/dea.population)*100 AS percent_pop_vaccinated
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.iso_code = vac.iso_code
	AND dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT null
AND vac.people_fully_vaccinated IS NOT NULL
AND dea.location IN (SELECT location
FROM Q4
WHERE highest_infection_count IS NOT NULL
ORDER BY highest_infection_count DESC
LIMIT 20)
GROUP BY 1,2,3
ORDER BY 4 DESC

SELECT * FROM coviddeaths where location = 'Qatar'

-- Infection count, total deaths vs gdp per capita and human development index
WITH TB5 AS (
SELECT dea.location, dea.population, MAX(dea.total_cases) AS highest_infection_count,
MAX(dea.total_deaths) AS highest_death_count, MAX(vac.people_vaccinated) AS highest_vacc_count,
MAX(vac.gdp_per_capita) AS gdp_per_capita,
MAX(vac.human_development_index) AS human_development_index
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
AND vac.gdp_per_capita IS NOT NULL
AND vac.human_development_index IS NOT NULL
GROUP BY 1,2
)
SELECT *,
(highest_infection_count::NUMERIC/population)*100 AS percent_pop_infected, 
(highest_death_count::NUMERIC/population)*100 AS percent_pop_dead, 
(highest_vacc_count::NUMERIC/population::NUMERIC)*100 AS percent_pop_vaccinated
FROM TB5

-- PERCENT_POP_INFECTED VS POPULATION DENSITY
SELECT dea.location, dea.population, vac.population_density,
MAX(dea.total_cases) AS highest_infection_count, 
(MAX(dea.total_cases::NUMERIC)/dea.population)*100 AS percent_pop_infected
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
AND vac.population_density IS NOT NULL
GROUP BY 1,2,3
ORDER BY 1

--Visualizations
-- 1. 

Select SUM(new_cases) as total_cases, 
SUM(new_deaths) as total_deaths, 
SUM(new_deaths::numeric)/SUM(new_cases)*100 as death_percentage
From coviddeaths
where continent is not null 
order by 1,2


-- 2. 

-- We take these out as they are not inluded in the above queries and want to stay consistent
-- European Union is part of Europe

Select location, SUM(cast(new_deaths as int)) as TotalDeathCount
From PortfolioProject..CovidDeaths
--Where location like '%states%'
Where continent is null 
and location not in ('World', 'European Union', 'International')
Group by location
order by TotalDeathCount desc


-- 3.

Select Location, Population, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From PortfolioProject..CovidDeaths
--Where location like '%states%'
Group by Location, Population
order by PercentPopulationInfected desc


-- 4.


Select Location, Population,date, MAX(total_cases) as HighestInfectionCount,  Max((total_cases/population))*100 as PercentPopulationInfected
From PortfolioProject..CovidDeaths
--Where location like '%states%'
Group by Location, Population, date
order by PercentPopulationInfected desc
