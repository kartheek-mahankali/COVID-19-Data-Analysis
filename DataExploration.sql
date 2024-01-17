-- Check if the counts match for both tables

SELECT COUNT(1)
FROM coviddeaths

SELECT COUNT(1)
FROM covidvaccinations

-- Get basic numbers from coviddeaths

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM coviddeaths
WHERE continent IS NOT NULL
ORDER BY 1,2

-- Looking at total cases vs total deaths
-- Shows the likelihood of death if you're affected with COVID in India

SELECT location, 
date, 
total_cases, 
total_deaths, 
(total_deaths::NUMERIC/total_cases)*100 AS death_percentage
FROM coviddeaths
WHERE location = 'India'
AND continent IS NOT NULL
AND total_deaths IS NOT NULL
ORDER BY 1,2

-- Looking at the highest cases vs population

SELECT location,  
population, 
MAX(total_cases) AS highest_casecount,
COALESCE(MAX(total_cases::NUMERIC/population)*100,0) AS case_percentage
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY location, population
ORDER BY case_percentage DESC


-- Looking at the highest death counts at country level

SELECT location,  
MAX(total_deaths) AS highest_deathcount
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY highest_deathcount DESC

-- Global numbers

SELECT date, 
SUM(new_cases) AS total_cases, 
SUM(new_deaths) AS total_deaths, 
SUM(new_deaths)/SUM(new_cases) * 100 AS death_percentage
FROM coviddeaths
WHERE continent IS NOT NULL
AND new_cases > 0
GROUP BY 1
ORDER BY 1,2

-- People vaccinated vs population by day

WITH TB1 AS
(SELECT dea.continent, 
dea.location, 
dea.date, 
dea.population, 
vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinations
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL
-- AND dea.location = 'India'
ORDER BY 2,3
)

SELECT *, rolling_vaccinations/population * 100 AS vaccine_percentage
FROM TB1
WHERE new_vaccinations IS NOT NULL

-- Creating Views
CREATE VIEW Percent_people_vaccinated AS
SELECT dea.continent, 
dea.location, 
dea.date, 
dea.population, 
vac.new_vaccinations,
SUM(vac.new_vaccinations) OVER (PARTITION BY dea.location ORDER BY dea.location, dea.date) AS rolling_vaccinations
FROM coviddeaths dea
JOIN covidvaccinations vac
	ON dea.location = vac.location
	AND dea.date = vac.date
WHERE dea.continent IS NOT NULL

-- Countries ranked on peak cases
SELECT 
location, 
population, 
MAX(total_cases) AS peak_cases, 
MAX(total_deaths) AS peak_deaths
FROM coviddeaths
WHERE continent IS NOT NULL
GROUP BY location, population
HAVING MAX(total_cases) IS NOT NULL AND MAX(total_deaths) IS NOT NULL
ORDER BY MAX(total_cases) DESC
