show tables;
use covid_project;

describe covid_deaths;

-- Data Cleaning - covid_deaths

alter table covid_deaths 
rename column `State/UnionTerritory` to State;

alter table covid_deaths 
rename column ConfirmedIndianNational to conf_ind;

alter table covid_deaths 
rename column ConfirmedForeignNational to conf_for;

delete from covid_deaths 
where State = 'Cases being reassigned to states';

delete from covid_deaths
where State = 'Daman & Diu';

delete from covid_deaths 
where State = 'Unassigned';

update covid_deaths 
set State = 'Dadra and Nagar Haveli and Daman and Diu' 
where State = 'Dadra and Nagar Haveli';

update covid_deaths 
set State = 'Bihar' 
where State = 'Bihar****';
  
update covid_deaths 
set State = 'Maharashtra' 
where State = 'Maharashtra***';  

update covid_deaths 
set state = 'Karnataka'
where state = 'Karanataka';

update covid_deaths
set state = 'Himachal Pradesh'
where state = 'Himanchal Pradesh';

update covid_deaths
set state = 'Madhya Pradesh'
where state = 'Madhya Pradesh***';

update covid_deaths 
set State = 'Telangana' 
where State = 'Telengana';

-- adding date column with datatype of datetime

alter table covid_deaths 
add column date_num date;

update covid_deaths 
set date_num = concat(substr(date, 7, 4),'-',substr(date, 4, 2),'-', substr(date, 1, 2));

alter table covid_deaths 
change column date_num date_num date after sno;

alter table covid_deaths 
drop column date;

alter table covid_deaths 
rename column date_num to date;

-- Adding extra Features to the table:
-- Active Cases per day -- done

alter table covid_deaths 
add column active_cases int;

update covid_deaths 
set active_cases = (confirmed - deaths - cured);

-- adding column confirmed_per_day
alter table covid_deaths 
add column confirmed_per_day int;

with v as (
select
 date, 
 state, 
 confirmed - lag(confirmed,1) over (partition by state order by date) as per_day_confirmed
 from covid_deaths
) update covid_deaths join v on 
(covid_deaths.date = v.date and covid_deaths.state = v.state) 
set confirmed_per_day = v.per_day_confirmed;

select * from covid_deaths;

-- adding column death_per_day

alter table covid_deaths 
add column death_per_day int;

with v as (
select
 date, 
 state, 
 deaths - lag(deaths,1) over (partition by state order by date) as per_day_death
 from covid_deaths
) update covid_deaths join v on 
(covid_deaths.date = v.date and covid_deaths.state = v.state) 
set death_per_day = v.per_day_death;



-- adding column cured_per_day

alter table covid_deaths 
add column cured_per_day int;

with v as (
select
 date, 
 state, 
 cured - lag(cured,1) over (partition by state order by date) as per_day_cured
 from covid_deaths
) update covid_deaths join v on 
(covid_deaths.date = v.date and covid_deaths.state = v.state) 
set cured_per_day = v.per_day_cured;

-- Top 10 states with highest total cases

select state, max(confirmed) as Maximum_Deaths 
from covid_deaths 
group by state 
order by maximum_deaths desc limit 10;

-- Top 10 states with maximum active cases at any time

with v as
(select  date,
		 state,
		 date_format(date, '%Y-%m') as month,
		 max(active_cases) over (partition by state) as max_active_cases,
		 dense_rank() over (partition by State order by active_cases desc) as HighestActive
from covid_deaths)
select state, month, max_active_cases 
from v 
where HighestActive = 1 
order by max_active_cases desc limit 10;

-- Max Per Day Confirmed cases in States

with v as 
(
	select date,
    state,
    max(confirmed_per_day) over (partition by state) as max_confirmed_per_day,
    dense_rank() over (partition by state order by confirmed_per_day desc) as highestconfirmed
    from covid_deaths
) select date, state, max_confirmed_per_day
from v
where highestconfirmed = 1
order by max_confirmed_per_day desc;

-- Max Per Day Death cases in States

with v as (
	select date,
    state,
    max(death_per_day) over (partition by state) as max_death_per_day,
    dense_rank() over (partition by state order by death_per_day desc) as highestdeaths
    from covid_deaths
) select date, state, max_death_per_day
from v
where highestdeaths = 1
order by max_death_per_day desc;

-- State-wise Mortality Rate

with v as (
	select state,
    round((max(deaths) / max(confirmed)) * 100, 2) as mortality_rate
    from covid_deaths 
    group by state
) select state, mortality_rate
from v
order by mortality_rate desc
limit 10; 

-- data cleanign - covid_vaccinations

-- date

alter table covid_vaccinations 
add column date_num date;

alter table covid_vaccinations
rename column `Updated On` to date;

update covid_vaccinations 
set date_num = concat(substr(date, 7, 4),'-',substr(date, 4, 2),'-', substr(date, 1, 2));

alter table covid_vaccinations 
change column date_num date_num date after state;

alter table covid_vaccinations 
drop column date;

alter table covid_vaccinations 
rename column date_num to date;

alter table covid_vaccinations 
rename column `First Dose Administered` to first_dose;

alter table covid_vaccinations 
rename column `Second Dose Administered` to second_dose;

-- adding column - total_doses

alter table covid_vaccinations 
add column total_doses int;

update covid_vaccinations
set total_doses = first_dose + second_dose;

select * from covid_vaccinations limit 5;

-- adding column - vacc_per_day

alter table covid_vaccinations
add column vacc_per_day int;

with v as (
	select date, state,
	total_doses - lag(total_doses ,1) over (partition by state order by date) as per_day_vacc
    from covid_vaccinations
) update covid_vaccinations join v on (covid_vaccinations.state = v.state and covid_vaccinations.date = v.date) 
set vacc_per_day = v.per_day_vacc;

-- maximum vaccinations per state (exclude India)

select state, max(total_doses) as total_vaccination
from covid_vaccinations
where state != 'India'
group by state
order by total_vaccination desc;

-- maximum vaccination on any one dayper state (exclude India)

select date, state, vacc_per_day from (
	select date, state, max(vacc_per_day) over (partition by state)as vacc_per_day,
	dense_rank() over (partition by state order by vacc_per_day) as highestvaccination
	from covid_vaccinations
) as d where highestvaccination = 1 and state != 'India' order by vacc_per_day desc;

