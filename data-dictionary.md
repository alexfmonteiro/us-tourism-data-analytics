## Data Dictionary

Here we have a data dictionary for the proposed data model.

### Arrivals
column name | description
---: | :---
| id | arrival unique id |
| airport | port of entry code |
| arrival_date | immigrant arrival date |
| departure_date | immigrant departure date |
| mode | arrival mode (e.g. land, air, etc.) |
| visa | visa code |
| visatype | visa type |
| age | age of the immigrant |
| gender | gender of the immigrant |
| airline | airline code |
| flight_num | flight number |
| occupation | occupation of the immigrant |
| admission_num | admission number |
| origin_country | immigrant's country of origin |

### Visa
column name | description
---: | :---
| type | type of visa (e.g. B-1) |
| purpose | visa purpose |
| code | visa i94 code |
| description | visa description |

### i94mode 
column name | description
---: | :---
| code | mode code |
| description | mode description (e.g. Air, Land, etc.)|

### Calendar
column name | description
---: | :---
| date | date in the format YYYY-MM-DD |
| day | day as number |
| week_day | week day as string |
| month | month as number |
| month_name | month name as string |
| quarter | quarter of the year |
| year | year as number |
| season | season as number |
| season_name | season name as string |

### Ports
column name | description
---: | :---
| code | port of entry code |
| city | port of entry city |
| state | port of entry state |

### Airlines
column name | description
---: | :---
| name | airline company name |
| code | IATA code |
| country | company's origin country |

### Countries
column name | description
---: | :---
| code | country code |
| country | country name |

### Temperatures
column name | description
---: | :---
| city | US city|
| month | month as a 1-12 number |
| avg_temp | historical average temperature per month |