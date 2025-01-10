# ETL pre MovieLens Dataset
Tento repozitár implementuje ETL proces v __Snowflake__, a následne analyzuje dáta z __MovieLens__ datasetu. Projekt sa zameriava na analýzu používateľských interakcií s filmovým obsahom na platforme, pričom sa sústreďuje na správanie používateľov, ako aj na ich demografické faktory.

## 1. Úvod a popis zdrojových dát
Cieľom je analyzovať a lepšie pochopiť, ktoré filmy a žánre sú najviac preferované, kto sú najaktívnejší používatelia a ako tieto dáta využiť na personalizované odporúčania, marketingové stratégie a optimalizáciu obsahu na platforme.

__Zdroj dát__: Dáta pochádzajú z datasetu [tu](https://grouplens.org/datasets/movielens/). 

Tento dataset obsahuje 8 tabuliek:

- __users__
- __age_group__
- __occupations__
- __movies__
- __tags__
- __genres_movies__
- __genres__
- __ratings__

Cielom ETL procesu bolo, aby tieto dáta pripravil, transformoval a sprístupnil pre ďalšiu analýzu.
___
### 1.1 Dátová architektúra
Entitno-relačný diagram (ERD)
Zdrojové dáta zobrazené na ER diagrame.

<p align="center">
  <img src="https://github.com/SomeUsername456/MovieLens-ETL/blob/main/MovieLens_ERD.png" alt=Entitno-relačná schéma MovieLens">
  <br />
  <i>Obrázok 1: Entitno-relačná schéma MovieLens</i>
</p>

Tabuľka ```users``` obsahuje informácie o jednotlivých používateľoch:
  - id: Unikátny identifikátor pre každého používateľa
  - age: Vek používateľa
  - gender: Pohlavie používateľa
  - zip_code: PŠČ používateľa
  - occupation_id: Cudzí kľúč odkazujúci na tabuľku ```occupations```
  - age_group_id: Cudzí kľúč odkazujúci na tabuľku ```age_group```

Tabuľka ```age_group``` slúži na kategorizáciu používateľov podľa veku:
  - id: Unikátny identifikátor pre každú vekovú skupinu
  - name: Názov vekovej skupiny

Tabuľka ```occupations``` slúži na kategorizáciu používateľov podľa zamestnania:
  - id: Unikátny identifikátor pre každé povolanie
  - name: Názov povolania

Tabuľka ```movies``` obsahuje informácie o jednotlivých filmoch:
  - id: Unikátny identifikátor pre každý film
  - title: Názov filmu
  - release_year: Rok vydania filmu

Tabuľka ```tags``` slúžiť na kategorizáciu filmových tagov:
  - id: Unikátny identifikátor pre každý tag.
  - user_id: Cudzí kľúč odkazujúci na tabuľku ```users```
  - movie_id: Cudzí kľúč odkazujúci na tabuľku ```movies```
  - tags: Názov tagu
  - created_at: Kedy bol tag priradený

Tabuľka ```genres``` obsahuje zoznam rôznych filmových žánrov:
  - id: Unikátny identifikátor pre každý žáner
  - name: Názov žánru

Tabuľka ```genres_movies``` vytvára spojenie medzi filmami a ich žánrami:
  - movie_id: Cudzí kľúč odkazujúci na tabuľku ```movies```
  - genre_id: Cudzí kľúč odkazujúci na tabuľku ```genres```

Tabuľka ```ratings``` obsahuje informácie o hodnoteniach filmov:
  - id: Unikátny identifikátor pre každé hodnotenie
  - user_id: Cudzí kľúč odkazujúci na tabuľku ```users```
  - movie_id: Cudzí kľúč odkazujúci na tabuľku ```movies```
  - rating: Hodnotenie filmu
  - rated_at: Kedy bolo hodnotenie udelené
___
## 2 Návrh dimenzionálneho modelu
Na účely analýzy bol vytvorneý __hviezdicový model__ (star schema). Faktovú tabuľku predstavuje fact_ratings, tá je prepojená s ďalšími dimenziami:
  - ```dim_movies```: Obsahuje informácie o filmoch (názov, autor, žáner, tagy).
  - ```dim_users```: Obsahuje informácie o používateľoch (pohlavie, PSČ, veková kategória, zamestnanie)
  - ```dim_date```: Obsahuje informácie o dátumoch hodnotení (deň, mesiac, rok, štvrťrok).
  - ```dim_time```: Obsahuje informácie o času hodnotení (hodina, minúta, sekunda).

Diagram hviezdicového modelu ukazuje predstavuje medzi faktovou tabuľkou a dimenziami.

<p align="center">
  <img src="https://github.com/SomeUsername456/MovieLens-ETL/blob/main/movieLens_star_schema.png" alt="Schéma hviezdy pre MovieLens">
  <br />
  <i>Obrázok 2: Schéma hviezdy pre MovieLens</i>
</p>

___
## 3. ETL proces v Snowflake
ETL proces sa skladá z 3 častí: __extrahovanie__ (Extract), __transformácia__ (Transform) a __načítanie__ (Load). Tento proces bol realizovaný na platforme __Snowflake__ s cieľom pripraviť dáta na analýzu.
___
### 3.1 Extract
Dáta z ```.csv``` súborov boli najprv nahraté do platformi Snowflake cez interný stage ```DOLPHIN_MOVIELENS_STAGE```, pomocou príkazu:
``` sql
CREATE OR REPLACE STAGE DOLPHIN_MOVIELENS_STAGE;
COPY INTO age_group_staging
FROM @DOLPHIN_MOVIELENS_STAGE/age_group.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE';
```
Tieto dáta obsahujú údaje o používateľoch a ich zamestnanie a vekovú skupinu, filmoch a ich žánrach a tagov, a hodnoteniach. Dáta boli importované do staging tabuliek pomocou príkazu ```COPY INTO```.
V prípade nekonzistentných záznamov bol použitý parameter ```ON_ERROR = 'CONTINUE'```, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.
___
### 3.2 Transform

V tejto fáze boli dáta zo staging tabuliek vyčistené a transformované, aby boli dimenzie a faktová tabuľka, pripravená na analýzu.

```dim_users``` obsahuje údaje o používateľoch: ich vekových kategórií, pohlavia, zamestnania a vzdelania. Pridanie vekových skupín: Hodnota ```us.age``` bola mapovaná cez ```age_group_staging``` na názov vekovej skupiny (```age.name```), napr. "25-34".
Pridanie povolaní: Hodnota ```us.occupations_id``` bola mapovaná na názov povolania cez ```occupations_staging``` (```oc.name```). Táto tabuľka je typu SCD 2, čiže môžeme sledovať zmeny v zamestnaní používateľov.
``` sql
CREATE TABLE dim_users AS
SELECT DISTINCT
    us.users_id,
    us.gender,
    age.name AS age_group,
    oc.name AS occupation
FROM users_staging us
JOIN age_group_staging age ON us.age = age.age_group_id
JOIN occupations_staging oc ON us.occupations_id = oc.occupations_id;
```

```dim_time``` obsahuje údaje o časoch, kedy bolo hodnotenia urobené. ```ROW_NUMBER()``` usporiadal timestampy podľa hodiny. ```DATE_TRUNC('HOUR', ratings.rated_at)``` zaokrúhlilo dátum a čas na hodinu.
``` sql
CREATE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('HOUR', ratings.rated_at)) AS time_id,
    TO_TIMESTAMP(ratings.rated_at) AS time,
    TO_NUMBER(TO_CHAR(ratings.rated_at, 'HH24')) AS hour
FROM ratings_staging ratings
GROUP BY ratings.rated_at;
```

```dim_date``` obsahuje informácie o dátumoch, kedy boli hodnotenia urobené. Pomocou funkcií ```DATE_PART``` boli z ```rated_at``` extrahované časové intervaly (deň, mesiac, rok).
```CASE``` bol použitý na prevod číselných hodnôt na text. Obidve tieto tabuľky sú typu SCD, keďže obsahujú údaje, ktoré sú nemenia.
``` sql
CREATE TABLE dim_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY rated_at) AS date_id,
    CAST(rated_at AS DATE) AS date,  
    DATE_PART(day, rated_at) AS day,
    DATE_PART(month, rated_at) AS month,
    DATE_PART(year, rated_at) AS year,                
    DATE_PART(week, rated_at) AS week,
    DATE_PART(dow, rated_at) + 1 AS day_of_week,        
    CASE DATE_PART(dow, rated_at) + 1
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week_string,             
    CASE DATE_PART(month, rated_at)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END AS month_string         
FROM ratings_staging
GROUP BY rated_at,
         DATE_PART(day, rated_at),
         DATE_PART(month, rated_at), 
         DATE_PART(year, rated_at), 
         DATE_PART(week, rated_at), 
         DATE_PART(dow, rated_at);
```
```dim_movies``` obsahuje metadáta o; ich názov, rok vydania, žánre, tagy. Pomocou ```genres_movies_staging``` a ```genres_staging``` bol pre každý film zistený žáner a pomocou ```tags_staging``` boli pridané tagy. Táto tabuľka je typu SCD 1, pretože k filmu sa môžu pridať rôzne tagy.
``` sql
CREATE TABLE dim_movies AS
SELECT DISTINCT
    movies.movies_id,
    movies.title,
    movies.release_year,
    genres.name,
    tags.tags
FROM movies_staging movies
JOIN genres_movies_staging gm ON gm.movies_id = movies.movies_id
JOIN genres_staging genres ON genres.genres_id = gm.genres_id
LEFT JOIN tags_staging tags ON tags.movies_id = movies.movies_id;
```

```fact_ratings``` zachytáva vzťahy medzi používateľmi, filmami, časom a dátumom, pričom meraným faktom sú hodnotenia. Faktové tabuľky nepoužívajú SCD.
``` sql
CREATE TABLE fact_ratings AS
SELECT 
    r.ratingId AS fact_ratingID,
    r.timestamp AS timestamp,
    r.rating,
    d.dim_dateID AS dateID,
    t.dim_timeID AS timeID,
    b.dim_bookId AS bookID,
    u.dim_userId AS userID
FROM ratings_staging r
JOIN dim_date d ON CAST(r.timestamp AS DATE) = d.date
JOIN dim_time t ON r.timestamp = t.timestamp
JOIN dim_books b ON r.ISBN = b.dim_bookId
JOIN dim_users u ON r.userId = u.dim_userId;
```
___

### 3.3 Load
Vo finálnej fáze sú staging tabuľky odstranené na optimalizovanie priestoru.
``` sql
DROP TABLE IF EXISTS age_group_staging;
DROP TABLE IF EXISTS genres_staging;
DROP TABLE IF EXISTS movies_staging;
DROP TABLE IF EXISTS genres_movies_staging;
DROP TABLE IF EXISTS occupations_staging;
DROP TABLE IF EXISTS users_staging;
DROP TABLE IF EXISTS ratings_staging;
DROP TABLE IF EXISTS tags_staging;
```
Pomocou ETL procesu sme v Snowflake sme upravili dát z ```.csv``` formátu do viacdimenzionálneho modelu typu hviezda. Výsledný model umožňuje analýzu správania používateľov a ich preferencie, vďaka ktorým môžeme robiť vizualizácie.

## 4. Vizualizácia dát
Dashboard zahŕňa __6__ vizualizácií, ktoré poskytujú základný prehľad o hlavných metrikách a trendoch spojených s filmami, používateľmi a hodnoteniami. Tieto vizualizácie odpovedajú na kľúčové otázky a pomáhajú lepšie pochopiť správanie a preferencie používateľov.

<p align="center">
  <img src="https://github.com/SomeUsername456/MovieLens-ETL/blob/main/movielens_dashboard.png" alt="Dashboard">
  <br />
  <i>Obrázok 3: Dashboard MovieLnes datasetu</i>
</p>

### Graf 1: Obľúbené žánre

Tento graf zobrazuje najobľúbenejšie filmové žánre na základe celkového počtu hodnotení. Keď vieme, ktoré žánre sú najobľúbenejšie, môžeme sa zamerať na produkciu alebo propagáciu väčšieho množstva obsahu v rámci týchto žánrov, aby sme uspokojili dopyt publika. Tento graf ukazuje že __Drama__ je najpoluárnejší žáner.
``` sql
SELECT 
    dm.name AS Genre, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.movies_id = dm.movies_id
GROUP BY dm.name
ORDER BY COUNT(fr.ratings_id) DESC;
```
### Graf 2: Hodnotenie počas dňa

Tento graf znázorňuje, ako sú hodnotenia rozdelené počas dňa. Poznanie aktívnych hodín umožňuje firmám plánovať propagačné akcie alebo špeciálne udalosti v čase, keď je najväčšia pravdepodobnosť interakcie používateľov s obsahom, čo vedie k vyššej miere interakcie. Tento graf ukazuje že užívatelia sú najaktívnejší počas __5__ hodiny.
``` sql
SELECT 
    dt.hour AS Hour, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_time dt ON fr.time_id = dt.time_id
GROUP BY dt.hour
ORDER BY dt.hour;
```
### Graf 3: Rozdelenie hodnotení podľa žánru a hodiny

Tento graf ukazuje, ako sú hodnotenia rôznych žánrov rozdelené v jednotlivých hodinách dňa. Tieto údaje môžeme použiť na poskytovanie odporúčaní špecifických pre daný žáner v čase, keď je s nimi najväčšia pravdepodobnosť interakcie používateľov.
``` sql
SELECT 
    dm.name AS Genre, 
    dt.hour AS Hour, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.movies_id = dm.movies_id
JOIN dim_time dt ON fr.time_id = dt.time_id
GROUP BY dm.name, dt.hour
ORDER BY Genre, Hour;
```
### Graf 4: Počet hodnotení v porovnaní s priemerným hodnotením

Tento graf zobrazuje vzťah medzi celkovým počtom hodnotení a priemerným hodnotením každého filmu. Analýzou filmov s veľkým počtom hodnotení a vysokým priemerným skóre môžeme identifikovať a propagovať filmy, ktoré majú širokú príťažlivosť a zároveň kvalitnú spätnú väzbu od používateľov.
``` sql
SELECT 
    dm.name AS Movie, 
    AVG(fr.rating) AS Average_Rating, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_movies dm ON fr.movies_id = dm.movies_id
GROUP BY dm.name
HAVING COUNT(fr.ratings_id) > 50
ORDER BY Total_Ratings DESC;
```
### Graf 5: Používatelia s najvyšším počtom hodnotení

Tento graf zobrazuje 10 používateľov, ktorí odoslali najviac hodnotení. Pochopenie toho, kto sú najaktívnejší používatelia, nám umožňuje vytvárať cielené kampane zamerané na odmeňovanie lojality alebo na povzbudenie pokračujúcej angažovanosti. Tento graf ukazuje že užívatel s __id 1680__ odoslal najviac hodnotení.
``` sql
SELECT 
    du.users_id AS User_ID, 
    COUNT(fr.ratings_id) AS Total_Ratings
FROM fact_ratings fr
JOIN dim_users du ON fr.users_id = du.users_id
GROUP BY du.users_id
ORDER BY Total_Ratings DESC
LIMIT 10;
```
### Graf 6: Počet hodnotení na pohlavie

Tento graf zobrazuje celkový počet hodnotení, rozdelených podľa pohlavia.  Ak sa 1 pohlavie viac zaoberá hodnotením obsahu, firmy môžu zvážiť personalizované obsahové stratégie. Tento graf ukazuje že __muži__ odoslali značne viac hodnotení.
``` sql
SELECT d.gender AS Pohlavie, COUNT(f.ratings_id) AS Pocet
FROM fact_ratings f
JOIN dim_users d ON d.users_id = f.users_id
GROUP BY Pohlavie;
```
Dashboard ponúka ucelený prehľad o dátach, odpovedá na kľúčové otázky o preferenciách a správaní používateľov. Vizualizácie uľahčujú porozumenie dát a môžu slúžiť na zlepšenie odporúčacích systémov, marketingových kampaní a poskytovaných filmových služieb.
___
__Autor:__ _Marek Repiský_

