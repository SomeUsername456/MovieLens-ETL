# ETL pre MovieLens Dataset
Tento repozitár implementuje ETL proces v Snowflake, a následne analyzuje dáta z MovieLens datasetu.

// Projekt sa zameriava na preskúmanie správania používateľov a ich čitateľských preferencií na základe hodnotení kníh a demografických údajov používateľov. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik. //

## 1. Úvod a popis zdrojových dát
// Cieľom semestrálneho projektu je analyzovať dáta týkajúce sa kníh, používateľov a ich hodnotení. Táto analýza umožňuje identifikovať trendy v čitateľských preferenciách, najpopulárnejšie knihy a správanie používateľov.//

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

![Entitno-relačná schéma MovieLens](https://github.com/SomeUsername456/MovieLens-ETL/blob/main/MovieLens_ERD.png)
<p align="center"><i>Obrázok 1: Entitno-relačná schéma MovieLens</i><p>

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
## 2 Dimenzionálny model
Pre analýzu bol navrhnutý hviezdicový model (star schema). Centrálnu faktovú tabuľku predstavuje fact_ratings, tá je prepojená s ďalšími dimenziami:
  - ```dim_movies```: Obsahuje informácie o filmoch (názov, autor, žáner, tagy).
  - ```dim_users```: Obsahuje informácie o používateľoch (pohlavie, PSČ, vekov8 kategória, zamestnanie)
  - ```dim_date```: Zahrňuje informácie o dátumoch hodnotení (deň, mesiac, rok, štvrťrok).
  - ```dim_time```: Obsahuje podrobné časové údaje (hodina, AM/PM).

Diagram hviezdicového modelu ukazuje prepojenia medzi centrálnou faktovou tabuľkou a ďalšími dimenziami.

![Schéma hviezdy pre MovieLens](https://github.com/SomeUsername456/MovieLens-ETL/blob/main/movieLens_star_schema.png)
<p align="center"><i>Obrázok 2: Schéma hviezdy pre MovieLens</i></p>

___
## 3. ETL proces v Snowflake
ETL proces sa skladá z 3 častí: __extrahovanie__ (Extract), __transformácia__ (Transform) a __načítanie__ (Load). Tento proces bol realizovaný na platforme __Snowflake__ s cieľom pripraviť dáta na analýzu. 
### 3.1 Extract (Extrahovanie dát)

Dáta zo zdrojového datasetu (formát ```.csv```) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom ```my_stage```. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:
Príklad kódu:
``` sql
CREATE OR REPLACE STAGE my_stage;
```
Do stage boli následne nahraté súbory obsahujúce údaje o knihách, používateľoch, hodnoteniach, zamestnaniach a úrovniach vzdelania. Dáta boli importované do staging tabuliek pomocou príkazu ```COPY INTO```. Pre každú tabuľku sa použil podobný príkaz:
``` sql
COPY INTO occupations_staging
FROM @my_stage/occupations.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```
V prípade nekonzistentných záznamov bol použitý parameter ```ON_ERROR = 'CONTINUE'```, ktorý zabezpečil pokračovanie procesu bez prerušenia pri chybách.
