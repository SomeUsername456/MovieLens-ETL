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
- __tags__
- __ratings__
- __movies__
- __genres_movies__
- __genres__

Cielom ETL procesu bolo, aby tieto dáta pripravil, transformoval a sprístupnil pre ďalšiu analýzu.
___
## 1.1 Dátová architektúra
Entitno-relačný diagram (ERD)
Zdrojové dáta zobrazené na ER diagrame.
![ER diagram](https://example.com/obrazek.jpg)
_Obrázok 1: Entitno-relačná schéma MovieLens_
