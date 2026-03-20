# immovision-import

## Etat reel du depot

Le projet reste un pipeline local simple autour de SQLite et d'une file `import_queue`.

Modules principaux :

- `import.py` : wrapper minimal pour `python import.py`
- `cli.py` : point d'entree applicatif
- `config.py` : configuration et environnement
- `pipeline.py` : orchestration du pipeline
- `analysis_logic.py` : scoring investissement, filtre strategie et score de confiance
- `storage.py` / `sqlite_storage.py` / `supabase_storage.py` : persistence
- `ingest_listings.py` : ingestion d'un feed JSON ou JSONL vers `import_queue`
- `sources/` : couche multi-sources

## Structure multi-sources

La base multi-sources est maintenant :

- `sources/base.py` : definition standard `SourceConnector`
- `sources/common.py` : helpers communs HTTP / parsing / JSONL
- `sources/registry.py` : registre des connecteurs disponibles
- `sources/source_cli.py` : CLI partagee pour les scripts de collecte simples
- `sources/immoweb_source.py` : connecteur HTML / HTTP Immoweb V1
- `sources/immoweb_browser_source.py` : connecteur navigateur Immoweb V5
- `sources/biddit_source.py` : connecteur Biddit HTML / HTTP de base
- `sources/biddit_browser_source.py` : connecteur navigateur Biddit V6 (DOM + API paginee)
- `sources/notaire_source.py` : connecteur Notaire.be V4 (fixture + JSON live + pagination HTML + detail)
- `sources/immovlan_source.py` : connecteur Immovlan V1 (listing HTML + pagination + detail JSON-LD)

Chaque connecteur produit des objets compatibles avec le format interne deja utilise par `ingest_listings.py` :

- `source_name`
- `source_listing_id`
- `source_url`
- `title`
- `description`
- `price`
- `postal_code`
- `commune`
- `property_type`
- `transaction_type`
- `existing_units`
- `surface`
- `copro_status` (`true` / `false` / `unknown`)
- `is_active`
- `notes`

## Chemin stable local

Installer les dependances du projet une fois, y compris `tzdata` pour Windows :

```powershell
python -m pip install -r requirements.txt
```

```powershell
.\.venv\Scripts\Activate.ps1
python ingest_listings.py
python import.py
python -m pytest
```

## Dashboard local

Le dashboard local lit directement la base SQLite reelle du moteur via un endpoint JSON local :

- `GET /api/dashboard` : payload dashboard complet
- `GET /` : interface HTML

Le flux est volontairement simple :

- `dashboard.py` lance un petit serveur HTTP local standard library
- `dashboard_data.py` lit `normalized_listings` et `listing_analysis`
- l'interface HTML appelle `/api/dashboard` puis affiche les annonces produites par `python import.py`

Origines de donnees distinguees :

- `live` : collecte live reelle via connecteur
- `fixture` : HTML local de fixture
- `seed` : seed local
- `test` : donnees artificielles de test
- `file_feed` : ingestion locale depuis un fichier JSON/JSONL
- `unknown` : origine ancienne ou non encore qualifiee

Regle de confiance retenue :

- `data_origin` est la source de verite pour distinguer le reel du non-reel dans le dashboard
- tout import local via `ingest_listings.py` sans `data_origin` explicite est force en `file_feed`
- un export JSONL produit par un connecteur live conserve son `data_origin` explicite (`live` ou `fixture`)

Le dashboard principal charge par defaut en vue investisseur stricte :

- seules les annonces `live` avec `source_url` fiable sont affichees
- les annonces `unknown`, `fixture`, `seed`, `test`, `file_feed` et les liens douteux sont exclus de cette vue principale
- les annonces notariales / encheres `Biddit` et `Notaire.be` sont exclues si la vente parait cloturee, terminee ou non exploitable
- les biens live evidemment hors fit investisseur sont exclus de la vue principale :
  - moins de `2` unites
  - `price_per_unit > 170000`
  - `house` ou `apartment` simples
  - type hors `commercial_house`, `apartment_block`, `commercial`, `mixed_use`
  - zone hors coeur/peripherie cible
  - `strategy_label = Hors criteres`
- le toggle peut ouvrir un mode debug separe pour revoir toutes les origines
- la barre d'etat affiche le nombre d'annonces exploitables visibles, le nombre exclu car non-live, le nombre exclu car lien invalide, le nombre exclu car vente inactive/cloturee et le nombre exclu car hors criteres

Le dashboard expose aussi une vue secondaire `A analyser serieusement` :

- uniquement des annonces `live`
- uniquement avec lien source valide
- uniquement si la vente est encore active
- jamais de `seed`, `fixture`, `test`, `file_feed`, `unknown`
- jamais de vente cloturee
- jamais de bien manifestement absurde
- reservee aux biens qui ratent 1 ou 2 points non totalement bloquants, par exemple une zone encore a analyser

Le panneau `A regarder d'abord` est plus strict que le reste du dashboard :

- uniquement des annonces `is_dashboard_eligible = true`
- uniquement des zones coeur/peripherie cible
- uniquement des types `commercial_house`, `apartment_block`, `commercial`, `mixed_use`
- minimum `2` unites
- `price_per_unit <= 170000`
- le classement interne privilegie d'abord le fit investisseur (type, zone, unites, copro, eligibilite stricte), puis seulement le score investissement et la confiance

Validation stricte des liens live :

- URL non vide
- URL differente de `#`, `/`, `about:blank` ou placeholder JavaScript
- URL bien formee en `http` ou `https`
- domaine coherent avec `source_name`
  - `Biddit` => `biddit.be`
  - `Immoweb` => `immoweb.be`
  - `Notaire.be` => `notaire.be` ou `immo.notaire.be`
  - `Immovlan` => `immovlan.be`
  - `Zimmo` => `zimmo.be`
  - `Immoscoop` => `immoscoop.be`

Consequence :

- `live only` exclut les annonces live au lien douteux
- en mode "toutes origines", elles restent visibles pour debug mais avec un badge de lien invalide et un lien desactive

Champs affiches par bien :

- source
- titre
- prix total
- unites
- prix par unite
- score investissement
- score confiance
- `strategy_label`
- `observation_status`
- lien source

Controles front disponibles :

- filtre par `source`
- filtre par `strategy_label`
- filtre par `observation_status`
- filtre par `zone_label`
- recherche texte sur titre, commune, CP, source et raison d'analyse
- tri par priorite investisseur, score investissement, score confiance, prix total, prix par unite ou fraicheur
- panneau `A regarder d'abord` avec les biens les plus prioritaires dans la vue courante

Vues disponibles :

- `Nouveau (< 7 jours)`
- `Compatible`
- `A analyser`
- `Hors criteres`
- `Modifies`

Lancement local :

```powershell
.\.venv\Scripts\Activate.ps1
python dashboard.py
```

URL locale par defaut :

```text
http://127.0.0.1:8765
```

Avec une base explicite ou un autre port :

```powershell
python dashboard.py --db .\immovision-local.db --port 8780
```

Point important :

- sans variable `SQLITE_PATH`, tous les scripts utilisent maintenant par defaut la meme base [`immovision-local.db`](/D:/OneDrive/Desktop/immovision-import-main/immovision-local.db) du repo, meme si la commande est lancee depuis un autre dossier
- cela evite qu'un `fetch_biddit.py`, un `import.py` et un `dashboard.py` lisent des bases differentes a cause du repertoire courant

Mode API live only explicite :

```text
http://127.0.0.1:8765/api/dashboard?live_only=1
```

Test cible du dashboard :

```powershell
python -m pytest test_dashboard_data.py
```

## Immoweb

Fixture stable :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --html-file .\sample_data\immoweb_search_fixture.html
python ingest_listings.py .\sample_data\immoweb_latest.jsonl --source-name Immoweb
python import.py
```

Le live browser Playwright reste disponible mais n'est plus le chemin prioritaire du projet.

## Biddit V6

Fixture stable :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_biddit.py --html-file .\sample_data\biddit_search_fixture.html
python ingest_listings.py .\sample_data\biddit_latest.jsonl --source-name Biddit
python import.py
```

Le connecteur live Biddit utilise maintenant Playwright par defaut pour charger la page comme un vrai navigateur, attendre le rendu utile, puis extraire les annonces depuis le DOM rendu ou depuis les reponses reseau JSON utiles.

Prerequis navigateur :

```powershell
.\.venv\Scripts\Activate.ps1
python -m playwright install chromium
```

Collecte browser live :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_biddit.py --search-url "https://www.biddit.be/fr/search"
```

Le mode navigateur Biddit essaie maintenant :

- la page rendue initiale
- le chargement additionnel par scroll / bouton "plus"
- les pages suivantes detectees via la pagination du DOM
- l'API JSON de recherche paginee (search-service/lot/_search) quand elle est exposee par la page
- l'API detail lot par lot (`/api/eco/biddit-bff/lot/{id}`) pour fiabiliser titre, description, type, surface, unites, statut de vente et copro quand l'information existe

Pour etendre la couverture :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_biddit.py --search-url "https://www.biddit.be/fr/search" --max-pages 4
```

La CLI affiche aussi un resume de couverture du type :

- annonces uniques collectees
- pages visitees
- pagination candidates detectees
- pages supplementaires suivies
- pages API suivies
- total detecte si le site l'expose
- couverture atteinte
- notes explicites si aucun chargement additionnel, aucune pagination DOM n'est detectee, ou si une page API dupliquee est ignoree

Diagnostic browser visible :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_biddit.py --search-url "https://www.biddit.be/fr/search" --headed --timeout 60000 --debug-save-html --debug-screenshot
```

Fallback HTTP diagnostic :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_biddit.py --search-url "https://www.biddit.be/fr/search" --http
```

## Notaire.be V4

Fixture stable :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_notaire.py --html-file .\sample_data\notaire_search_fixture.html
python ingest_listings.py .\sample_data\notaire_latest.jsonl --source-name Notaire.be
python import.py
```

Le connecteur Notaire.be utilise maintenant deux chemins compatibles :

- fallback fixture / HTML simple pour les tests reproductibles
- parsing prioritaire du JSON embarque `estates_json` sur la page live actuelle
- pagination HTML simple sur les pages suivantes (?page=2, ?page=3, ...)

Le parser live reconstruit chaque annonce avec ses propres champs, reutilise l'URL detail reelle trouvee dans le DOM, suit les pages suivantes tant que `--max-pages` le permet, puis enrichit chaque annonce via sa page detail `estate_json` pour fiabiliser titre, description, type, surface, unites, statut de vente et copro quand l'information existe, sans melange entre cartes.

Collecte HTTP live :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_notaire.py --search-url "https://immo.notaire.be/fr/biens-a-vendre" --max-pages 4
```

Injection live directe dans le pipeline :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_notaire.py --search-url "https://immo.notaire.be/fr/biens-a-vendre" --max-pages 4 --ingest
python import.py
```

## Immovlan V1

Fixture stable :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immovlan.py --html-file .\sample_data\immovlan_search_fixture.html
python ingest_listings.py .\sample_data\immovlan_latest.jsonl --source-name Immovlan
python import.py
```

Le connecteur Immovlan collecte maintenant les cartes listing live en HTTP, suit la pagination HTML simple, ignore explicitement les URLs `projectdetail` non assimilables a une annonce simple, puis enrichit chaque annonce `/detail/` via la page detail et son JSON-LD.

Collecte HTTP live :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immovlan.py --search-url "https://immovlan.be/fr/immobilier?transactiontypes=a-vendre" --max-pages 4
```

Injection live directe dans le pipeline :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immovlan.py --search-url "https://immovlan.be/fr/immobilier?transactiontypes=a-vendre" --max-pages 4 --ingest
python import.py
```
## Dedoublonnage et historique
Le pipeline garde maintenant une logique simple et stable par cle composite `(source_name, source_listing_id)` :

- `import_queue` dedoublonne les imports entrants par `(source_name, source_listing_id)`
- `normalized_listings` garde la derniere version connue du bien
- un meme `source_listing_id` peut maintenant exister sur plusieurs sources sans collision
- si une collecte live revient avec des champs vides ou `None`, le pipeline conserve les dernieres valeurs connues au lieu d'ecraser brutalement le bien
- chaque observation est historisee dans `listing_observation_history`
- l'historique de prix `listing_price_history` n'ajoute une ligne que si le prix change vraiment
- `modified` ne se declenche plus sur des champs volatils comme `description`, `source_url` ou du bruit de casse / ponctuation dans les titres

Statuts d'observation :

- `new` : premiere apparition du bien
- `seen` : bien deja connu sans changement utile detecte
- `modified` : bien deja connu avec au moins un champ modifie

Champs consideres comme changements metier significatifs :

- `title`
- `price`
- `postal_code`
- `commune`
- `property_type`
- `transaction_type`
- `existing_units`
- `surface`
- `copro_status`

Champs utiles pour lire l'etat courant :

- `normalized_listings.observation_count`
- `normalized_listings.last_observation_status`
- `normalized_listings.last_changed_fields`
- `normalized_listings.last_seen_at`
- `normalized_listings.source_name`
- `normalized_listings.source_listing_id`

Champs utiles pour lire l'historique :

- `listing_observation_history.observation_status`
- `listing_observation_history.changed_fields`
- `listing_observation_history.is_price_changed`
- `listing_observation_history.observed_at`
- `listing_price_history.price`
- `listing_price_history.observed_at`

Exemples SQLite :

```sql
SELECT source_name, source_listing_id, title, price, observation_count, last_observation_status, last_changed_fields
FROM normalized_listings
ORDER BY last_seen_at DESC;
```

```sql
SELECT listing_id, observation_status, changed_fields, is_price_changed, price, observed_at
FROM listing_observation_history
ORDER BY id DESC;
```

```sql
SELECT listing_id, price, observed_at
FROM listing_price_history
ORDER BY id DESC;
```

## Strategie investisseur

Le scoring investissement est maintenant aligne sur une strategie simple et explicite :

- prix par unite cible : `<= 170000 EUR`
- minimum : `2 unites`
- copropriete :
  - `true` => hors criteres
  - `false` => ok sur ce critere
  - `unknown` => a analyser, jamais `Compatible` plein
- zones cibles : Bruxelles et peripherie proche, notamment `Vilvoorde`, `Zellik`, `Grimbergen` et communes comparables
- travaux lourds : acceptes
- commerce : accepte
- types prioritaires : `commercial_house` et `apartment_block`
- ventes `Biddit` / `Notaire.be` : acceptees, mais avec prudence sur le prix final

Le score investissement reste sur `100`, avec des facteurs lisibles :

- `prix_unite` : `28`
- `unites` : `20`
- `copro` : `15`
- `type` : `15`
- `localisation` : `10`
- `rendement` : `8`
- `contexte_vente` : `4`

La classification strategie est stockee dans `listing_analysis.strategy_label` :

- `Compatible` : bien aligne avec les criteres principaux
- `A analyser` : bien potentiellement interessant, mais avec points de prudence
- `Hors criteres` : bien bloque par au moins un critere fort

Regles fortes actuelles :

- `existing_units < 2` => `Hors criteres`
- `price_per_unit > 170000` => `Hors criteres`
- `copro_status = true` => `Hors criteres`
- `copro_status = unknown` => au minimum `A analyser`
- source `Biddit` ou `Notaire.be` => au minimum `A analyser`

Champs utiles dans `listing_analysis` :

- `strategy_label`
- `strategy_compatible`
- `zone_label`
- `price_per_unit`
- `investment_score`
- `investment_score_label`
- `compatibility_reason`

Exemple SQLite :

```sql
SELECT listing_id, price_per_unit, investment_score, investment_score_label, strategy_label, zone_label, compatibility_reason
FROM listing_analysis
ORDER BY listing_id DESC;
```

## Injection directe dans le pipeline

Les scripts de collecte peuvent aussi injecter directement dans `import_queue` avec `--ingest`.

Exemple Biddit :

```powershell
python fetch_biddit.py --html-file .\sample_data\biddit_search_fixture.html --ingest
python import.py
```

Exemple Notaire.be :

```powershell
python fetch_notaire.py --html-file .\sample_data\notaire_search_fixture.html --ingest
python import.py
```

## Fixtures et sorties

Fixtures HTML reproductibles :

- `sample_data\immoweb_search_fixture.html`
- `sample_data\biddit_search_fixture.html`
- `sample_data\notaire_search_fixture.html`

Sorties JSONL par defaut :

- `sample_data\immoweb_latest.jsonl`
- `sample_data\biddit_latest.jsonl`
- `sample_data\notaire_latest.jsonl`

## Tests

Suite complete :

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest
```

Tests cibles :

```powershell
python -m pytest test_biddit_source.py
python -m pytest test_notaire_source.py
python -m pytest test_immovlan_source.py
python -m pytest test_sources_registry.py
```

## Limites actuelles

- Biddit live couvre maintenant plusieurs tranches via l'API JSON paginee detectee par le navigateur, mais pas encore l'ensemble des 39 pages par defaut
- Notaire.be suit maintenant plusieurs pages HTML live et parse en priorite le JSON embarque `estates_json`
- la couverture par defaut reste bornee par `--max-pages 4` sur Biddit et Notaire.be
- pas de scraping detail annonce par annonce
- pas de dedoublonnage inter-sources
- la cle technique d'unicite est maintenant `(source_name, source_listing_id)`
- il n'y a toujours pas de dedoublonnage inter-sources metier quand deux portails parlent du meme bien physique avec des identifiants differents
- le pipeline reste unifie apres normalisation, ce qui est volontaire pour garder la base simple et stable








