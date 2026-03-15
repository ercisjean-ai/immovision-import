# immovision-import

## Etat reel du depot

Fichiers presents dans le projet applicatif :

- `import.py` : point d'entree minimal pour `python import.py`
- `cli.py` : point d'entree applicatif
- `config.py` : configuration et environnement
- `storage.py` : facade de selection du backend
- `storage_base.py` : interface commune de stockage
- `sqlite_storage.py` : backend SQLite local
- `supabase_storage.py` : backend Supabase
- `business.py` : facade de compatibilite metier
- `parsing.py` : parsing HTML et extraction historique
- `normalization.py` : normalisation des payloads
- `analysis_logic.py` : calculs et logique d'analyse
- `pipeline.py` : orchestration du pipeline
- `seed_local.py` : seed SQLite local minimal
- `listing_feed.py` : chargement d'un feed JSON/JSONL d'annonces
- `ingest_listings.py` : ingestion d'un feed local vers `import_queue`
- `sources\__init__.py` : facade des connecteurs de sources
- `sources\immoweb_source.py` : connecteur Immoweb HTML / HTTP V1
- `sources\immoweb_browser_source.py` : connecteur Immoweb live V3 via Playwright
- `fetch_immoweb.py` : commande de collecte Immoweb vers JSONL ou `import_queue`
- `sample_data\sample_listings.jsonl` : exemple de feed local
- `sample_data\immoweb_search_fixture.html` : fixture HTML reproductible pour le connecteur Immoweb
- `requirements.txt` : dependances Python
- `.github/workflows/import.yml` : workflow GitHub Actions
- `test_e2e_local.py` : test local de bout en bout ajoute
- `test_refactor_smoke.py` : tests de fumee sur les modules extraits
- `test_feed_ingestion.py` : tests du flux d'ingestion fichier et de l'historique
- `test_immoweb_source.py` : tests du connecteur Immoweb HTTP / fixture
- `test_immoweb_browser_source.py` : tests de la V3 navigateur sans reseau reel

## Connecteur Immoweb

### Fixture stable

Le chemin stable reste :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --html-file .\sample_data\immoweb_search_fixture.html
python ingest_listings.py .\sample_data\immoweb_latest.jsonl --source-name Immoweb
python import.py
```

### Live browser V3

Le mode live Playwright utilise maintenant une navigation plus robuste :

- navigation initiale en `commit`
- plus d'attente naive sur un `load` complet
- tentative de clic sur banniere cookie / consentement
- stabilisation progressive avec attente courte, `networkidle` tolere, puis scroll
- tentative d'extraction depuis :
  - le DOM rendu
  - le JSON embarque
  - les reponses reseau JSON capturees si elles semblent utiles
- messages d'erreur plus precis
- artefacts de debug en cas d'echec si demandes

## Installation Windows pour le live browser

Dans la venv active :

```powershell
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Diagnostic live exact

### Collecte live simple

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE"
```

### Diagnostic live complet

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE" --headed --timeout 60000 --debug-save-html --debug-screenshot
```

### Ancien mode HTTP pour comparaison

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE" --http
```

### Injection directe apres collecte live

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE" --ingest
python import.py
```

## Artefacts de debug

Si `--debug-save-html` ou `--debug-screenshot` est active, les artefacts d'echec sont enregistres par defaut dans :

```text
.\debug\immoweb\
```

Noms generes :

- `immoweb_failure_YYYYMMDD_HHMMSS.html`
- `immoweb_failure_YYYYMMDD_HHMMSS.png`

Tu peux changer le dossier :

```powershell
python fetch_immoweb.py --search-url "..." --debug-save-html --debug-screenshot --debug-dir .\debug\immoweb-run-1
```

## Historique metier local

Le pipeline conserve deux traces temporelles distinctes :

- `listing_observation_history` : une observation a chaque import d'annonce, meme si le prix ne change pas
- `listing_price_history` : uniquement le premier prix observe puis les vrais changements de prix

## Tests

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest
```

Test cible sur le connecteur navigateur :

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest test_immoweb_browser_source.py
```

## Limites actuelles

- pas de pagination automatique
- pas de scraping detail annonce par annonce
- Immoweb peut encore renforcer son anti-bot meme cote navigateur
- l'extraction reseau JSON reste opportuniste et depend du trafic reel de la page
- le chemin le plus stable pour valider le projet reste toujours la fixture `sample_data\immoweb_search_fixture.html`
