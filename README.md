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
- `sources\immoweb_browser_source.py` : connecteur Immoweb live V2 via Playwright
- `fetch_immoweb.py` : commande de collecte Immoweb vers JSONL ou `import_queue`
- `sample_data\sample_listings.jsonl` : exemple de feed local
- `sample_data\immoweb_search_fixture.html` : fixture HTML reproductible pour le connecteur Immoweb
- `requirements.txt` : dependances Python
- `.github/workflows/import.yml` : workflow GitHub Actions
- `test_e2e_local.py` : test local de bout en bout ajoute
- `test_refactor_smoke.py` : tests de fumee sur les modules extraits
- `test_feed_ingestion.py` : tests du flux d'ingestion fichier et de l'historique
- `test_immoweb_source.py` : tests du connecteur Immoweb HTTP / fixture
- `test_immoweb_browser_source.py` : tests de la V2 Playwright sans reseau reel

## Connecteur Immoweb

### V1 HTTP / fixture

- `sources\immoweb_source.py`
- utilise `requests` pour le HTTP simple
- reste utile pour la fixture locale et le parsing HTML brut

### V2 Browser Playwright

- `sources\immoweb_browser_source.py`
- ouvre la page de recherche Immoweb dans Chromium via Playwright
- attend le chargement utile
- tente d'extraire les annonces depuis :
  - le HTML rendu
  - des scripts JSON embarques si presents
- convertit les resultats vers le format interne actuel
- echoue explicitement si Playwright n'est pas installe, si Chromium n'est pas installe, ou si le HTML rendu reste inexploitable

La fixture locale reste le chemin stable de test. Le live n'est jamais annonce comme succes si aucune annonce n'est extraite.

## Historique metier local

Le pipeline conserve deux traces temporelles distinctes :

- `listing_observation_history` : une observation a chaque import d'annonce, meme si le prix ne change pas
- `listing_price_history` : uniquement le premier prix observe puis les vrais changements de prix

## Installation Windows pour la V2 Playwright

Dans la venv active :

```powershell
python -m pip install -r requirements.txt
python -m playwright install chromium
```

## Commandes Windows exactes

### Fixture locale stable

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --html-file .\sample_data\immoweb_search_fixture.html
python ingest_listings.py .\sample_data\immoweb_latest.jsonl --source-name Immoweb
python import.py
```

### Collecte live Immoweb via Playwright

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE"
```

Mode visible pour diagnostic :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE" --headed
```

Ancien mode HTTP forcé pour comparaison :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE" --http
```

Injection directe apres collecte :

```powershell
.\.venv\Scripts\Activate.ps1
python fetch_immoweb.py --search-url "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/bruxelles/province?countries=BE" --ingest
python import.py
```

### Tests

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest
```

Test cible sur la V2 navigateur :

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest test_immoweb_browser_source.py
```

## Limites actuelles

- pas de pagination automatique
- pas de scraping detail annonce par annonce
- pas de garanties si Immoweb durcit encore son anti-bot navigateur
- l'extraction JSON embarquee est opportuniste, pas specialisee par schema Immoweb complet
- le chemin le plus stable pour valider le projet reste toujours la fixture `sample_data\immoweb_search_fixture.html`
