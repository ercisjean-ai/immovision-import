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
- `parsing.py` : parsing HTML et extraction
- `normalization.py` : normalisation des payloads
- `analysis_logic.py` : calculs et logique d'analyse
- `pipeline.py` : orchestration du pipeline
- `seed_local.py` : seed SQLite local minimal
- `requirements.txt` : dependances Python
- `.github/workflows/import.yml` : workflow GitHub Actions
- `test_e2e_local.py` : test local de bout en bout ajoute
- `test_refactor_smoke.py` : tests de fumee sur les modules extraits

Ce qui n'est pas present dans le depot visible :

- aucun package Python
- aucun autre module metier
- aucune "phase 3/4" separee
- aucun module de comparables
- aucun module d'analyse d'opportunite avancee
- aucun modele SQLite externe
- aucun script d'installation Windows

Le depot reel reste un pipeline d'import simple, maintenant decoupe en modules sans changer les commandes de lancement.

## Role des modules

- `import.py` : wrapper minimal qui appelle `cli.main()`
- `cli.py` : lance le pipeline avec la configuration courante
- `config.py` : lit les variables d'environnement et choisit SQLite ou Supabase
- `storage.py` : choisit et expose le backend de stockage
- `storage_base.py` : definit l'interface attendue par le pipeline
- `sqlite_storage.py` : contient toute la persistance locale SQLite
- `supabase_storage.py` : contient toute la persistance Supabase
- `business.py` : conserve une API stable vers les fonctions metier existantes
- `parsing.py` : contient la decouverte Immoweb et l'extraction HTML
- `normalization.py` : prepare les payloads normalises avant persistence
- `analysis_logic.py` : contient les calculs et labels d'analyse
- `pipeline.py` : relie stockage, decouverte, import et analyse
- `seed_local.py` : insere un jeu minimal de donnees locales de test

## Mode local par defaut

Le script utilise SQLite par defaut.
Supabase n'est active que si `SUPABASE_URL` et `SUPABASE_KEY` sont definies.

Base SQLite par defaut :

```text
.\immovision-local.db
```

Pour utiliser un autre fichier :

```powershell
$env:SQLITE_PATH = ".\data\immovision.db"
```

## Probleme actuel de la venv casse

La venv presente dans le repo n'est pas fiable sur cette machine car son fichier `.\.venv\pyvenv.cfg` pointe vers un interpreteur qui n'existe plus :

```text
C:\Users\suryo\AppData\Local\Python\pythoncore-3.11-64\python.exe
```

Resultat :

- `.\.venv\Scripts\python.exe` ne demarre pas correctement
- `pytest` de cette venv ne peut pas etre lance proprement

Il faut recreer une venv saine localement.

## Procedure Windows exacte pour recreer une venv saine

Depuis `D:\OneDrive\Desktop\immovision-import-main` :

```powershell
Remove-Item -Recurse -Force .\.venv
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Si `py` n'est pas disponible mais `python` 3.11 l'est :

```powershell
Remove-Item -Recurse -Force .\.venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Verification simple :

```powershell
python --version
python -m pytest --version
```

## Commandes Windows exactes

### Lancer le seed local minimal

```powershell
.\.venv\Scripts\Activate.ps1
python seed_local.py
```

### Lancer le pipeline local complet

```powershell
.\.venv\Scripts\Activate.ps1
python import.py
```

### Lancer le test minimal de bout en bout

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest test_e2e_local.py
```

### Lancer tous les tests presents

```powershell
.\.venv\Scripts\Activate.ps1
python -m pytest
```

## Variables d'environnement utiles

Variables reellement utiles :

- aucune pour le mode local SQLite par defaut
- `SQLITE_PATH` : optionnelle, pour choisir le fichier SQLite
- `SUPABASE_URL` : optionnelle, seulement pour Supabase
- `SUPABASE_KEY` : optionnelle, seulement pour Supabase
- `SUPABASE_ANON_KEY` : alias encore accepte pour compatibilite

## Test minimal de bout en bout reproductible

1. Creer une venv saine.
2. Installer les dependances.
3. Lancer :

```powershell
python seed_local.py
python import.py
```

4. Le script doit :

- creer la base SQLite si elle n'existe pas
- inserer ou mettre a jour un listing de test dans `import_queue`
- importer ce listing dans `normalized_listings`
- generer `listing_analysis`
- generer `listing_price_history`
- mettre a jour `sources`

5. Validation automatisable :

```powershell
python -m pytest test_e2e_local.py
```
