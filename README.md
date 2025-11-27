# dp-spark-utils

Package Python utilitaire pour les opérations PySpark sur Cloudera CDP 7.1.9 avec intégration Hive/HDFS.

## 📋 Description

`dp-spark-utils` est un package Python conçu pour standardiser et simplifier les opérations courantes avec PySpark dans un environnement Cloudera CDP 7.1.9. Il fournit un ensemble de fonctions utilitaires pour :

- Opérations sur HDFS (vérification de fichiers, listing, déplacement)
- Opérations sur Hive (vérification de tables, récupération de colonnes)
- Manipulation de DataFrames (chargement, repartition, écriture, renommage de colonnes)
- Gestion des schémas et types de données
- Utilitaires de dates
- Validation de données et de fichiers

## 🔧 Prérequis

- Python 3.9 ou 3.10
- PySpark 3.3.2
- Cloudera CDP 7.1.9

## 📦 Installation

### Installation depuis le dépôt Git

```bash
# Cloner le dépôt
git clone https://gitlab.internal/data-platform/dp-spark-utils.git
cd dp-spark-utils

# Installation en mode développement
pip install -e .

# Ou installation classique
pip install .
```

### Installation avec les dépendances de développement

```bash
pip install -e ".[dev]"
```

### Installation depuis requirements.txt

```bash
pip install -r requirements.txt
```

## 🚀 Utilisation

### Import du package

```python
# Import de toutes les fonctions
from dp_spark_utils import (
    get_hadoop_fs,
    check_file_exists,
    check_table_exists,
    load_dataframe,
    map_spark_type,
)

# Ou import par module
from dp_spark_utils.hdfs import check_file_exists, hdfs_list_files
from dp_spark_utils.hive import check_table_exists, get_columns_map
from dp_spark_utils.dataframe import load_dataframe, write_dataframe_csv
from dp_spark_utils.schema import map_spark_type
from dp_spark_utils.date import get_last_day_of_previous_month
from dp_spark_utils.validation import validate_filename_pattern
```

### Exemples d'utilisation

#### Opérations HDFS

```python
from pyspark.sql import SparkSession
from dp_spark_utils.hdfs import check_file_exists, hdfs_list_files, move_files

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Vérifier si un fichier existe
if check_file_exists(spark, "/data/input/file.json", extension=".json"):
    print("Le fichier JSON existe")

# Lister les fichiers CSV d'un répertoire
csv_files = hdfs_list_files(spark, "/data/input/", extension=".csv")
print(f"Fichiers CSV trouvés: {csv_files}")

# Déplacer des fichiers CSV
moved = move_files(spark, "/data/temp/", "/data/output/", extension=".csv")
print(f"Fichiers déplacés: {moved}")
```

#### Opérations Hive

```python
from dp_spark_utils.hive import check_table_exists, get_columns_map

# Vérifier si une table existe
if check_table_exists(spark, "ma_base", "ma_table"):
    print("La table existe")

# Obtenir les colonnes d'une table
columns, columns_map = get_columns_map(spark, "ma_base", "ma_table")
print(f"Colonnes: {columns}")
print(f"Map des colonnes (insensible à la casse): {columns_map}")
```

#### Opérations DataFrame

```python
from dp_spark_utils.dataframe import (
    load_dataframe,
    repartition_dataframe,
    write_dataframe_csv,
    rename_columns,
)

# Charger une table Hive
df = load_dataframe(spark, "ma_base", "ma_table", columns=["id", "nom", "email"])

# Repartitionner selon le nombre de lignes par fichier
df, total_rows, partitions = repartition_dataframe(df, lines_per_file=100000)

# Écrire en CSV
write_dataframe_csv(df, "/data/output/export", separator=";", encoding="ISO-8859-1")

# Renommer des colonnes
mapping = [
    {"source": "id", "destination": "user_id"},
    {"source": "nom", "destination": "user_name"},
]
df = rename_columns(df, mapping)
```

#### Opérations sur les types

```python
from dp_spark_utils.schema import map_spark_type, get_ordered_columns_from_schema

# Mapper un type string vers un type Spark
spark_type = map_spark_type("bigint")  # Retourne LongType()

# Obtenir l'ordre des colonnes depuis un schéma
schema = [
    {"source": "id", "destination": "user_id"},
    {"source": "name", "destination": "user_name"},
]
ordered_cols = get_ordered_columns_from_schema(schema)  # ['user_id', 'user_name']
```

#### Utilitaires de dates

```python
from dp_spark_utils.date import (
    get_last_day_of_previous_month,
    get_last_day_of_previous_two_months,
)

# Dernier jour du mois précédent
last_day = get_last_day_of_previous_month()  # Format: "20240131"

# Avec format personnalisé
last_day = get_last_day_of_previous_month(date_format="%Y-%m-%d")  # "2024-01-31"

# Dernier jour de deux mois avant
two_months_ago = get_last_day_of_previous_two_months()
```

#### Validation

```python
from dp_spark_utils.validation import validate_columns_match, validate_filename_pattern

# Valider que les colonnes correspondent
source_cols = ["id", "name", "email"]
target_cols = ["ID", "Name", "Email"]
is_match, extra, missing = validate_columns_match(source_cols, target_cols)

# Valider le pattern d'un nom de fichier
is_valid = validate_filename_pattern(
    "20240131_export.csv",
    r"^\d{8}_export\.csv$"
)
```

## 📁 Structure du projet

```
dp-spark-utils/
├── dp_spark_utils/
│   ├── __init__.py           # Point d'entrée du package
│   ├── hdfs/
│   │   ├── __init__.py
│   │   └── operations.py     # Opérations HDFS
│   ├── hive/
│   │   ├── __init__.py
│   │   └── operations.py     # Opérations Hive
│   ├── dataframe/
│   │   ├── __init__.py
│   │   └── operations.py     # Opérations DataFrame
│   ├── schema/
│   │   ├── __init__.py
│   │   └── operations.py     # Opérations sur les schémas
│   ├── date/
│   │   ├── __init__.py
│   │   └── operations.py     # Utilitaires de dates
│   └── validation/
│       ├── __init__.py
│       └── operations.py     # Fonctions de validation
├── tests/
│   ├── __init__.py
│   ├── conftest.py           # Configuration pytest et fixtures
│   ├── test_hdfs.py
│   ├── test_hive.py
│   ├── test_dataframe.py
│   ├── test_schema.py
│   ├── test_date.py
│   ├── test_validation.py
│   └── test_init.py
├── .gitignore
├── .pre-commit-config.yaml
├── CHANGELOG.md
├── pyproject.toml
├── README.md
├── requirements.txt
└── requirements-dev.txt
```

## 🧪 Tests

### Exécuter tous les tests

```bash
# Avec couverture de code
pytest

# Sans couverture
pytest --no-cov

# Tests verbeux
pytest -v

# Un fichier de test spécifique
pytest tests/test_hdfs.py
```

### Couverture de code

Le projet est configuré pour maintenir une couverture de code d'au moins 80%.

```bash
# Générer le rapport de couverture HTML
pytest --cov=dp_spark_utils --cov-report=html

# Voir le rapport dans le terminal
pytest --cov=dp_spark_utils --cov-report=term-missing
```

## 🔨 Build du package

### Build manuel

```bash
# Installer les outils de build
pip install build twine

# Créer les distributions
python -m build

# Les fichiers seront créés dans dist/
# - dp_spark_utils-0.1.0-py3-none-any.whl
# - dp_spark_utils-0.1.0.tar.gz
```

### Vérification du package

```bash
# Vérifier le package avec twine
twine check dist/*
```

## 🔄 Pre-commit hooks

Le projet utilise pre-commit pour assurer la qualité du code.

### Installation

```bash
# Installer pre-commit
pip install pre-commit

# Installer les hooks
pre-commit install
```

### Utilisation

```bash
# Exécuter les hooks manuellement sur tous les fichiers
pre-commit run --all-files

# Exécuter un hook spécifique
pre-commit run black --all-files
pre-commit run flake8 --all-files
```

### Hooks configurés

- **trailing-whitespace** : Supprime les espaces en fin de ligne
- **end-of-file-fixer** : Assure une ligne vide en fin de fichier
- **check-yaml** : Valide la syntaxe YAML
- **check-toml** : Valide la syntaxe TOML
- **black** : Formatage du code Python
- **isort** : Tri des imports
- **flake8** : Linting du code
- **mypy** : Vérification des types

## 🤝 Contribution

### Comment contribuer

1. **Cloner le dépôt**
   ```bash
   git clone https://gitlab.internal/data-platform/dp-spark-utils.git
   cd dp-spark-utils
   ```

2. **Créer une branche**
   ```bash
   git checkout -b feature/ma-nouvelle-fonctionnalite
   ```

3. **Installer les dépendances de développement**
   ```bash
   pip install -e ".[dev]"
   pre-commit install
   ```

4. **Faire vos modifications**
   - Écrire le code
   - Écrire les tests unitaires
   - S'assurer que les tests passent
   - S'assurer que la couverture est >= 80%

5. **Valider avec pre-commit**
   ```bash
   pre-commit run --all-files
   ```

6. **Commit et push**
   ```bash
   git add .
   git commit -m "feat: description de la fonctionnalité"
   git push origin feature/ma-nouvelle-fonctionnalite
   ```

7. **Créer une Merge Request**

### Ajouter une nouvelle fonction

1. **Identifier le module approprié**
   - `hdfs/` : Opérations sur HDFS
   - `hive/` : Opérations sur Hive
   - `dataframe/` : Manipulations de DataFrame
   - `schema/` : Gestion des types et schémas
   - `date/` : Utilitaires de dates
   - `validation/` : Fonctions de validation

2. **Ajouter la fonction dans le fichier `operations.py` du module**
   ```python
   def ma_nouvelle_fonction(param1: str, param2: int) -> bool:
       """
       Description courte de la fonction.

       Description détaillée si nécessaire.

       Args:
           param1 (str): Description du premier paramètre.
           param2 (int): Description du second paramètre.

       Returns:
           bool: Description de ce qui est retourné.

       Example:
           >>> ma_nouvelle_fonction("test", 42)
           True
       """
       # Implementation
       return True
   ```

3. **Exporter la fonction dans `__init__.py` du module**
   ```python
   from dp_spark_utils.module.operations import ma_nouvelle_fonction

   __all__ = [
       "ma_nouvelle_fonction",
       # ... autres fonctions
   ]
   ```

4. **Exporter au niveau du package** (optionnel, pour les fonctions principales)
   Dans `dp_spark_utils/__init__.py`:
   ```python
   from dp_spark_utils.module import ma_nouvelle_fonction

   __all__ = [
       "ma_nouvelle_fonction",
       # ... autres fonctions
   ]
   ```

5. **Écrire les tests unitaires**
   Dans `tests/test_module.py`:
   ```python
   class TestMaNouvelleFonction:
       """Tests for ma_nouvelle_fonction."""

       def test_cas_nominal(self):
           """Test the normal use case."""
           result = ma_nouvelle_fonction("test", 42)
           assert result is True

       def test_cas_erreur(self):
           """Test error handling."""
           # ...
   ```

6. **Mettre à jour le CHANGELOG.md**

### Conventions de code

- **Docstrings** : En anglais, format Google/Numpy
- **Nommage** : snake_case pour les fonctions et variables
- **Types** : Utiliser les annotations de type Python
- **Tests** : Un fichier de test par module

## 📝 Logging

### Comportement du logging

Ce package utilise le module standard `logging` de Python avec la convention `logging.getLogger(__name__)` pour chaque module. **Cela signifie que le package ne configure aucun handler, formatter ou niveau de log par défaut**.

Cette approche garantit que :
- **Pas d'impact sur votre système de logging existant** : Le package n'interfère pas avec votre configuration de logging personnalisée
- **Contrôle total** : Vous gardez le contrôle complet sur la façon dont les logs sont formatés et où ils sont envoyés
- **Intégration facile** : Le package s'intègre naturellement avec n'importe quel système de logging que vous utilisez

### Intégration avec un système de logging personnalisé

Si vous utilisez un système de logging personnalisé (par exemple une classe `Monitoring`), vous pouvez facilement intégrer les logs de `dp-spark-utils` :

```python
import logging

# Exemple de classe Monitoring personnalisée (à remplacer par votre propre implémentation)
class Monitoring:
    """Votre système de monitoring avec trame de logs spécifique."""

    def info(self, message):
        # Votre logique de logging info avec format personnalisé
        pass

    def warning(self, message):
        # Votre logique de logging warning avec format personnalisé
        pass

    def error(self, message):
        # Votre logique de logging error avec format personnalisé
        pass


# Instancier votre système de logging personnalisé
monitoring = Monitoring()


# Créer un handler personnalisé pour rediriger vers votre système
class MonitoringHandler(logging.Handler):
    """
    Handler personnalisé qui redirige vers votre système de monitoring.

    Args:
        monitoring_instance: Instance avec méthodes info(), warning(), error()
    """

    def __init__(self, monitoring_instance):
        super().__init__()
        self.monitoring = monitoring_instance

    def emit(self, record):
        try:
            log_message = self.format(record)
            if record.levelno >= logging.ERROR:
                self.monitoring.error(log_message)
            elif record.levelno >= logging.WARNING:
                self.monitoring.warning(log_message)
            else:
                self.monitoring.info(log_message)
        except Exception:
            self.handleError(record)


# Ajouter le handler aux loggers de dp-spark-utils
dp_logger = logging.getLogger('dp_spark_utils')
dp_logger.addHandler(MonitoringHandler(monitoring))
dp_logger.setLevel(logging.INFO)
```

### Contrôler le niveau de log

```python
import logging

# Activer les logs DEBUG pour tout le package
logging.getLogger('dp_spark_utils').setLevel(logging.DEBUG)

# Ou uniquement pour un module spécifique
logging.getLogger('dp_spark_utils.hdfs').setLevel(logging.DEBUG)

# Désactiver les logs du package
logging.getLogger('dp_spark_utils').setLevel(logging.CRITICAL)
```

### Exemple avec une configuration de logging standard

```python
import logging

# Configuration de base avec format personnalisé
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Maintenant les logs de dp-spark-utils utiliseront cette configuration
from dp_spark_utils import check_file_exists
```

## 📄 Licence

MIT License

## 👥 Auteurs

Data Platform Team
