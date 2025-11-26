# Changelog

Toutes les modifications notables de ce projet seront documentées dans ce fichier.

Le format est basé sur [Keep a Changelog](https://keepachangelog.com/fr/1.0.0/),
et ce projet adhère au [Versionnement Sémantique](https://semver.org/lang/fr/).

## [Non publié]

### Ajouté
- Structure initiale du package
- Module `hdfs` : opérations sur HDFS (vérification de fichiers, listing, déplacement)
- Module `hive` : opérations sur Hive (vérification de tables, colonnes)
- Module `dataframe` : opérations sur DataFrame (chargement, repartition, écriture, renommage)
- Module `schema` : opérations sur les schémas (mapping de types, validation)
- Module `date` : utilitaires de date
- Module `validation` : validation de patterns et colonnes
- Tests unitaires avec couverture de code >= 80%
- Configuration pre-commit
- Documentation complète en français

## [0.1.0] - 2024-01-01

### Ajouté
- Version initiale du package `dp-spark-utils`
- Support de PySpark 3.3.2
- Support de Python 3.9 et 3.10
- Compatible avec Cloudera CDP 7.1.9
