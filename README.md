# Extraction Project

Ce package permet d’extraire des données et d’appliquer des traitements sur des fichiers xml de declarations fiscal 2058CG.

## Installation

Pour installer le package, vous pouvez utiliser pip :

```sh
pip install -e .[dev]
```
## exemple d'utilisation

### un exemple simple

Un exemple de la façon dont on peut utiliser le projet


```sh
extraction-cli --help
```

### un exemple plus pousser

Pour appliquer le programme un repertoire de fichier xml

```sh
extraction-cli --input-dir /path/to/xml/files --output-dir /path/to/output
```

## Tests

Pour executer les tests on peut utiliser pytest

```sh
pytest
```

## Auteurs

- **Mouhamed Fadel Samb** - *Développeur principal* - [fadwa.samb@gmail.com](mailto:fadwa.samb@gmail.com)
