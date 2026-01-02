# ckanext-dge-brokenlinks

`ckanext-dge-brokenlinks` es una extensión para CKAN utilizada en la plataforma datos.gob.es para comprobar si los enlaces de las distribuciones (recursos) son accesibles.

> [!TIP]
> Guía base y contexto del proyecto: https://github.com/datosgobes/datos.gob.es

## Descripción

- Añade un plugin CKAN para auditoría de enlaces.
- Incluye comandos *CLI* y tareas de *celery* para la ejecución de comprobaciones.

## Requisitos

### Compatibilidad
Compatibilidad con versiones de CKAN:

| CKAN version | Compatible?                                                                 |
|--------------|-----------------------------------------------------------------------------|
| 2.8          | ❌ No (>= Python 3)                                                          |
| 2.9          | ✅ Yes  |
| 2.10         | ❓ Unknown |
| 2.11         | ❓ Unknown |

### Dependencias
- Una instancia de CKAN.
- Dependencias Python adicionales:

```sh
pip install -r requirements.txt
```

## Instalación

```sh
pip install -e .
```

## Configuración

Activa el plugin en tu configuración de CKAN:

```ini
ckan.plugins = … dge_brokenlinks
```

### Plugins

- `dge_brokenlinks`

## Tests

```sh
pytest --ckan-ini=test.ini ckanext/dge_brokenlinks/tests
```

## Licencia

Este proyecto se distribuye bajo licencia **GNU Affero General Public License (AGPL) v3.0 o posterior**. Consulta el fichero [`LICENSE`](LICENSE).
