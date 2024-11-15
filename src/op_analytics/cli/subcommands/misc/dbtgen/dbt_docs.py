import json

from op_analytics.coreutils.logger import structlog
from op_analytics.coreutils.path import repo_path

log = structlog.get_logger()


def customize():
    """Customize html dbt docs.

    Splice in the optimism.css stylesheet on the generated dbt docs index.html
    """

    html_path = repo_path("dbt/target/index.html")
    manifest_path = repo_path("dbt/target/manifest.json")
    catalog_path = repo_path("dbt/target/catalog.json")
    stylesheet_path = repo_path("docs/dbt/optimism.css")
    html_new_path = repo_path("docs/dbt/index.html")

    assert html_path is not None
    assert manifest_path is not None
    assert catalog_path is not None
    assert stylesheet_path is not None
    assert html_new_path is not None

    with open(html_path, "r") as f:
        html_content = f.read()

    with open(stylesheet_path, "r") as f:
        stylesheet_content = f.read()

    with open(manifest_path, "r") as f:
        json_manifest = json.loads(f.read())

    with open(catalog_path, "r") as f:
        json_catalog = json.loads(f.read())

    with open(html_new_path, "w") as f:
        search_str = 'n=[o("manifest","manifest.json"+t),o("catalog","catalog.json"+t)]'

        new_str = (
            "n=[{label: 'manifest', data: "
            + json.dumps(json_manifest)
            + "},{label: 'catalog', data: "
            + json.dumps(json_catalog)
            + "}]"
        )

        new_content = html_content.replace(search_str, new_str).replace(
            "<head>", f"<head><style>{stylesheet_content}</style>"
        )
        f.write(new_content)

    log.info(f"Set optimism.css stylesheet at {html_new_path}")
