# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html


# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "OP Analytics"
copyright = "2024, OP Labs"
author = "OP Labs"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["myst_parser"]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_logo = "https://github.com/ethereum-optimism/brand-kit/blob/71ea3bb1ea24e87968804b388e99bed0b52e2a4b/assets/images/optimism-wordmark-large/OPTIMISM.png?raw=true"
html_theme = "pydata_sphinx_theme"
html_static_path = ["_static"]
html_css_files = [
    "css/custom.css",
]
