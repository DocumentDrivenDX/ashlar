"""Checkout entrypoint for the installed publication-current query host."""
from ashlar_host.publication_current_cli import (
    execute_publication_queries, require_fresh_output, load_selected_weft, main,
)

if __name__ == '__main__':
    main()
