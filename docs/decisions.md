## PC Store Uruguay — discarded from MVP

PC Store uses Cloudflare + dynamic CSRF tokens per session.
Reliable scraping would require complex session handling and is
potentially fragile to Cloudflare changes.

Decision: exclude from MVP. Initial dataset covers:

- MercadoLibre (API)
- Thot Computación (static WooCommerce)
- Banifox (internal JSON endpoint)

It can be reviewed in future iterations with Playwright + session handling.
