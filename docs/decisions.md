## PC Store Uruguay — discarded from MVP

PC Store uses Cloudflare + dynamic CSRF tokens per session.
Reliable scraping would require complex session handling and is
potentially fragile to Cloudflare changes.

Decision: exclude from MVP. Initial dataset covers:

- Thot Computación (static WooCommerce)
- Banifox (internal JSON endpoint)

It can be reviewed in future iterations with Playwright + session handling.

## PCcompu — included in MVP

PCcompu uses server-rendered HTML with simple pagination via ?pagina=N query parameter.
No Cloudflare or complex session handling required.

Decision: include in MVP. Dataset now covers Thot + Banifox + PCcompu.

## MercadoLibre — excluded from MVP

The MercadoLibre API returns 403 (PolicyAgent) for all endpoints from
Uruguay, regardless of authentication. Web scraping with Playwright is
also blocked: ML detects automated sessions and redirects to login after
the first page navigation.

Decision: exclude ML from MVP. Dataset covers Thot + Banifox + PCcompu.
Future option: playwright-stealth, proxy rotation, or authenticated session.
