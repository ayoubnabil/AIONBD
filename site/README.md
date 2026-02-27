# AIONBD static website (GitHub Pages)

This folder contains the static website published with GitHub Pages.

## Structure

- `index.html`: main landing page optimized for "Vector AI" and "Vector AI Search".
- `docs/index.html`: documentation hub page.
- `blog/index.html`: blog hub page.
- `blog/use-case-vector-ai.html`: use-case article page.
- `blog/vector-ai-search-edge.html`: search strategy article page.
- `styles.css`: shared visual style.
- `robots.txt`: crawler directives.
- `sitemap.xml`: XML sitemap for all website pages.
- `site.webmanifest`: web app manifest.

## Local preview

```bash
python3 -m http.server 8000 --directory site
```

Open `http://127.0.0.1:8000`.

## GitHub Pages deploy

The workflow `.github/workflows/pages.yml` deploys `site/`.

Recommended GitHub setting:

1. `Settings` -> `Pages`
2. `Build and deployment` -> `Source: GitHub Actions`

## SEO notes

- SEO-critical tags are static in HTML (no JavaScript dependency for canonical or JSON-LD).
- Canonical URLs are set to `https://ayoubnabil.github.io/AIONBD/...`.
- If you move to a custom domain, update canonical/OG URLs and `sitemap.xml`.
