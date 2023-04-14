# ponyexpress-telegram

A Telegram-scraper for ponyexpress.

## Usage with ponyexpress

The default configuration for this connector is:

```yaml
    timeout: 10,
    max_retries: 3,
    retry_delay: 5,
    wait_time: 4,
    user_agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36
```

## Standalone Application

Additionally, this package comes with a standalone application that can be used to scrape public Telegram channels.

```console
.venv ❯ telegram --help
Usage: telegram [OPTIONS] [NAMES]...

  Scrape Telegram Channels.

Options:
  --version                       Show the version and exit.
  -m, --messages-output FILENAME
  -u, --users-output FILENAME
  -p, --prepare-edges
  -l, --log-file PATH
  -v, --verbose
  --help                          Show this message and exit.
```

## Usage

1. Install `poetry` if you don't have it: `pipx install poetry`.
2. Clone this repo, go into the repo's folder.
3. Install the dependencies with `poetry install` and spawn a shell in your new virtual environment with `poetry shell`.
3. To run tests type `pytest`, to try ponyexpress-telegram run `ponyexpress-telegram --help`.

---

[Philipp Kessling](mailto:p.kessling@leibniz-hbi.de) under MIT.
