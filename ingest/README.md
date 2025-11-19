# OpenAQ Ingest methods

## Using the check file

Downloading a specific fetchlog file
``` shell
AWS_PROFILE=openaq-user \
DOTENV=.env.aeolus \
poetry run python ./check.py --id=17213484 --download
```
