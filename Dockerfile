FROM python:3.8

WORKDIR /src

ENV PATH="/root/.poetry/bin:${PATH}"

ENV PYTHONBUFFERED=0 \
    SSL_CERT_FILE="/etc/ssl/certs/ca-certificates.crt" \
    REQUESTS_CA_BUNDLE="/etc/ssl/certs/ca-certificates.crt" \
    PATH="/root/.poetry/bin:${PATH}" \
    POETRY_VIRTUALENVS_CREATE=false

COPY CERN_Root_Certification_Authority_2.pem /usr/local/share/ca-certificates/CERN_Root_Certification_Authority_2.crt

RUN update-ca-certificates \
 && pip config set global.cert "${REQUESTS_CA_BUNDLE}"

RUN pip install poetry==1.1.6

COPY poetry.lock pyproject.toml ./
COPY arxiv_completness_check.py ./

RUN poetry install
CMD poetry run python arxiv_completness_check.py
