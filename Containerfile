FROM python:3.13-alpine AS base

ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

FROM base as deps

COPY Pipfile .
COPY Pipfile.lock .
RUN apk --no-cache --update-cache add build-base python3-dev \
	&& pip install --upgrade pip \
	&& pip install pipenv

RUN PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy

FROM base AS final
COPY --from=deps /.venv /.venv

WORKDIR /app
COPY . .

ENTRYPOINT ["/.venv/bin/python"]
