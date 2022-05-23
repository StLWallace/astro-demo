FROM quay.io/astronomer/astro-runtime:5.0.1

ENV GCP_PROJECT = "decisive-fold-221320"

COPY sa-astro-demo.json ./