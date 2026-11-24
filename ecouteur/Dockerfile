# Image du programme qui enregistre les modifications du cluster dans le serveur "historique".
FROM python:3.12-slim
RUN pip install --no-cache-dir pymongo
WORKDIR /app
COPY ecouteur.py .
# -u : affiche les messages tout de suite (utile pour "docker logs")
CMD ["python", "-u", "ecouteur.py"]
