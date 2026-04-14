FROM python:3.9

RUN useradd -m -u 1000 user

WORKDIR /app

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=user . .

EXPOSE 7860

USER user

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]
