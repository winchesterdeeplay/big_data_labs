FROM python:3.8-slim-buster
WORKDIR /app
EXPOSE 5000

COPY . .
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt

CMD ["python", "./setup_keyring.py"]
CMD ["python", "./data_generator_executor.py"]