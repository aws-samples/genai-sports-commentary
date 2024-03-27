FROM python:3.10
RUN pip install gradio==3.39.0 boto3 pandas
COPY app.py /workdir/app.py
COPY live-sports-data-simulator.py /workdir/live-sports-data-simulator.py
COPY ./data /workdir/data/
WORKDIR /workdir
EXPOSE 7860
ENTRYPOINT [ "python", "app.py" ]
