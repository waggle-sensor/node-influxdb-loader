FROM waggle/plugin-base:1.1.1-base

COPY . /app/
RUN pip3 install --no-cache-dir -r /app/requirements.txt

ENTRYPOINT [ "python3", "/app/loader.py" ]