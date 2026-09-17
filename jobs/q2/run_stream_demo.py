import argparse
import os
import shutil
import socket
import subprocess
import threading
from pathlib import Path


def run_demo(seconds):
    events = ["INFO service started", "ERROR database timeout", "INFO retrying", "ERROR upstream unavailable", "ERROR request failed"]
    executable = shutil.which("spark-submit")
    if executable is None:
        raise RuntimeError("spark-submit is missing")
    stop = threading.Event()
    errors = []
    sent = []
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Bind before launching Spark. An occupied port fails without affecting its owner.
        server.bind(("127.0.0.1", 9999))
        server.listen(1)
        server.settimeout(90)

        def send_events():
            try:
                connection, _ = server.accept()
                with connection:
                    for event in events:
                        if stop.wait(1):
                            break
                        connection.sendall((event + "\n").encode("utf-8"))
                        sent.append(event)
                        print("Q2 SOCKET SENT: " + event, flush=True)
                    stop.wait(seconds + 15)
            except Exception as error:
                errors.append(error)

        sender = threading.Thread(target=send_events, daemon=True)
        sender.start()
        command = [executable, "--master", "local[2]", "--driver-memory", "1g", str(Path(__file__).with_name("stream_log_processor.py")), "--seconds", str(seconds)]
        print("Q2 RUN: " + " ".join(command), flush=True)
        try:
            subprocess.run(command, check=True, timeout=seconds + 90, env=dict(os.environ, SPARK_LOCAL_IP="127.0.0.1"))
        finally:
            stop.set()
            sender.join(timeout=2)
        if errors:
            raise RuntimeError("Socket source failed") from errors[0]
        if sent != events:
            raise RuntimeError("Not all source events were sent")
        print("Q2 SOCKET SOURCE: sent={} ERROR_events={}".format(len(sent), sum("ERROR" in event for event in sent)), flush=True)
    finally:
        stop.set()
        server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    if args.seconds < 10:
        parser.error("--seconds must be at least 10")
    run_demo(args.seconds)
