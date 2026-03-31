import logging
def warn(msg):
    msg=f"[WARNING] {msg}"
    print(msg)
    logging.warning(msg)
def info(msg):
    msg=f"[INFO] {msg}"
    print(msg)
    logging.info(msg)