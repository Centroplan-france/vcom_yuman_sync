import logging, json, pprint, os
from logging.handlers import RotatingFileHandler

LOG_PATH = os.getenv("VYSYNC_LOG", "sync_debug.log")      # ⇦ export VYSYNC_LOG=...
LOG_MAX  = 5 * 1024 * 1024                               # 5 Mo
LOG_BKP  = 3                                              # 3 fichiers max


class PrettyJSON(logging.Formatter):
    def format(self, record):
        # Rien à faire si pas d’arguments
        if not record.args:
            return super().format(record)

        # Cas 1 : un dict/list passé directement
        if isinstance(record.args, (dict, list)):
            record.msg  = json.dumps(record.args, indent=2, sort_keys=True)
            record.args = ()
        # Cas 2 : tuple(len=1) contenant dict/list (logger.debug("%s", payload))
        elif (isinstance(record.args, tuple)
              and len(record.args) == 1
              and isinstance(record.args[0], (dict, list))):
            record.msg  = json.dumps(record.args[0], indent=2, sort_keys=True)
            record.args = ()

        return super().format(record)


def init_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger                                   # déjà configuré

    logger.setLevel(logging.DEBUG)                      # root niveau DEBUG

    # ─── Console (INFO+) ──────────────────────────────
    ch = logging.StreamHandler()
    ch.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    ch.setFormatter(PrettyJSON("%(levelname)s | %(message)s"))
    logger.addHandler(ch)

    # ─── Fichier rotatif (DEBUG) ──────────────────────
    fh = RotatingFileHandler(LOG_PATH, maxBytes=LOG_MAX, backupCount=LOG_BKP)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(PrettyJSON(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    ))
    logger.addHandler(fh)

    return logger

# --------------------------------------------------------------------------- #
# Helper DEBUG universel : sérialise n’importe quel objet en JSON lisible.
# --------------------------------------------------------------------------- #
def _dump(label: str, obj, *, logger: logging.Logger | None = None) -> None:
    """
    Écrit « label » puis l’objet (pretty-JSON) au niveau DEBUG.
    Ne fait rien si le logger n’est pas en DEBUG – zéro overhead en prod.
    """
    log = logger or logging.getLogger()
    if not log.isEnabledFor(logging.DEBUG):
        return
    log.debug("%s\n%s", label, json.dumps(obj, default=str, indent=2, sort_keys=True))