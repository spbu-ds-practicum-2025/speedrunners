import os

DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "server_state.wal")


class StateManager:
    """
    Менеджер состояний для ID Gen.
    Работает ТОЛЬКО с диском. Никакой сети.
    """

    def __init__(self, filepath: str = STATE_FILE):
        self.filepath = filepath

        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        if not os.path.exists(self.filepath):
            with open(self.filepath, "w") as f:
                f.write("0")

    def get_current_max(self) -> int:
        with open(self.filepath, "r") as f:
            content = f.read().strip()
            return int(content) if content else 0

    def update_max(self, new_max: int):
        with open(self.filepath, "w") as f:
            f.write(str(new_max))
