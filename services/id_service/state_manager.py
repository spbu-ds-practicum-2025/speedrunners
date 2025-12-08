import os

DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "server_state.wal")

class StateManager:

    '''
    Менеджер состояний для ID Gen, 
    записывает в файл server_state.wal конец последнего выданного диапазона ID
    (к примеру, если выдано 1-1000, запишет 1000)
    '''
    
    def __init__(self, filepath: str = STATE_FILE):
        self.filepath = filepath

        # Создаем папку data, если её вдруг нет
        if not os.path.exists(DATA_DIR):
            os.makedirs(DATA_DIR)

        # Если файла нет, создаем его и пишем 0
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w") as f:
                f.write("0")

    def get_current_max(self) -> int:
        """Читает текущий максимум из файла"""
        with open(self.filepath, "r") as f:
            content = f.read().strip()
            return int(content) if content else 0

    def update_max(self, new_max: int):
        """Перезаписывает файл новым значением"""
        # В реальном HighLoad тут нужны файловые блокировки (flock),
        # но для MVP и одного инстанса ID Gen это ок.
        with open(self.filepath, "w") as f:
            f.write(str(new_max))