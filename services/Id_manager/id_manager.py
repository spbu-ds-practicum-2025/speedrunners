class IDManager:
    def __init__(self):
        self.ids = list(range(1, 1001))

    def get_next_id(self):
        if not self.ids:
            print("No IDs available")
            return None
        return self.ids.pop()

