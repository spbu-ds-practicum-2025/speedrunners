import os

# Берем лимит из переменных окружения, по умолчанию 1 млн.
# Это позволит нам легко менять лимит для тестов, не переписывая код.
SHARD_LIMIT = int(os.getenv("SHARD_LIMIT", 1_000_000))

def get_target_shard(id: int) -> str:
    """
    Возвращает имя файла: shard_0.db, shard_1.db и т.д.
    """
    shard_index = id // SHARD_LIMIT
    return f"shard_{shard_index}.db"

def should_preallocate(id: int) -> int:
    """
    Возвращает индекс СЛЕДУЮЩЕГО шарда, если пора его создавать.
    Иначе возвращает -1.
    """
    # Текущий индекс
    current_shard_index = id // SHARD_LIMIT
    
    # Позиция внутри текущего шарда (от 0 до 999 999)
    position_in_shard = id % SHARD_LIMIT
    
    # Порог срабатывания: 90%
    threshold = SHARD_LIMIT * 0.9
    
    if position_in_shard >= threshold:
        return current_shard_index + 1
        
    return -1