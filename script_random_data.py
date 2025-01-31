import psycopg2
import random
import traceback

# Настройки подключения к PostgreSQL
DB_SETTINGS = {
    "dbname": "Users",  # Имя вашей базы данных
    "user": "postgres",
    "password": "2004",  # Пароль для пользователя
    "host": "localhost",
    "port": 5432
}

# Пул имен для заполнения
NAME_POOL = ["Алексей", "Мария", "Иван", "Анна", "Дмитрий", "Ольга", "Сергей", "Екатерина", "Владимир", "Юлия"]

# Функция для создания случайной строки
def generate_random_data():
    name = random.choice(NAME_POOL)
    age = random.randint(18, 60)  # Возраст от 18 до 60
    city = random.choice(["Москва", "Санкт-Петербург", "Новосибирск", "Казань", "Екатеринбург", "Воронеж", "Воркута"])
    return name, age, city

# Основной скрипт для заполнения таблицы
def fill_table():
    connection = None
    try:
        print("Попытка подключения к базе данных...")
        connection = psycopg2.connect(**DB_SETTINGS)
        print("Подключение успешно.")
        cursor = connection.cursor()

        # Проверка существования таблицы
        cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'random_data')")
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            print("Таблица random_data не существует.")
            return  # Прерываем выполнение, если таблицы нет

        # Проверка количества записей в таблице до удаления
        cursor.execute("SELECT COUNT(*) FROM random_data")
        count_before = cursor.fetchone()[0]
        print(f"Количество записей в таблице до удаления: {count_before}")

        # Удаление старых записей из таблицы
        print("Удаление старых записей из таблицы random_data...")
        cursor.execute("DELETE FROM random_data")
        connection.commit()  # Фиксируем изменения

        # Проверка количества записей в таблице после удаления
        cursor.execute("SELECT COUNT(*) FROM random_data")
        count_after = cursor.fetchone()[0]
        print(f"Количество записей в таблице после удаления: {count_after}")

        if count_after == 0:
            print("Старые записи успешно удалены.")
        else:
            print("Не удалось удалить записи. Возможно, таблица была пустой.")

        # Вставка новых записей
        num_records = 100
        print(f"Заполнение таблицы {num_records} записями...")

        for _ in range(num_records):
            name, age, city = generate_random_data()
            cursor.execute(
                "INSERT INTO random_data (name, age, city) VALUES (%s, %s, %s)",
                (name, age, city)
            )

        connection.commit()
        print(f"Успешно добавлено {num_records} записей в таблицу random_data.")

    except psycopg2.Error as db_error:
        print("Ошибка подключения или выполнения запроса в PostgreSQL:")
        print(f"Код ошибки: {db_error.pgcode}")
        print(f"Текст ошибки: {db_error.pgerror}")
        print("Стек вызовов:")
        traceback.print_exc()

    except Exception as general_error:
        print("Произошла общая ошибка:")
        print(str(general_error))
        print("Стек вызовов:")
        traceback.print_exc()

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто.")

if __name__ == "__main__":
    print("Начало выполнения скрипта...")
    fill_table()
    print("Конец выполнения скрипта.")
