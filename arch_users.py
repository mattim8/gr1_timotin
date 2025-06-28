from pymongo import MongoClient
from datetime import datetime, timedelta
import os
import json
import shutil

client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]



# Создаем пороговые данные для дальнейшей фильтрации.

today = datetime.today()
reg_30days = today - timedelta(days=30)
notactive_14days = today - timedelta(days=14)

print(f'''Зарегистринованные более 30 дней назад: {reg_30days}
Без активности 14 дней: {notactive_14days}''')

# Ищем тех, кто зарегистрировался более 30 дней назад и не проявлял активность последние 14 дней.

required_users = {"user_info.registration_date":{"$lte":reg_30days},
                  "event_time":{"$lte":notactive_14days}}
result = list(collection.find(required_users))
archived_ids = [user['user_id'] for user in result]
for doc in result:
    doc.pop('_id', None)                                            # Удалить _id, чтобы не было ошибки при вставке в коллекцию


# Добавляем пользователей в архивную коллекцию archived_users

db.archived_users.insert_many(result)

# Сохранение отчёт в формате .json

report = {
    "date": today.strftime("%Y-%m-%d"),
    "archived_users_count": len(result),
    "archived_user_ids": archived_ids
}

if not os.path.exists("reports"):
    os.makedirs("reports")

with open("report.json", "w") as f:
    json.dump(report, f,  indent=4)

shutil.move("report.json", "reports/report.json")
