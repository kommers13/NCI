import csv
import os
import asyncio
import json
from re import sub
from openai import OpenAI
import pandas as pd

data_error = []
data_ignore = []


def remExc(s1):
    s2 = []
    for i, char in enumerate(s1):
        if ('а' <= char <= 'я') or char == ' ' or ('a' <= char <= 'z') or ('0' <= char <= '9'):
            s2.append(char)
        elif char == '-':
            # Проверка границ массива перед обращением к индексам
            if i > 0 and i < len(s1) - 1:
                if s1[i - 1] != ' ' and s1[i + 1] != ' ':
                    s2.append(char)
    s2 = ''.join(s2)
    s2 = ' '.join(s2.split())
    s2 = sub(r'^\d+', '', s2)
    return s2.strip()


async def corMis(word, client):
    try:
        response = await client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": """Твоя роль - ты руководитель отдела кадров.
                        Твоя задача - Расшифруй сокращения и исправь ошибки в профессиональных должностях.
                        Сохрани тире и смысл. Ответ должен быть в нижнем регистре. 
                        Примеры: медсестра - медицинская сестра, 
                        зав. пед. отделения - заведующий педиатрического отделения, 
                        академик РАН - академик российской академии наук.
                        Отправь только готовую расшифровку с исправленными ошибками.
                        Верни их в формате JSON."""
                },
                {"role": "user", "content": json.dumps({"input": word})}
            ],
            temperature=0.5
        )
        content = response.choices[0].message.content
        try:
            ans = json.loads(content)
            print(ans)
        except json.JSONDecodeError:
            ans = {"processed": content.strip().lower()}
        print(ans)
        return ans.get('processed', word).lower().strip()

    except Exception as e:
        data_error.append(f"{word} Ошибка: {str(e)}")
        data_ignore.append(word)
        return word


async def main():

        client = OpenAI(
            base_url="https://free.v36.cm/v1/",
            api_key="sk-YzLUNROgXwAUVGsu394aA972536047E5Ab1bEd6a5627D15b",
            default_headers={"x-foo": "true"}
        )

        dc = os.getcwd()
        data_dir = os.path.join(dc, "data")

        os.chdir(data_dir)

        all_profs = []
        unique_profs = set()

        for file in os.listdir('.'):
            if not file.endswith('.csv'):
                continue
            try:
                with open(file, 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    for row in reader:
                        if row and row[0].strip():
                            original = row[0].strip()
                            all_profs.append(original)
                            processed = remExc(original.lower())
                            if processed:
                                unique_profs.add(processed)
            except Exception as e:
                print(f"Ошибка при обработке файла {file}: {str(e)}")
                continue

        # Обработка уникальных должностей
        processed_cache = {}
        tasks = []
        for prof in unique_profs:
            if prof not in processed_cache:
                print(prof)
                tasks.append(corMis(prof, client))

        results = await asyncio.gather(*tasks)

        for prof, result in zip(unique_profs, results):
            processed_cache[prof] = result

        # Формирование финальных данных
        data_after = []
        for original in all_profs:
            processed = remExc(original.lower())
            data_after.append(processed_cache.get(processed, processed))

        # Сохранение результатов
        os.chdir(dc)
        df = pd.DataFrame({"Original": all_profs, "Processed": data_after})
        df.to_excel("processed_results.xlsx", index=False)

        print("Обработка завершена успешно!")


if __name__ == '__main__':
    asyncio.run(main())